"""Aggregator App for faust-regional.

ADR-0023 Phase 6b §B. One aggregator App per region, bound to redpanda-hq.
Owns ALL state (RocksDB Tables); source Apps are stateless wrap-and-
forward.

Inputs:
  - per-region fan-in topic on hq (from source Apps, wraps edge-cluster
    events in a RegionalAggregatorInput envelope)
  - asset-cm-state on hq (cm-service produces directly to hq per §A;
    no source App needs to fan it in — the aggregator consumes it
    natively from its own broker)

Per-asset Table (`region_assets_latest`) keyed by asset_id. Tracks each
asset's latest contributions to all three rollups:
  - logistics_severity (int from LogisticsSeverity)
  - logistics_factors  (list of {factor_id, severity_name})
  - cm_overall_status, cm_lifecycle  (ints from cm-state JSON)
  - sustainment_wear    (dict of {component_id: {unit, rul_remaining}})
  - last_updated_ns

Three aggregator agents emit rolled-up topics on heartbeat AND on
state-change.

ASYMMETRIC COVERAGE per §B greenlight: asset_telemetry_windows envelopes
ARE consumed (the source Apps wrap them, the fan-in agent dispatches by
oneof tag), but they currently fall to a DEBUG-log no-op for the
wear-trends aggregator. derived-sustainment alone drives wear-trend
emission in §B. Full join with the windowed-input path waits for
follow-up #11's sustainment-data test fixtures.
"""
from __future__ import annotations

import json
import logging
import os
import time
from datetime import timedelta
from typing import Any

import faust

from google.protobuf.timestamp_pb2 import Timestamp
from openddil.regional.v1 import (
    regional_aggregator_input_pb2 as inp_pb,
    region_fleet_summary_pb2 as fs_pb,
    region_top_factors_pb2 as tf_pb,
    region_wear_trends_pb2 as wt_pb,
)

from severity import (
    bucket_from_cm_state,
    bucket_from_logistics_severity,
    worse_bucket,
)

log = logging.getLogger("faust_regional.aggregator")


_LOGISTICS_SEVERITY_NAME = {
    0: "UNSPECIFIED", 1: "OK", 2: "DEGRADED", 3: "CRITICAL",
    4: "NON_OPERATIONAL",
}

_TOP_FACTORS_N = int(os.getenv("REGIONAL_TOP_FACTORS_N", "10"))
_HEARTBEAT_S = float(os.getenv("REGIONAL_HEARTBEAT_INTERVAL_S", "30"))

# Output topic names. Fixed (not env-tunable): the projector handlers
# subscribe to these exact names; tunability would require coordinated
# config across services and provides no operational value.
_TOPIC_FLEET_SUMMARY = "region-fleet-summary"
_TOPIC_TOP_FACTORS = "region-top-factors"
_TOPIC_WEAR_TRENDS = "region-wear-trends"


# Per-asset state record stored in the aggregator's Faust Table.
class AssetState(faust.Record, serializer="json"):
    logistics_severity: int = 0
    # list of (factor_id, severity_name) tuples for the most recent
    # logistics update.
    logistics_factors: list = []
    cm_overall_status: int = 0
    cm_lifecycle: int = 0
    # dict {component_id: {"unit": str, "rul": float}}
    sustainment_wear: dict = {}
    last_updated_ns: int = 0
    last_source_edge_id: str = ""


def make_aggregator_app(
    *,
    region_id: str,
    hq_brokers: str,
    fan_in_topic: str,
    web_port: int,
) -> faust.App:
    app_id = f"region-{region_id}-aggregator"
    app = faust.App(
        app_id,
        broker=f"kafka://{hq_brokers}",
        store="memory://",
        value_serializer="raw",
        web_port=web_port,
    )

    # State table — keyed by asset_id, holds the latest contribution from
    # each input stream so the heartbeat emit can recompute the rollups
    # from a single in-process snapshot.
    assets_latest = app.Table(
        f"region_{region_id.replace('-', '_')}_assets_latest",
        default=AssetState,
        partitions=1,
    )

    fan_in = app.topic(fan_in_topic, value_type=bytes)
    cm_state = app.topic("asset-cm-state", value_type=bytes)

    out_fleet_summary = app.topic(_TOPIC_FLEET_SUMMARY, value_type=bytes)
    out_top_factors = app.topic(_TOPIC_TOP_FACTORS, value_type=bytes)
    out_wear_trends = app.topic(_TOPIC_WEAR_TRENDS, value_type=bytes)

    @app.agent(fan_in)
    async def on_fan_in(stream):
        async for raw in stream:
            if not raw:
                continue
            env = inp_pb.RegionalAggregatorInput()
            try:
                env.ParseFromString(raw)
            except Exception as exc:
                log.warning("aggregator(%s): bad envelope bytes (len=%d): %s",
                            region_id, len(raw), exc)
                continue
            # Defensive cross-check: an envelope tagged with a different
            # region_id is a deployment-time-contract violation per
            # ADR-0023. Log and drop rather than poisoning state.
            if env.region_id and env.region_id != region_id:
                log.warning("aggregator(%s): dropped envelope tagged "
                            "region_id=%r (cross-region leakage?)",
                            region_id, env.region_id)
                continue
            await _dispatch_envelope(env, assets_latest)

    @app.agent(cm_state)
    async def on_cm_state(stream):
        """cm-state JSON is produced to hq directly by cm-service. The
        aggregator consumes it natively from its own broker — no source-
        App fan-in needed for this topic."""
        async for raw in stream:
            if not raw:
                continue
            try:
                envelope = json.loads(raw)
            except Exception as exc:
                log.warning("aggregator(%s): bad cm-state JSON (len=%d): %s",
                            region_id, len(raw), exc)
                continue
            event_region = envelope.get("region_id") or ""
            # cm-service produces ALL regions' cm-state to one hq topic;
            # filter to just this region's assets.
            if event_region and event_region != region_id:
                continue
            await _apply_cm_state(envelope, assets_latest)

    @app.timer(interval=timedelta(seconds=_HEARTBEAT_S))
    async def heartbeat():
        await _emit_rollups(
            region_id=region_id,
            assets_latest=assets_latest,
            out_fleet_summary=out_fleet_summary,
            out_top_factors=out_top_factors,
            out_wear_trends=out_wear_trends,
        )

    return app


async def _dispatch_envelope(
    env: inp_pb.RegionalAggregatorInput,
    assets_latest,
) -> None:
    asset_id = env.asset_id or ""
    if not asset_id:
        return
    payload = env.WhichOneof("payload")
    if payload == "logistics_status":
        await _apply_logistics_status(env, assets_latest)
    elif payload == "derived_sustainment":
        await _apply_derived_sustainment(env, assets_latest)
    elif payload == "asset_telemetry_windows":
        # §B ASYMMETRIC COVERAGE: windowed-input path is wired in the
        # envelope but does NOT drive wear-trend emissions in §B. Logged
        # at DEBUG so the path's existence is visible to a future debugger
        # without contaminating wear-trends with unverified updates.
        log.debug("aggregator: asset_telemetry_windows received for %s "
                  "(no-op pending follow-up #11)", asset_id)
    elif payload == "asset_cm_state_json":
        # The cm-state path is normally handled by the on_cm_state agent
        # (direct hq subscription). If a source App ever wraps cm-state
        # in an envelope (it doesn't today, but the envelope supports it
        # for forward-compat), unwrap it here.
        try:
            envelope_json = json.loads(env.asset_cm_state_json)
        except Exception:
            return
        await _apply_cm_state(envelope_json, assets_latest)


async def _apply_logistics_status(env, assets_latest) -> None:
    upd = env.logistics_status
    asset_id = upd.status.asset_id or env.asset_id or ""
    if not asset_id:
        return
    state = assets_latest[asset_id]
    state.logistics_severity = int(upd.status.overall_severity)
    state.logistics_factors = [
        (f.factor_id, _LOGISTICS_SEVERITY_NAME.get(int(f.severity), "UNSPECIFIED"))
        for f in upd.status.constraining_factors
        if int(f.severity) > 1  # > OK
    ]
    state.last_updated_ns = int(time.time() * 1_000_000_000)
    state.last_source_edge_id = env.source_edge_id or ""
    assets_latest[asset_id] = state


async def _apply_derived_sustainment(env, assets_latest) -> None:
    ete = env.derived_sustainment
    asset_id = ete.asset.asset_id or env.asset_id or ""
    if not asset_id:
        return
    state = assets_latest[asset_id]
    wear = dict(state.sustainment_wear or {})
    components = ete.sustainment.wear.components
    for comp_id, wear_state in components.items():
        rul = wear_state.remaining_useful_life
        # Quantity.value may be 0.0 if unset; skip empty entries.
        if rul.value == 0.0 and not rul.unit:
            continue
        wear[comp_id] = {"unit": rul.unit or "", "rul": float(rul.value)}
    state.sustainment_wear = wear
    state.last_updated_ns = int(time.time() * 1_000_000_000)
    state.last_source_edge_id = env.source_edge_id or ""
    assets_latest[asset_id] = state


async def _apply_cm_state(envelope_json: dict, assets_latest) -> None:
    asset_id = envelope_json.get("asset_id") or ""
    if not asset_id:
        return
    state = assets_latest[asset_id]
    state.cm_overall_status = int(envelope_json.get("overall_status") or 0)
    state.cm_lifecycle = int(envelope_json.get("lifecycle") or 0)
    state.last_updated_ns = int(time.time() * 1_000_000_000)
    state.last_source_edge_id = envelope_json.get("edge_id") or state.last_source_edge_id
    assets_latest[asset_id] = state


# ---------------------------------------------------------------------------
# Heartbeat emit — computes all three rollups from current Table snapshot
# ---------------------------------------------------------------------------

async def _emit_rollups(
    *,
    region_id: str,
    assets_latest,
    out_fleet_summary,
    out_top_factors,
    out_wear_trends,
) -> None:
    snapshot = list(assets_latest.items())
    if not snapshot:
        # Cold start — projector cold-state surfaces "Awaiting first
        # emission..." in the UI. Skip emit until at least one asset has
        # been observed.
        return

    now_ns = int(time.time() * 1_000_000_000)
    now_ts = Timestamp()
    now_ts.FromNanoseconds(now_ns)

    # ---- fleet summary -----------------------------------------------------
    counts = {"nominal": 0, "degraded": 0, "critical": 0, "non_operational": 0}
    for _asset_id, state in snapshot:
        logistics_bucket = bucket_from_logistics_severity(state.logistics_severity)
        cm_bucket = bucket_from_cm_state(state.cm_overall_status, state.cm_lifecycle)
        bucket = worse_bucket(logistics_bucket, cm_bucket)
        counts[bucket] = counts[bucket] + 1
    fs_msg = fs_pb.RegionFleetSummary(
        region_id=region_id,
        nominal=counts["nominal"],
        degraded=counts["degraded"],
        critical=counts["critical"],
        non_operational=counts["non_operational"],
        asset_count=sum(counts.values()),
    )
    fs_msg.observed_at.CopyFrom(now_ts)
    _stamp_provenance(fs_msg.provenance, region_id, now_ts)
    await out_fleet_summary.send(key=region_id, value=fs_msg.SerializeToString())

    # ---- top factors -------------------------------------------------------
    factor_counts: dict[str, dict] = {}  # factor_id -> {count, severity_breakdown}
    for _asset_id, state in snapshot:
        for factor_id, sev_name in (state.logistics_factors or []):
            entry = factor_counts.setdefault(
                factor_id,
                {"count": 0, "severity_breakdown": {}},
            )
            entry["count"] += 1
            br = entry["severity_breakdown"]
            br[sev_name] = br.get(sev_name, 0) + 1
    # sort by count desc, take top-N
    sorted_factors = sorted(
        factor_counts.items(),
        key=lambda kv: kv[1]["count"],
        reverse=True,
    )[:_TOP_FACTORS_N]
    if sorted_factors:
        tf_msg = tf_pb.RegionTopFactors(region_id=region_id)
        for factor_id, entry in sorted_factors:
            fc = tf_msg.factors.add()
            fc.factor_id = factor_id
            fc.count = entry["count"]
            for sev_name, c in entry["severity_breakdown"].items():
                fc.severity_breakdown[sev_name] = c
        tf_msg.observed_at.CopyFrom(now_ts)
        _stamp_provenance(tf_msg.provenance, region_id, now_ts)
        await out_top_factors.send(key=region_id, value=tf_msg.SerializeToString())

    # ---- wear trends -------------------------------------------------------
    # Per the mixed-unit handling rule: group by (component_id, unit).
    # Each bucket gets its own ComponentWearTrend row.
    by_comp_unit: dict[tuple[str, str], list[float]] = {}
    for _asset_id, state in snapshot:
        for comp_id, entry in (state.sustainment_wear or {}).items():
            unit = (entry or {}).get("unit") or ""
            rul = (entry or {}).get("rul")
            if rul is None:
                continue
            by_comp_unit.setdefault((comp_id, unit), []).append(float(rul))
    if by_comp_unit:
        wt_msg = wt_pb.RegionWearTrends(region_id=region_id)
        for (comp_id, unit), ruls in sorted(by_comp_unit.items()):
            cw = wt_msg.components.add()
            cw.component_id = comp_id
            cw.unit = unit
            cw.mean_rul_remaining = sum(ruls) / len(ruls)
            cw.asset_count = len(ruls)
        wt_msg.observed_at.CopyFrom(now_ts)
        _stamp_provenance(wt_msg.provenance, region_id, now_ts)
        await out_wear_trends.send(key=region_id, value=wt_msg.SerializeToString())


def _stamp_provenance(provenance, region_id: str, now_ts: Timestamp) -> None:
    provenance.producer_id = "regional-aggregator"
    provenance.region_id = region_id
    provenance.ingest_time.CopyFrom(now_ts)
    provenance.classification = "U"
