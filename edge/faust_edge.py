# =============================================================================
# ADR-0007: Dual-path streaming architecture.
# Migration target is Quix Streams for sub-10ms p99 latency and native
# Protobuf+Schema Registry support. Faust is current; Quix is planned.
# DO NOT leak Faust types (faust.Record, faust.Stream, App) into algorithms.py
# or detection/units.py. The boundary is EventView/AssetState/Anomaly only.
# =============================================================================
import json
import uuid
from datetime import datetime, timezone

"""
OpenDDIL Faust Edge Agent
-------------------------
Modernized anomaly detection for tactical telemetry.

DEPLOYMENT NOTE:
- Production: Uses `docker-compose.yml` which pulls pre-built images.
- Development: Uses `docker-compose.override.yml` to build from source and
               mount local code for hot-reloading.
"""
import logging
import os
import time
from collections import deque
from typing import Optional

import faust
from openddil.telemetry.v1 import telemetry_pb2 as pb
from openddil.logistics.v1 import windowed_telemetry_pb2 as winpb
from detection.algorithms import REGISTERED, EventView, AssetState, Anomaly
from detection.units import from_proto
from element_rollup import rollup_from_envelope
from detection.windows import (
    Sample,
    build_window_spec,
    compute_trend,
    proto_to_pint,
    trend_to_proto,
)
# ADR-0044 amendment ("posture, a third column") -- a pure sibling module,
# no Faust/proto types (same boundary discipline as detection/algorithms.py).
# Aliased so the StateRecord field named `posture` below never shadows it.
import posture as posture_mod

logger = logging.getLogger(__name__)
# Phase 5 prognostics derivation engine — the one-line seam (ADR-0020).
# `register(app)` below adds the engine's own agent + Table; this is the
# *only* coupling. To extract the engine into its own service later,
# delete the import and the call site at the bottom of this file.
from prognostics import register as register_prognostics

# ADR-0023 §Faust-edge: per-edge instance gets a distinct FAUST_APP_ID so
# each instance's consumer groups + changelog topics are namespaced per
# edge ("openddil-edge-01-prognostics_accumulators-changelog" etc.). The
# broker URL is built from KAFKA_BROKERS env so each instance points at its
# own broker. Defaults preserve single-tier behavior pre-6a.
FAUST_APP_ID  = os.getenv("FAUST_APP_ID",  "openddil-edge")
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "redpanda-edge:9092")

# Origin-node provenance env (ADR-0022 / ADR-0023). Phase 6a stamps these
# into emitted-event Provenance so 6b's coordinated handler upgrades
# inherit a populated field rather than having to backfill it then.
OPENDDIL_EDGE_ID   = os.getenv("OPENDDIL_EDGE_ID",   "edge-01")
OPENDDIL_REGION_ID = os.getenv("OPENDDIL_REGION_ID", "region-01")

# ADR-0044 amendment ("posture, a third column") -- thresholds for the
# per-asset posture state machine (edge/posture.py), env-driven per
# the ADR-0044 amendment's declared defaults so ops can tune without a code change.
POSTURE_MOVE_SPEED_MPS = float(os.getenv("POSTURE_MOVE_SPEED_MPS", "1.0"))
POSTURE_MOVE_HOLD_S    = float(os.getenv("POSTURE_MOVE_HOLD_S",    "10"))
POSTURE_STOP_HOLD_S    = float(os.getenv("POSTURE_STOP_HOLD_S",    "20"))
POSTURE_THRESHOLDS = posture_mod.Thresholds(
    move_speed_mps=POSTURE_MOVE_SPEED_MPS,
    move_hold_s=POSTURE_MOVE_HOLD_S,
    stop_hold_s=POSTURE_STOP_HOLD_S,
)

# posture.py's lower-case state names <-> the proto enum. Lower-case without
# prefix is also the projector's on-disk representation (telemetry_latest.py),
# so this mapping and that one must be kept in agreement.
_POSTURE_TO_PROTO = {
    posture_mod.UNSPECIFIED:   pb.POSTURE_STATUS_UNSPECIFIED,
    posture_mod.EMPLACED:      pb.POSTURE_STATUS_EMPLACED,
    posture_mod.MARCH_ORDERED: pb.POSTURE_STATUS_MARCH_ORDERED,
    posture_mod.MOVING:        pb.POSTURE_STATUS_MOVING,
    posture_mod.EMPLACING:     pb.POSTURE_STATUS_EMPLACING,
}

# In-memory only -- a restart or reset losing this is harmless: it only
# widens which records get the byte-identical fast path for a short while,
# never a correctness issue (see the emission comment in process() below).
_launcher_ever_seen: dict[str, bool] = {}


def _posture_inputs(evt: "pb.EntityTelemetryEvent"):
    """Pull the state machine's three inputs out of the parsed event.

    Returns (launcher_raised, speed, now):
      - launcher_raised: True/False if the field was set, else None (this
        platform's domain has no launcher bit at all -- HasField, not a
        truthiness check, so an explicit False is never confused with unset).
      - speed: |velocity| in m/s, computed from kinematics.velocity.ecef
        only (ground_speed is a separate, never-populated-for-DIS field --
        see the ADR-0044 amendment). None if no ecef velocity was sent.
      - now: provenance.sample_time as epoch seconds (the record's OWN event
        time, not wall clock, so replay stays deterministic).
    """
    launcher_raised = (
        evt.operational_state.launcher_raised
        if evt.operational_state.HasField("launcher_raised")
        else None
    )
    vel = evt.kinematics.velocity
    if vel.WhichOneof("frame") == "ecef":
        v = vel.ecef
        speed = (v.x * v.x + v.y * v.y + v.z * v.z) ** 0.5
    else:
        speed = None
    ts = evt.provenance.sample_time
    now = ts.seconds + ts.nanos / 1e9
    return launcher_raised, speed, now

app = faust.App(
    FAUST_APP_ID,
    broker=f"kafka://{KAFKA_BROKERS}",
    value_serializer="raw",
    # Element snapshots run to several MB per record; the per-partition fetch
    # default (1 MiB) is below that. Matches the element topic's
    # max.message.bytes so a record the broker accepts is one we can fetch.
    consumer_max_fetch_size=16 * 1024 * 1024,
)

raw_topic = app.topic("raw-sensor-stream", value_type=bytes)
state_topic = app.topic("telemetry-latest-state", value_type=bytes)
events_topic = app.topic("tactical-events", value_type=bytes)
# Phase 3.5: rolling-window aggregations consumed by the logistics fusion
# service. Output records are openddil.telemetry.v1.WindowedTelemetry.
windows_topic = app.topic("asset-telemetry-windows", value_type=bytes)
# Element snapshots published on this edge's own broker, keyed by asset_id;
# one JSON envelope per asset per ~30 s, up to a few MB each.
ELEMENT_TOPIC = os.getenv("ELEMENT_TOPIC", "asset-element-telemetry")
element_topic = app.topic(ELEMENT_TOPIC, value_type=bytes)

# Window sizing — env-driven for ops tuning without code change.
FLUID_WINDOW_NS = int(float(os.getenv("FLUID_WINDOW_MIN", "15")) * 60 * 1e9)
WEAR_WINDOW_NS  = int(float(os.getenv("WEAR_WINDOW_MIN",  "60")) * 60 * 1e9)
EMIT_EVERY_N_SAMPLES = int(os.getenv("WINDOW_EMIT_EVERY_N", "5"))
# How many points per signal to retain — sized so a 60-min wear window with
# 1 sample/sec stays in memory comfortably. Tune via env if telemetry rates
# differ.
DEQUE_CAP = int(os.getenv("WINDOW_DEQUE_CAP", "4096"))

# Faust-managed state record for RocksDB serialization.
class StateRecord(faust.Record):
    last_temp_k: float = 0.0
    temp_ewma_k: float = 0.0
    temp_ewma_alpha: float = 0.2
    # ADR-0044 amendment ("posture, a third column"). Defaults so a
    # changelog record written before this field existed still loads --
    # as a cold start, same as a genuinely new asset. A reset trims the
    # changelog outright, so after a reset every asset cold-starts
    # unspecified too; same externally-visible result, different cause.
    posture: str = posture_mod.UNSPECIFIED
    posture_since: Optional[float] = None
    motion: str = ""
    motion_since: Optional[float] = None

asset_state = app.Table(
    "asset_state",
    default=StateRecord,
    # MUST match raw-sensor-stream's partition count. See
    # openddil-tactical-agents/README.md "Faust Tables — the
    # partition-count invariant" for why this is strict-but-invisible
    # until the app has more than one Table. Was `partitions=8` for
    # a long time; surfaced as a crash-loop the moment Phase 5 added
    # the second Table.
    partitions=1,
)

def _build_view(evt: pb.EntityTelemetryEvent) -> EventView:
    """Project proto -> algorithm-friendly view. Single conversion point."""
    return EventView(
        asset_id=evt.asset.asset_id,
        sample_time_ns=evt.kinematics.position.valid_at.ToNanoseconds(),
        component_temp=from_proto(evt.sustainment.thermal.component_temperature),
        ambient_temp=from_proto(evt.sustainment.thermal.ambient_temperature),
        ground_speed=from_proto(evt.kinematics.velocity.ground_speed),
        fuel_remaining=from_proto(evt.sustainment.fluids.fuel_remaining),
        bus_voltage=from_proto(evt.sustainment.power.bus_voltage),
    )

# ---------------------------------------------------------------------------
# Rolling-window state — per-process in-memory buffers (Phase 3.5).
#
# Schema: window_buffers[asset_id][signal_key] -> deque[Sample]
# where signal_key is one of:
#   - "fluid:fuel_remaining"
#   - f"ammo:{slot}"
#   - f"wear_hours:{component}"
#   - f"wear_rul:{component}"
#
# Buffers live in process memory; loss on restart is acceptable because
# trends regenerate within minutes from the live feed (no per-asset durable
# state lives here — that's the logistics fusion service's job).
# ---------------------------------------------------------------------------
window_buffers: dict[str, dict[str, deque]] = {}
samples_since_last_emit: dict[str, int] = {}
# Latest element rollup per asset. Both window producers (the raw path and the
# element path) write the same key on the windows topic, so each attaches this
# cache and neither clears what the other contributed.
element_rollups: dict[str, winpb.ElementRollup] = {}
_rollup_logged: set[str] = set()


def _emit_window_for_asset(evt: pb.EntityTelemetryEvent,
                            now_ns: int) -> winpb.WindowedTelemetry | None:
    """Build a WindowedTelemetry from the buffered samples for one asset.
    Returns None if there isn't enough data yet (no trend possible)."""
    aid = evt.asset.asset_id
    if not window_buffers.get(aid):
        return None
    return _build_window(
        aid,
        evt.asset.platform_variant or "",
        now_ns,
        sample_time=evt.provenance.sample_time,
        originator_nation=evt.provenance.originator_nation,
        releasable_to=evt.provenance.releasable_to,
        evt=evt,
    )


def _build_window(aid: str,
                  platform_variant: str,
                  now_ns: int,
                  *,
                  sample_time=None,
                  originator_nation: str = "",
                  releasable_to=(),
                  evt: pb.EntityTelemetryEvent | None = None,
                  window_range_ns: tuple[int, int] | None = None,
                  ) -> winpb.WindowedTelemetry:
    """Shared window construction for both producers: the raw path (which
    passes the source event, for capacity and fault passthrough) and the
    element path (which passes only the envelope's labels and a fixed range).
    Carries whatever sustainment trends this asset's buffers already hold and
    the cached element rollup, if any."""
    buffers = window_buffers.get(aid, {})

    out = winpb.WindowedTelemetry()
    out.asset_id = aid
    out.platform_variant = platform_variant
    out.computed_at.FromNanoseconds(now_ns)
    # ADR-0023 Phase 6b §A.2: stamp origin-node provenance from this
    # faust-edge instance's env (faust-edge is already per-edge). The
    # projector telemetry_windows handler reads this with env-default
    # fallback; faust-regional's region-wear-trends aggregator (§B)
    # consumes it as an attributed input. Inherit producer_id/sample_time
    # from the source event when possible.
    if sample_time is not None and (sample_time.seconds or sample_time.nanos):
        out.provenance.sample_time.CopyFrom(sample_time)
    out.provenance.producer_id = "faust-edge"
    out.provenance.edge_id = OPENDDIL_EDGE_ID
    out.provenance.region_id = OPENDDIL_REGION_ID
    out.provenance.ingest_time.FromNanoseconds(now_ns)
    out.provenance.classification = "U"

    # ADR-0029 §3: PROPAGATE, do not derive. A window is a rollup of ONE
    # asset's own samples, so it inherits that asset's labels whole -- this
    # is not the aggregate case the regional aggregator handles, where rows
    # from several authors are combined and no one of them may claim
    # authorship of the result. Here there is exactly one author and it is
    # the same author as the source event.
    #
    # WITHOUT THIS COPY the windowed path is lossy, and lossy in the
    # direction that reads as correct: an asset reaching fusion only through
    # `asset-telemetry-windows` would arrive unlabelled and be refused at
    # the egress gate as `unlabelled` -- indistinguishable from an asset
    # nobody declared, with the fix appearing to belong at an ingress that
    # had in fact done its job. It did not bite because raw-sensor-stream is
    # also a direct fusion input and fusion's stored label is sticky, so one
    # labelled inbound was enough to mask it.
    #
    # No else-branch and no default. An unlabelled source event produces an
    # unlabelled window, which the gate then refuses; inventing a value here
    # would hide the thing the gate exists to surface.
    if originator_nation:
        out.provenance.originator_nation = originator_nation
    if releasable_to:
        out.provenance.releasable_to.extend(releasable_to)

    total_samples = 0
    window_min_start: int | None = None

    # Fluid trends (fuel and any future named fluid)
    for key, dq in buffers.items():
        if not key.startswith("fluid:"):
            continue
        signal = key.split(":", 1)[1]
        trend = compute_trend(list(dq), window_ns=FLUID_WINDOW_NS, now_ns=now_ns)
        if trend is None:
            continue
        out.fluid_trends[signal].CopyFrom(trend_to_proto(trend))
        total_samples += trend.sample_count
        if window_min_start is None or trend.window_start_ns < window_min_start:
            window_min_start = trend.window_start_ns

    # Consumable trends — group by slot key
    ammo_slots: dict[str, dict] = {}
    for key, dq in buffers.items():
        if not key.startswith("ammo:"):
            continue
        slot = key.split(":", 1)[1]
        trend = compute_trend(list(dq), window_ns=FLUID_WINDOW_NS, now_ns=now_ns)
        if trend is None:
            continue
        ammo_slots[slot] = {"trend": trend}
    # Capacity passthrough from the latest event
    for slot, info in ammo_slots.items():
        cs = (evt.sustainment.consumables.items.get(slot)
              if evt is not None else None)
        t = out.consumable_trends.add()
        t.slot_key = slot
        t.remaining.CopyFrom(trend_to_proto(info["trend"]))
        if cs is not None:
            t.capacity = cs.quantity_capacity
            t.nsn = cs.nsn

    # Wear trends — pair hours_in_service with remaining_useful_life per component
    component_names = set()
    for key in buffers:
        if key.startswith("wear_hours:"):
            component_names.add(key.split(":", 1)[1])
    for comp in component_names:
        hours_buf = buffers.get(f"wear_hours:{comp}")
        rul_buf   = buffers.get(f"wear_rul:{comp}")
        if hours_buf is None or rul_buf is None:
            continue
        h_trend = compute_trend(list(hours_buf),
                                 window_ns=WEAR_WINDOW_NS, now_ns=now_ns)
        r_trend = compute_trend(list(rul_buf),
                                 window_ns=WEAR_WINDOW_NS, now_ns=now_ns)
        if h_trend is None and r_trend is None:
            continue
        t = out.wear_trends.add()
        t.component_key = comp
        if h_trend is not None:
            t.hours_in_service.CopyFrom(trend_to_proto(h_trend))
        if r_trend is not None:
            t.remaining_useful_life.CopyFrom(trend_to_proto(r_trend))

    # Latest subsystem-fault tokens (passthrough — not aggregated)
    if evt is not None and list(evt.sustainment.health.active_fault_codes):
        out.active_fault_codes_latest.extend(
            list(evt.sustainment.health.active_fault_codes),
        )

    # WindowSpec — use the widest range we covered.
    if window_range_ns is not None:
        start_ns, end_ns = window_range_ns
    else:
        start_ns, end_ns = window_min_start or now_ns, now_ns
    out.window.CopyFrom(build_window_spec(
        window_start_ns=start_ns,
        window_end_ns=end_ns,
        sample_count=total_samples,
    ))

    rollup = element_rollups.get(aid)
    if rollup is not None:
        out.element_rollup.CopyFrom(rollup)
    return out


def _window_from_envelope(env: dict,
                          rollup: winpb.ElementRollup,
                          now_ns: int) -> winpb.WindowedTelemetry:
    """Window for the element path: the window spans the snapshot's own
    observation instant, and labels come from the envelope (ADR-0029
    PROPAGATE, no default -- see _build_window). The rollup is attached from
    the cache, so the caller stores it there first."""
    aid = env.get("asset_id") or ""
    obs_ns = (rollup.observed_at.ToNanoseconds()
              if rollup.HasField("observed_at") else now_ns)
    releasable = env.get("releasable_to") or []
    if not isinstance(releasable, list):
        releasable = []
    nation = env.get("originator_nation") or ""
    return _build_window(
        aid,
        env.get("platform_variant") or "",
        now_ns,
        originator_nation=nation if isinstance(nation, str) else "",
        releasable_to=[r for r in releasable if isinstance(r, str)],
        window_range_ns=(obs_ns, obs_ns),
    )


def _buffer_event(evt: pb.EntityTelemetryEvent, now_ns: int) -> None:
    """Add the event's sustainment quantities to the per-asset window buffers."""
    aid = evt.asset.asset_id
    if not aid:
        return
    buffers = window_buffers.setdefault(aid, {})

    def _push(signal_key: str, proto_qty) -> None:
        pq = proto_to_pint(proto_qty)
        if pq is None:
            return
        dq = buffers.setdefault(signal_key, deque(maxlen=DEQUE_CAP))
        dq.append(Sample(epoch_ns=now_ns, quantity=pq))

    _push("fluid:fuel_remaining", evt.sustainment.fluids.fuel_remaining)

    for slot, state in evt.sustainment.consumables.items.items():
        if state.quantity_capacity > 0:
            # Convert remaining count to a dimensionless quantity so the
            # regression has consistent units.
            from pint import UnitRegistry
            ureg = UnitRegistry()
            dq = buffers.setdefault(f"ammo:{slot}", deque(maxlen=DEQUE_CAP))
            dq.append(Sample(
                epoch_ns=now_ns,
                quantity=ureg.Quantity(float(state.quantity_remaining), "count"),
            ))

    for comp, state in evt.sustainment.wear.components.items():
        _push(f"wear_hours:{comp}", state.hours_in_service)
        _push(f"wear_rul:{comp}",   state.remaining_useful_life)


@app.agent(element_topic)
async def process_elements(stream):
    async for raw in stream:
        try:
            env = json.loads(raw)
            if not isinstance(env, dict):
                raise ValueError("envelope is not an object")
        except Exception as e:
            logging.error(f"Failed to parse element envelope: {e}")
            continue

        aid = env.get("asset_id")
        if not aid or not isinstance(aid, str):
            logger.debug("element envelope without asset_id; skipped")
            continue
        rollup = rollup_from_envelope(env)
        if rollup is None:
            continue
        element_rollups[aid] = rollup

        w = _window_from_envelope(env, rollup, int(time.time() * 1e9))
        out_bytes = w.SerializeToString()
        await windows_topic.send(key=aid.encode(), value=out_bytes)

        if aid not in _rollup_logged:
            _rollup_logged.add(aid)
            log = logger.info
        else:
            log = logger.debug
        log("element rollup emitted %s", {
            "asset": aid,
            "profile": rollup.profile_name,
            "element_count": rollup.element_count,
            "bytes_in": len(raw),
            "bytes_out": len(out_bytes),
        })


@app.agent(raw_topic)
async def process(stream):
    async for raw in stream:
        evt = pb.EntityTelemetryEvent()
        try:
            evt.ParseFromString(raw)
        except Exception as e:
            logging.error(f"Failed to parse protobuf: {e}")
            continue

        aid = evt.asset.asset_id

        # Load state once per record -- carries both the anomaly-detection
        # rolling state (below) and the posture state machine's state
        # (ADR-0044 amendment), read and written together.
        rec = asset_state[aid]

        # 0. Posture state machine (ADR-0044 amendment, "posture, a third
        # column"). Decided ONCE, here, at the owning (edge) tier; every
        # other tier (projector, store) only carries what we decide here,
        # never re-derives it.
        launcher_raised, speed, posture_now = _posture_inputs(evt)
        if launcher_raised is not None:
            _launcher_ever_seen[aid] = True
        prev_posture_state = posture_mod.PostureState(
            posture=rec.posture,
            posture_since=rec.posture_since,
            motion=rec.motion,
            motion_since=rec.motion_since,
        )
        new_posture_state = posture_mod.step(
            prev_posture_state, launcher_raised, speed, posture_now, POSTURE_THRESHOLDS
        )
        if new_posture_state.posture != prev_posture_state.posture:
            logger.info("posture transition %s", {
                "asset": aid,
                "from": prev_posture_state.posture,
                "to": new_posture_state.posture,
                "at": posture_now,
            })
        rec.posture = new_posture_state.posture
        rec.posture_since = new_posture_state.posture_since
        rec.motion = new_posture_state.motion
        rec.motion_since = new_posture_state.motion_since

        # 1. Forward to latest-state. Byte-identical to today UNLESS this
        # asset's posture is anything but unspecified, OR it has ever
        # carried a launcher_raised signal at all (even a steady False) --
        # so traffic that never touches posture (no DIS appearance, or a
        # domain with no launcher bit at all) stays byte-identical on the
        # wire, exactly as it was before this feature existed.
        posture_untouched = (
            new_posture_state.posture == posture_mod.UNSPECIFIED
            and not _launcher_ever_seen.get(aid, False)
        )
        if posture_untouched:
            await state_topic.send(key=aid, value=raw)
        else:
            if new_posture_state.posture == posture_mod.UNSPECIFIED:
                evt.operational_state.ClearField("posture_since")
            else:
                evt.operational_state.posture_since.FromNanoseconds(
                    int(round(new_posture_state.posture_since * 1e9))
                )
            evt.operational_state.posture_status = _POSTURE_TO_PROTO[new_posture_state.posture]
            await state_topic.send(key=aid, value=evt.SerializeToString())

        # 1a. Windowing — buffer this sample and (every N samples) emit a
        # WindowedTelemetry for the logistics fusion service.
        now_ns = int(time.time() * 1e9)
        _buffer_event(evt, now_ns)
        if aid:
            samples_since_last_emit[aid] = samples_since_last_emit.get(aid, 0) + 1
            if samples_since_last_emit[aid] >= EMIT_EVERY_N_SAMPLES:
                samples_since_last_emit[aid] = 0
                w = _emit_window_for_asset(evt, now_ns)
                if w is not None:
                    await windows_topic.send(
                        key=aid.encode(),
                        value=w.SerializeToString(),
                    )

        # 2. Anomaly Detection Pipeline
        view = _build_view(evt)

        # rec was already loaded above (shared with the posture state
        # machine) -> Convert to Algo State (AssetState)
        st = AssetState(
            last_temp_k=rec.last_temp_k,
            temp_ewma_k=rec.temp_ewma_k,
            temp_ewma_alpha=rec.temp_ewma_alpha
        )

        for algo in REGISTERED:
            anomaly = algo(view, st)
            if anomaly:
                # Convert Anomaly dataclass to CloudEvent-ish JSON for tactical-events
                ce = {
                    "specversion": "1.0",
                    "id": str(uuid.uuid4()),
                    # ADR-0023 §Faust-edge: source carries edge-id so a
                    # tactical-event handler that ever reads provenance
                    # sees it without a separate field-shape upgrade.
                    "source": f"openddil/edge/{OPENDDIL_EDGE_ID}/{view.asset_id}",
                    "type": f"openddil.anomaly.{anomaly.rule_id}",
                    "subject": view.asset_id,
                    "time": datetime.now(timezone.utc).isoformat(),
                    "datacontenttype": "application/json",
                    "data": {
                        "severity": anomaly.severity,
                        "summary": anomaly.summary,
                        "evidence": anomaly.evidence,
                        # 6a shape-now-fill-later: tactical_events handler
                        # still uses env-default in 6a; 6b upgrade reads
                        # these. Stamping now so the data is on the wire.
                        "edge_id": OPENDDIL_EDGE_ID,
                        "region_id": OPENDDIL_REGION_ID,
                    }
                }
                await events_topic.send(
                    key=view.asset_id,
                    value=json.dumps(ce).encode('utf-8')
                )

        # Sync back to Table. One write carrying both the anomaly-detection
        # state (AssetState -> StateRecord) and the posture fields already
        # set on `rec` above -- one read, one write, per record.
        rec.last_temp_k = st.last_temp_k or 0.0
        rec.temp_ewma_k = st.temp_ewma_k or 0.0
        rec.temp_ewma_alpha = st.temp_ewma_alpha
        asset_state[aid] = rec

register_prognostics(app)

if __name__ == "__main__":
    app.main()
