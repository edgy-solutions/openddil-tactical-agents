# =============================================================================
# ADR-0007: Dual-path streaming architecture.
# Migration target is Quix Streams for sub-10ms p99 latency and native
# Protobuf+Schema Registry support. Faust is current; Quix is planned.
# DO NOT leak Faust types (faust.Record, faust.Stream, App) into
# detection/units.py or the declared units.
# =============================================================================
import json

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

import faust
from openddil.telemetry.v1 import telemetry_pb2 as pb
from openddil.logistics.v1 import windowed_telemetry_pb2 as winpb
from element_rollup import rollup_from_envelope
from detection.windows import build_window_spec
logger = logging.getLogger(__name__)
# Declared analytic units (ADR-0034, first slice; wear is the ADR-0020
# prognostics engine as one, thermal the runaway rule as another). `register(app)` below adds each enabled unit's
# own agent + Table; this is the *only* coupling. To extract the runner into
# its own service later, delete the import and the call site at the bottom of
# this file.
from algorithm_runtime import outputs as algorithm_outputs
from algorithm_runtime.runner import register as register_algorithms

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

app = faust.App(
    FAUST_APP_ID,
    broker=f"kafka://{KAFKA_BROKERS}",
    value_serializer="raw",
    # Element snapshots run to several MB per record; the per-partition fetch
    # default (1 MiB) is below that. Matches the element topic's
    # max.message.bytes so a record the broker accepts is one we can fetch.
    consumer_max_fetch_size=16 * 1024 * 1024,
)

events_topic = app.topic("tactical-events", value_type=bytes)
# Phase 3.5: rolling-window aggregations consumed by the logistics fusion
# service. Output records are openddil.telemetry.v1.WindowedTelemetry.
windows_topic = app.topic("asset-telemetry-windows", value_type=bytes)
# Element snapshots published on this edge's own broker, keyed by asset_id;
# one JSON envelope per asset per ~30 s, up to a few MB each.
ELEMENT_TOPIC = os.getenv("ELEMENT_TOPIC", "asset-element-telemetry")
element_topic = app.topic(ELEMENT_TOPIC, value_type=bytes)

# Rolling-window trends are the `trends` declared unit's (algorithms/trends).
# The runner keeps each asset's last window payload; the element path below
# reads it through `algorithm_runner.last_window`. Nothing windowed lives here.
# Latest element rollup per asset. Both window producers (the trends unit on
# the raw path and the element path) write the same key on the windows topic,
# so each attaches this cache and neither clears what the other contributed.
element_rollups: dict[str, winpb.ElementRollup] = {}
_rollup_logged: set[str] = set()
# Set by `register_algorithms` at the bottom of the file.
algorithm_runner = None

def _window_from_envelope(env: dict,
                          rollup: winpb.ElementRollup,
                          now_ns: int) -> winpb.WindowedTelemetry:
    """Window for the element path: the window spans the snapshot's own
    observation instant, and labels come from the envelope (ADR-0029
    PROPAGATE, no default: an envelope without labels gives an unlabelled
    window, which the egress gate then refuses). The rollup is attached from
    the cache, so the caller stores it there first.

    The sustainment trends are the `trends` unit's last window for this asset
    (the runner keeps it), carried as they were when the unit emitted them:
    they are not recomputed at this window's time, and they include what that
    payload carried (capacity and nsn on the ammunition entries, the latest
    fault codes). When the unit has emitted nothing for the asset yet the
    window has none."""
    aid = env.get("asset_id") or ""
    obs_ns = (rollup.observed_at.ToNanoseconds()
              if rollup.HasField("observed_at") else now_ns)
    releasable = env.get("releasable_to") or []
    if not isinstance(releasable, list):
        releasable = []
    nation = env.get("originator_nation") or ""
    last = algorithm_runner.last_window(aid) if algorithm_runner is not None else None

    out = winpb.WindowedTelemetry()
    trend_samples = 0
    if last is not None:
        try:
            out.CopyFrom(algorithm_outputs.build_payload("window", last))
            trend_samples = int(last["sample_count"])
        except (algorithm_outputs.OutputError, KeyError) as exc:
            logger.warning("element window for %s: last trends payload unusable: %s", aid, exc)
            out = winpb.WindowedTelemetry()
    out.asset_id = aid
    out.platform_variant = env.get("platform_variant") or ""
    out.computed_at.FromNanoseconds(now_ns)
    # ADR-0023 Phase 6b section A.2: origin-node provenance from this
    # instance's env (faust-edge is per-edge).
    out.provenance.producer_id = "faust-edge"
    out.provenance.edge_id = OPENDDIL_EDGE_ID
    out.provenance.region_id = OPENDDIL_REGION_ID
    out.provenance.ingest_time.FromNanoseconds(now_ns)
    out.provenance.classification = "U"
    if isinstance(nation, str) and nation:
        out.provenance.originator_nation = nation
    out.provenance.releasable_to.extend(r for r in releasable if isinstance(r, str))
    out.window.CopyFrom(build_window_spec(
        window_start_ns=obs_ns,
        window_end_ns=obs_ns,
        sample_count=trend_samples,
    ))
    out.element_rollup.CopyFrom(rollup)
    return out


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


algorithm_runner = register_algorithms(app, window_rollup=element_rollups.get)

if __name__ == "__main__":
    app.main()
