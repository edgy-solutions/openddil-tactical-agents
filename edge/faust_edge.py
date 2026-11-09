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
from typing import Optional

import faust
from openddil.telemetry.v1 import telemetry_pb2 as pb
from openddil.logistics.v1 import windowed_telemetry_pb2 as winpb
from element_rollup import rollup_from_envelope
from element_band_event import build_band_event
from element_bands import BandTracker, parse_config as parse_band_config
from detection.windows import build_window_spec
# ADR-0044 amendment ("posture, a third column") -- a pure sibling module,
# no Faust/proto types (same boundary discipline as detection/units.py).
# Aliased so the StateRecord field named `posture` below never shadows it.
import posture as posture_mod
from posture_event import build_transition_event

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
_signal_ever_seen: dict[str, bool] = {}


def _posture_inputs(evt: "pb.EntityTelemetryEvent"):
    """Pull the state machine's three inputs out of the parsed event.

    Returns (deploy_signal, speed, now):
      - deploy_signal: the variant's deployment signal. The mapper passes at
        most ONE per variant: launcher_raised (launcher variants) or emitting
        (sensor variants). launcher_raised is checked first only as a
        tie-break that should never occur. True/False if the field was set,
        else None (the variant declares no signal, or no claim this tick) --
        HasField, not a truthiness check, so an explicit False is never
        confused with unset.
      - speed: |velocity| in m/s, computed from kinematics.velocity.ecef
        only (ground_speed is a separate, never-populated-for-DIS field --
        see the ADR-0044 amendment). None if no ecef velocity was sent.
      - now: provenance.sample_time as epoch seconds (the record's OWN event
        time, not wall clock, so replay stays deterministic).
    """
    op = evt.operational_state
    if op.HasField("launcher_raised"):
        deploy_signal = op.launcher_raised
    elif op.HasField("emitting"):
        deploy_signal = op.emitting
    else:
        deploy_signal = None
    vel = evt.kinematics.velocity
    if vel.WhichOneof("frame") == "ecef":
        v = vel.ecef
        speed = (v.x * v.x + v.y * v.y + v.z * v.z) ** 0.5
    else:
        speed = None
    ts = evt.provenance.sample_time
    now = ts.seconds + ts.nanos / 1e9
    return deploy_signal, speed, now

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

# Faust-managed state record for RocksDB serialization.
class StateRecord(faust.Record):
    # The three thermal fields are no longer written (thermal runs as the
    # declared unit `thermal`, with its own table); they stay so the existing
    # asset_state changelog still decodes.
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

# Element band events (edge/element_bands.py). The band config is rendered
# into ELEMENT_BANDS_JSON by the chart; absent means off, invalid fails the
# start (raised at import) rather than falling back to anything.
def load_band_config(text: str) -> "dict | None":
    """None (and one log line) when no config was rendered; ValueError when
    one was rendered but is invalid -- never a fallback."""
    if not text or not text.strip():
        logger.info("element band events are off: no band config was "
                    "rendered (ELEMENT_BANDS_JSON is absent or empty)")
        return None
    return parse_band_config(text)


band_config = load_band_config(os.getenv("ELEMENT_BANDS_JSON", ""))
band_tracker = BandTracker(band_config) if band_config else None


def band_events_for_envelope(env: dict, tracker, config: dict,
                             edge_id: str, region_id: str) -> list[dict]:
    """Band-crossing events for one decoded element envelope. Sample time is
    the envelope's observed_at_ns (the instant the rollup carries); an
    envelope without one yields nothing rather than a guessed time. Labels
    come from the envelope, as for the element window."""
    aid = env.get("asset_id")
    obs_ns = env.get("observed_at_ns")
    elements = env.get("elements")
    if (not aid or not isinstance(aid, str) or not elements
            or isinstance(obs_ns, bool) or not isinstance(obs_ns, (int, float))):
        return []
    nation = env.get("originator_nation") or ""
    releasable = env.get("releasable_to") or []
    if not isinstance(releasable, list):
        releasable = []
    releasable = [r for r in releasable if isinstance(r, str)]
    out = []
    for el in elements:
        if not isinstance(el, dict):
            continue
        path = el.get("element_id")
        if not path or not isinstance(path, str):
            continue
        for field, spec in config.items():
            value = el.get(field)
            crossing = tracker.update(aid, path, field, value)
            if crossing is None:
                continue
            frm, to = crossing
            out.append(build_band_event(
                aid, path, el.get("layer_name") or "", field, frm, to,
                float(value), obs_ns / 1e9, spec["severity"][to],
                nation if isinstance(nation, str) else "", releasable,
                edge_id, region_id,
            ))
    return out


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

        if band_tracker is not None:
            for ev in band_events_for_envelope(
                    env, band_tracker, band_config,
                    OPENDDIL_EDGE_ID, OPENDDIL_REGION_ID):
                await events_topic.send(
                    key=aid, value=json.dumps(ev).encode("utf-8"))

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

        # Load the posture state machine's state (ADR-0044 amendment) once
        # per record, read and written together.
        rec = asset_state[aid]

        # 0. Posture state machine (ADR-0044 amendment, "posture, a third
        # column"). Decided ONCE, here, at the owning (edge) tier; every
        # other tier (projector, store) only carries what we decide here,
        # never re-derives it.
        deploy_signal, speed, posture_now = _posture_inputs(evt)
        if deploy_signal is not None:
            _signal_ever_seen[aid] = True
        prev_posture_state = posture_mod.PostureState(
            posture=rec.posture,
            posture_since=rec.posture_since,
            motion=rec.motion,
            motion_since=rec.motion_since,
        )
        new_posture_state = posture_mod.step(
            prev_posture_state, deploy_signal, speed, posture_now, POSTURE_THRESHOLDS
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
        # carried a deployment signal at all (even a steady False) --
        # so traffic that never touches posture (no DIS appearance, or a
        # variant with no deployment signal) stays byte-identical on the
        # wire, exactly as it was before this feature existed.
        posture_untouched = (
            new_posture_state.posture == posture_mod.UNSPECIFIED
            and not _signal_ever_seen.get(aid, False)
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

        # 1.0 Append the transition as its own record on tactical-events,
        # after the latest-state send (compaction would otherwise lose it).
        if new_posture_state.posture != prev_posture_state.posture:
            await events_topic.send(
                key=aid,
                value=json.dumps(build_transition_event(
                    aid,
                    prev_posture_state,
                    new_posture_state,
                    deploy_signal,
                    speed,
                    evt.provenance.originator_nation,
                    list(evt.provenance.releasable_to),
                    OPENDDIL_EDGE_ID,
                    OPENDDIL_REGION_ID,
                )).encode("utf-8"),
            )

        # One write per record: the posture fields set on `rec` above.
        asset_state[aid] = rec

algorithm_runner = register_algorithms(app, window_rollup=element_rollups.get)

if __name__ == "__main__":
    app.main()
