"""Phase 5 prognostics engine — Faust wiring layer.

**This is the ONLY file in the `prognostics/` package that imports Faust.**
The pure modules (`models`, `accumulators`, `coefficients`) stay
framework-free so that:
  - they are unit-testable without Faust / Kafka,
  - when the engine extracts into its own service (ADR-0020 "inside
    faust-edge for now, movable later"), only this module changes:
    delete the call in faust_edge.py, give `prognostics/` its own
    `faust.App` and Dockerfile, point its agent at `raw-sensor-stream`.
    The deployment topology changes; the derivation logic does not.

The agent registered here is **a separate agent** from faust_edge.py's
`process` agent — distinct consumer, distinct Faust Table, distinct
output topic. That separation is the input/output seam (ADR-0020): the
engine does not reach into faust-edge's `window_buffers` or
`asset_state` Table, and faust-edge does not see prognostics state. The
one-line registration in faust_edge.py is the *only* coupling.

Output contract: `derived-sustainment` topic, `EntityTelemetryEvent`
protobuf with `sustainment.value_provenance['*']` carrying the Phase 5
DERIVED marker. That topic + that shape are the engine's entire
downstream contract; nothing learns that the producer happens to run
inside the faust-edge process today.
"""
from __future__ import annotations

import logging
import math
import time
import uuid

import faust
from openddil.telemetry.v1 import telemetry_pb2 as pb

from prognostics.accumulators import AccumulatorState, observe_kinematic
from prognostics.coefficients import WearCoefficients
from prognostics.models import derive_sustainment

log = logging.getLogger("prognostics")

INPUT_TOPIC = "raw-sensor-stream"
OUTPUT_TOPIC = "derived-sustainment"
TABLE_NAME = "prognostics_accumulators"


# ---------------------------------------------------------------------------
# Faust storage record — wraps the pure AccumulatorState for the Table.
# Kept here, in the only file that imports faust, so accumulators.py stays
# pure. Mirrors the faust_edge.py `StateRecord` / `AssetState` split.
# ---------------------------------------------------------------------------
class _AccumulatorRecord(faust.Record, serializer="json"):
    cumulative_distance_m:   float       = 0.0
    observed_time_first_ns:  int         = 0
    observed_time_last_ns:   int         = 0
    terrain_integral_deg_km: float       = 0.0
    rounds_fired:            int         = 0
    last_ecef_x:             float | None = None
    last_ecef_y:             float | None = None
    last_ecef_z:             float | None = None
    last_pitch_deg:          float | None = None
    last_roll_deg:           float | None = None


def _record_to_state(rec: _AccumulatorRecord) -> AccumulatorState:
    last_ecef = None
    if (rec.last_ecef_x is not None
            and rec.last_ecef_y is not None
            and rec.last_ecef_z is not None):
        last_ecef = (rec.last_ecef_x, rec.last_ecef_y, rec.last_ecef_z)
    return AccumulatorState(
        cumulative_distance_m=rec.cumulative_distance_m,
        observed_time_first_ns=rec.observed_time_first_ns,
        observed_time_last_ns=rec.observed_time_last_ns,
        terrain_integral_deg_km=rec.terrain_integral_deg_km,
        rounds_fired=rec.rounds_fired,
        last_ecef_m=last_ecef,
        last_pitch_deg=rec.last_pitch_deg,
        last_roll_deg=rec.last_roll_deg,
    )


def _state_to_record(state: AccumulatorState) -> _AccumulatorRecord:
    return _AccumulatorRecord(
        cumulative_distance_m=state.cumulative_distance_m,
        observed_time_first_ns=state.observed_time_first_ns,
        observed_time_last_ns=state.observed_time_last_ns,
        terrain_integral_deg_km=state.terrain_integral_deg_km,
        rounds_fired=state.rounds_fired,
        last_ecef_x=state.last_ecef_m[0] if state.last_ecef_m else None,
        last_ecef_y=state.last_ecef_m[1] if state.last_ecef_m else None,
        last_ecef_z=state.last_ecef_m[2] if state.last_ecef_m else None,
        last_pitch_deg=state.last_pitch_deg,
        last_roll_deg=state.last_roll_deg,
    )


# ---------------------------------------------------------------------------
# Kinematic extraction — proto fields -> primitives the accumulator takes.
# Confined to this module so the pure layer stays free of proto traversal.
# ---------------------------------------------------------------------------
def _extract_kinematic(evt: pb.EntityTelemetryEvent):
    """Return (ecef_m, pitch_deg, roll_deg, sample_time_ns) or None if the
    event has no ECEF position to drive the accumulator with.

    Phase 5 only consumes ECEF — that is what the DIS sidecar produces.
    Other frames (WGS84, ENU) fall through with no update; broadening is
    a non-blocking enhancement."""
    position = evt.kinematics.position
    if not position.HasField("ecef"):
        return None

    ecef_m = (position.ecef.x.value, position.ecef.y.value, position.ecef.z.value)

    pitch_deg = None
    roll_deg = None
    if evt.kinematics.attitude.HasField("euler"):
        euler = evt.kinematics.attitude.euler
        if euler.pitch.unit:
            pitch_deg = (math.degrees(euler.pitch.value)
                         if euler.pitch.unit == "rad" else euler.pitch.value)
        if euler.roll.unit:
            roll_deg = (math.degrees(euler.roll.value)
                        if euler.roll.unit == "rad" else euler.roll.value)

    valid_at = position.valid_at
    sample_time_ns = valid_at.seconds * 1_000_000_000 + valid_at.nanos
    if sample_time_ns <= 0:
        sample_time_ns = int(time.time() * 1e9)

    return ecef_m, pitch_deg, roll_deg, sample_time_ns


def _build_derived_event(
    src: pb.EntityTelemetryEvent,
    sustainment: pb.SustainmentMetrics,
) -> pb.EntityTelemetryEvent:
    """Build the `derived-sustainment` envelope: a fresh EntityTelemetryEvent
    carrying only the derived sustainment + identity + provenance. No
    kinematics are echoed — the engine's job is sustainment derivation;
    consumers of `derived-sustainment` correlate by asset_id with the
    `raw-sensor-stream` kinematics."""
    out = pb.EntityTelemetryEvent()
    out.event_id = str(uuid.uuid4())
    out.schema_revision = 1
    # Identity passthrough so downstream can correlate without joining.
    out.asset.asset_id = src.asset.asset_id
    out.asset.callsign = src.asset.callsign
    out.asset.platform_type = src.asset.platform_type
    out.asset.platform_variant = src.asset.platform_variant
    out.asset.force = src.asset.force
    out.sustainment.CopyFrom(sustainment)
    out.provenance.producer_id = "prognostics-derivation"
    out.provenance.source_protocol = "openddil.prognostics.v1"
    # source_sequence intentionally 0 — the engine is not a sequenced source.
    # sample_time reflects when the underlying SAMPLE was taken (the source
    # event's sample_time), not when the derivation ran. Carrying the source
    # time through keeps fusion's staleness check honest — otherwise every
    # derived event would look "fresh as of now" regardless of whether the
    # input feed has stopped.
    if src.provenance.sample_time.seconds or src.provenance.sample_time.nanos:
        out.provenance.sample_time.CopyFrom(src.provenance.sample_time)
    else:
        out.provenance.sample_time.GetCurrentTime()
    out.provenance.ingest_time.GetCurrentTime()
    out.provenance.classification = "U"
    # Origin-node provenance (ADR-0022 / ADR-0023). Inherit from the source
    # event if it carries them (raw-sensor-stream events do post-6a); fall
    # back to the env defaults of the faust-edge instance the prognostics
    # engine runs inside. Either way the derived event ends up edge-attributed.
    import os as _os
    out.provenance.edge_id = (
        src.provenance.edge_id or _os.getenv("OPENDDIL_EDGE_ID", "edge-01")
    )
    out.provenance.region_id = (
        src.provenance.region_id or _os.getenv("OPENDDIL_REGION_ID", "region-01")
    )
    return out


# ---------------------------------------------------------------------------
# Public registration — the one-line seam for faust_edge.py
# ---------------------------------------------------------------------------
def register(app: faust.App) -> None:
    """Register the prognostics engine into the host Faust app.

    Adds:
      - one `@app.agent` consumer of `raw-sensor-stream` (distinct from
        faust_edge.py's `process` agent — its own consumer in the app),
      - one durable `Table` (`prognostics_accumulators`, RocksDB-backed,
        changelog-replicated, asset_id-keyed — ADR-0022 compliant),
      - production of `EntityTelemetryEvent` to `derived-sustainment`.

    Idempotent at the *file* level — calling `register(app)` more than
    once on the same `app` would register duplicate agents/tables; the
    intended usage is a single call at faust_edge.py module load."""
    coeffs = WearCoefficients.from_env()

    raw_topic = app.topic(INPUT_TOPIC, value_type=bytes)
    derived_topic = app.topic(OUTPUT_TOPIC, value_type=bytes)

    accumulator_table = app.Table(
        TABLE_NAME,
        default=_AccumulatorRecord,
        # MUST match raw-sensor-stream's partition count. See
        # openddil-tactical-agents/README.md "Faust Tables — the
        # partition-count invariant" — the rule is strict-but-invisible
        # (Faust only enforces strictly when the app has >1 Table). If
        # you re-partition the source for parallelism, every Table in
        # the app must change together — no per-Table autonomy.
        partitions=1,
    )

    log.info(
        "prognostics engine registered: input=%s output=%s table=%s coeffs=%s",
        INPUT_TOPIC, OUTPUT_TOPIC, TABLE_NAME, coeffs,
    )

    @app.agent(raw_topic)
    async def prognostics_process(stream):
        async for raw in stream:
            try:
                evt = pb.EntityTelemetryEvent()
                evt.ParseFromString(raw)
            except Exception as exc:  # noqa: BLE001 - parse errors are skip-not-crash
                log.warning("prognostics: failed to parse EntityTelemetryEvent: %s", exc)
                continue

            asset_id = evt.asset.asset_id
            if not asset_id:
                continue

            kinematic = _extract_kinematic(evt)
            if kinematic is None:
                continue
            ecef_m, pitch_deg, roll_deg, sample_time_ns = kinematic

            # Update per-asset accumulator (durable Table) and re-derive.
            rec = accumulator_table[asset_id]
            state = _record_to_state(rec)
            observe_kinematic(
                state,
                ecef_m=ecef_m,
                pitch_deg=pitch_deg,
                roll_deg=roll_deg,
                sample_time_ns=sample_time_ns,
            )
            accumulator_table[asset_id] = _state_to_record(state)

            sustainment = derive_sustainment(state, coeffs)
            out = _build_derived_event(evt, sustainment)
            await derived_topic.send(
                key=asset_id.encode(),
                value=out.SerializeToString(),
            )
