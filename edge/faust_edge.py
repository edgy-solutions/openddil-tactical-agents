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
import os
import time
from collections import deque

import faust
from openddil.telemetry.v1 import telemetry_pb2 as pb
from openddil.logistics.v1 import windowed_telemetry_pb2 as winpb
from detection.algorithms import REGISTERED, EventView, AssetState, Anomaly
from detection.units import from_proto
from detection.windows import (
    Sample,
    build_window_spec,
    compute_trend,
    proto_to_pint,
    trend_to_proto,
)
# Phase 5 prognostics derivation engine — the one-line seam (ADR-0020).
# `register(app)` below adds the engine's own agent + Table; this is the
# *only* coupling. To extract the engine into its own service later,
# delete the import and the call site at the bottom of this file.
from prognostics import register as register_prognostics

app = faust.App(
    "openddil-edge",
    broker="kafka://redpanda-edge:9092",
    value_serializer="raw",
)

raw_topic = app.topic("raw-sensor-stream", value_type=bytes)
state_topic = app.topic("telemetry-latest-state", value_type=bytes)
events_topic = app.topic("tactical-events", value_type=bytes)
# Phase 3.5: rolling-window aggregations consumed by the logistics fusion
# service. Output records are openddil.telemetry.v1.WindowedTelemetry.
windows_topic = app.topic("asset-telemetry-windows", value_type=bytes)

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


def _emit_window_for_asset(evt: pb.EntityTelemetryEvent,
                            now_ns: int) -> winpb.WindowedTelemetry | None:
    """Build a WindowedTelemetry from the buffered samples for one asset.
    Returns None if there isn't enough data yet (no trend possible)."""
    aid = evt.asset.asset_id
    buffers = window_buffers.get(aid, {})
    if not buffers:
        return None

    out = winpb.WindowedTelemetry()
    out.asset_id = aid
    out.platform_variant = evt.asset.platform_variant or ""
    out.computed_at.FromNanoseconds(now_ns)

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
        cs = evt.sustainment.consumables.items.get(slot)
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
    if list(evt.sustainment.health.active_fault_codes):
        out.active_fault_codes_latest.extend(
            list(evt.sustainment.health.active_fault_codes),
        )

    # WindowSpec — use the widest range we covered.
    out.window.CopyFrom(build_window_spec(
        window_start_ns=window_min_start or now_ns,
        window_end_ns=now_ns,
        sample_count=total_samples,
    ))
    return out


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


@app.agent(raw_topic)
async def process(stream):
    async for raw in stream:
        evt = pb.EntityTelemetryEvent()
        try:
            evt.ParseFromString(raw)
        except Exception as e:
            import logging
            logging.error(f"Failed to parse protobuf: {e}")
            continue

        # 1. Forward to latest-state (preserving original bytes)
        await state_topic.send(key=evt.asset.asset_id, value=raw)

        # 1a. Windowing — buffer this sample and (every N samples) emit a
        # WindowedTelemetry for the logistics fusion service.
        now_ns = int(time.time() * 1e9)
        _buffer_event(evt, now_ns)
        aid = evt.asset.asset_id
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
        
        # Load state from Table (StateRecord) -> Convert to Algo State (AssetState)
        rec = asset_state[view.asset_id]
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
                    "source": f"openddil/edge/{view.asset_id}",
                    "type": f"openddil.anomaly.{anomaly.rule_id}",
                    "subject": view.asset_id,
                    "time": datetime.now(timezone.utc).isoformat(),
                    "datacontenttype": "application/json",
                    "data": {
                        "severity": anomaly.severity,
                        "summary": anomaly.summary,
                        "evidence": anomaly.evidence
                    }
                }
                await events_topic.send(
                    key=view.asset_id,
                    value=json.dumps(ce).encode('utf-8')
                )

        # Sync back to Table (AssetState -> StateRecord)
        rec.last_temp_k = st.last_temp_k or 0.0
        rec.temp_ewma_k = st.temp_ewma_k or 0.0
        rec.temp_ewma_alpha = st.temp_ewma_alpha
        asset_state[view.asset_id] = rec

register_prognostics(app)

if __name__ == "__main__":
    app.main()
