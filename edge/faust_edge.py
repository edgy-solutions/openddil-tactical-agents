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
import faust
from openddil.telemetry.v1 import telemetry_pb2 as pb
from detection.algorithms import REGISTERED, EventView, AssetState, Anomaly
from detection.units import from_proto

app = faust.App(
    "openddil-edge",
    broker="kafka://redpanda-edge:9092",
    value_serializer="raw",
)

raw_topic = app.topic("raw-sensor-stream", value_type=bytes)
state_topic = app.topic("telemetry-latest-state", value_type=bytes)
events_topic = app.topic("tactical-events", value_type=bytes)

# Faust-managed state record for RocksDB serialization.
class StateRecord(faust.Record):
    last_temp_k: float = 0.0
    temp_ewma_k: float = 0.0
    temp_ewma_alpha: float = 0.2

asset_state = app.Table(
    "asset_state",
    default=StateRecord,
    partitions=8,
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

if __name__ == "__main__":
    app.main()
