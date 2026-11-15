# FROZEN: the thermal path of edge/faust_edge.py at the parent commit (8ab3148), minus faust, test oracle only.
# Imported by test_thermal_equivalence.py, never edited. Each block below is that file's code verbatim
# (`_build_view`, the AssetState build, the REGISTERED loop, the CloudEvent dict, the `or 0.0` write-back);
# the only changes are the table record (a plain object holding the stored floats), the send (events are
# returned) and the edge/region constants (arguments).
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

from openddil.telemetry.v1 import telemetry_pb2 as pb

from detection.units import from_proto
from detection_v0.algorithms import REGISTERED, EventView, AssetState


class StoredRecord:
    """The three thermal fields of faust_edge's StateRecord, with its defaults."""
    def __init__(self):
        self.last_temp_k = 0.0
        self.temp_ewma_k = 0.0
        self.temp_ewma_alpha = 0.2


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


def feed(evt: pb.EntityTelemetryEvent, rec: StoredRecord, OPENDDIL_EDGE_ID: str,
         OPENDDIL_REGION_ID: str) -> list[bytes]:
    """The 'Anomaly Detection Pipeline' block of the faust agent for one
    record: the events it would have sent (as the bytes it sent), `rec`
    written back."""
    sent = []
    view = _build_view(evt)

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
            sent.append(json.dumps(ce).encode('utf-8'))

    rec.last_temp_k = st.last_temp_k or 0.0
    rec.temp_ewma_k = st.temp_ewma_k or 0.0
    rec.temp_ewma_alpha = st.temp_ewma_alpha
    return sent
