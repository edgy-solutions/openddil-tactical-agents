"""EntityTelemetryEvent -> role primitives.

The proto traversal lives here so a unit sees plain numbers under the names
in `contract.ROLES` and never a message type."""
from __future__ import annotations

import math
import time

from openddil.telemetry.v1 import telemetry_pb2 as pb


def extract(evt: pb.EntityTelemetryEvent) -> dict:
    """Role primitives for one event.

    Only ECEF is consumed (that is what the DIS sidecar produces): other
    frames leave `position_ecef_m` absent. Attitude is taken in degrees: a
    "rad" unit converts, any other non-empty unit is taken as degrees, an
    empty unit means the field was never set and the role is absent. A
    `valid_at` at or before the epoch falls back to the wall clock, and the
    sample then carries `sample_time_fallback` so the runner can count it;
    that flag is not a role units see."""
    out: dict = {}
    position = evt.kinematics.position
    if position.HasField("ecef"):
        out["position_ecef_m"] = (
            position.ecef.x.value, position.ecef.y.value, position.ecef.z.value,
        )

    if evt.kinematics.attitude.HasField("euler"):
        euler = evt.kinematics.attitude.euler
        if euler.pitch.unit:
            out["pitch_deg"] = (math.degrees(euler.pitch.value)
                                if euler.pitch.unit == "rad" else euler.pitch.value)
        if euler.roll.unit:
            out["roll_deg"] = (math.degrees(euler.roll.value)
                               if euler.roll.unit == "rad" else euler.roll.value)

    valid_at = position.valid_at
    sample_time_ns = valid_at.seconds * 1_000_000_000 + valid_at.nanos
    if sample_time_ns <= 0:
        sample_time_ns = int(time.time() * 1e9)
        out["sample_time_fallback"] = True
    out["sample_time_ns"] = sample_time_ns
    return out
