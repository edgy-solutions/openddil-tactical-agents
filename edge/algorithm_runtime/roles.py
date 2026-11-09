"""EntityTelemetryEvent -> role primitives.

The proto traversal lives here so a unit sees plain numbers under the names
in `contract.ROLES` and never a message type."""
from __future__ import annotations

import math
import time

from openddil.telemetry.v1 import telemetry_pb2 as pb


# Roles that cost a unit conversion (pint). They are computed only when a unit
# declares them, so a unit that does not read a temperature never pays for it.
_TEMPERATURES = {
    "component_temp_k": lambda evt: evt.sustainment.thermal.component_temperature,
    "ambient_temp_k": lambda evt: evt.sustainment.thermal.ambient_temperature,
}


def _quantity(q):
    """(magnitude, str(pint units)) for a proto Quantity, or None when it is
    unset (zero with no unit) or its unit is one pint cannot read: the test the
    old window buffers used (`windows.proto_to_pint`)."""
    from detection.windows import proto_to_pint  # pint is slow to import; only a unit that names a sustainment role loads it
    pq = proto_to_pint(q)
    if pq is None:
        return None
    return (pq.magnitude, str(pq.units))


def _sustainment(evt: pb.EntityTelemetryEvent, wanted, out: dict) -> None:
    """The sustainment roles, in the proto's map order (the old code's)."""
    def want(role):
        return wanted is None or role in wanted

    s = evt.sustainment
    if want("fuel_remaining"):
        q = _quantity(s.fluids.fuel_remaining)
        if q is not None:
            out["fuel_remaining"] = q
    if want("consumables"):
        out["consumables"] = {
            slot: {"remaining": float(item.quantity_remaining),
                   "capacity": item.quantity_capacity, "nsn": item.nsn}
            for slot, item in s.consumables.items.items()}
    for role, field in (("wear_hours", "hours_in_service"), ("wear_rul", "remaining_useful_life")):
        if want(role):
            found = {}
            for comp, state in s.wear.components.items():
                q = _quantity(getattr(state, field))
                if q is not None:
                    found[comp] = q
            out[role] = found
    if want("active_fault_codes"):
        out["active_fault_codes"] = tuple(s.health.active_fault_codes)


def extract(evt: pb.EntityTelemetryEvent, wanted=None) -> dict:
    """Role primitives for one event. `wanted` is the set of roles the unit
    declares (None: all); the temperature roles are converted only if named.

    Only ECEF is consumed (that is what the DIS sidecar produces): other
    frames leave `position_ecef_m` absent. Attitude is taken in degrees: a
    "rad" unit converts, any other non-empty unit is taken as degrees, an
    empty unit means the field was never set and the role is absent. A
    `valid_at` at or before the epoch falls back to the wall clock, and the
    sample then carries `sample_time_fallback` so the runner can count it;
    that flag is not a role units see.

    A temperature is absent when the source left it unset (the same test the
    original detection path used, `from_proto` returning None). One whose unit
    pint cannot convert to kelvin is absent too, and is listed under
    `role_unconvertible` for the runner to count: the original path raised
    inside the agent there, and a count is the deliberate replacement.

    The sustainment roles (`fuel_remaining`, `consumables`, `wear_hours`,
    `wear_rul`, `active_fault_codes`) are extracted only when named, like the
    temperatures. `record_time_ns` is not read from the event at all; the core
    adds it from the record's broker time."""
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

    unconvertible = []
    for role, get in _TEMPERATURES.items():
        if wanted is not None and role not in wanted:
            continue
        from detection.units import from_proto  # pint is slow to import; a unit with no temperature role never loads it
        try:
            q = from_proto(get(evt))
            if q is not None:
                out[role] = q.to("kelvin").magnitude
        except Exception:  # noqa: BLE001 - dimensionless/unknown unit: absent and counted, never a crash
            unconvertible.append(role)
    if unconvertible:
        out["role_unconvertible"] = unconvertible
    _sustainment(evt, wanted, out)
    return out
