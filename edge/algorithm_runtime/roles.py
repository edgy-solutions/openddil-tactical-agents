"""EntityTelemetryEvent -> role primitives.

The proto traversal lives here so a unit sees plain numbers under the names
in `contract.ROLES` and never a message type."""
from __future__ import annotations

import copy
import math
import time

from openddil.telemetry.v1 import telemetry_pb2 as pb


# Roles that cost a unit conversion (pint). They are computed only when a unit
# declares them, so a unit that does not read a temperature never pays for it.
_TEMPERATURES = {
    "component_temp_k": lambda evt: evt.sustainment.thermal.component_temperature,
    "ambient_temp_k": lambda evt: evt.sustainment.thermal.ambient_temperature,
}


def posture_inputs(evt: pb.EntityTelemetryEvent):
    """Pull the posture state machine's three inputs out of the parsed event.

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


def extract_envelope(env: dict, wanted=None) -> dict:
    """Role primitives of a gated element envelope (a decoded JSON object)."""
    out: dict = {}
    if wanted is None or "observed_at_s" in wanted:
        obs_ns = env.get("observed_at_ns")
        if _is_json_number(obs_ns):
            out["observed_at_s"] = obs_ns / 1e9
    if wanted is None or "elements" in wanted:
        found = []
        for el in env.get("elements") or ():
            if not isinstance(el, dict):
                continue
            path = el.get("element_id")
            if not path or not isinstance(path, str):
                continue
            found.append((path, el.get("layer_name") or "", dict(el)))
        out["elements"] = tuple(found)
    if wanted is None or "element_values" in wanted:
        out["element_values"] = tuple(dict(el) for el in env.get("elements") or ())
    if wanted is None or "observed_at_ns" in wanted:
        obs_ns = env.get("observed_at_ns")
        out["observed_at_ns"] = int(obs_ns) if _is_json_number(obs_ns) else None
    if wanted is None or "profile_name" in wanted:
        out["profile_name"] = env.get("profile_name") or ""
    if wanted is None or "operational" in wanted:
        out["operational"] = copy.deepcopy(env.get("operational") or {})
    if wanted is None or "envelope_labels" in wanted:
        nation = env.get("originator_nation") or ""
        releasable = env.get("releasable_to") or []
        if not isinstance(releasable, list):
            releasable = []
        out["envelope_labels"] = (
            nation if isinstance(nation, str) else "",
            tuple(r for r in releasable if isinstance(r, str)))
    if wanted is None or "platform_variant" in wanted:
        out["platform_variant"] = env.get("platform_variant") or ""
    return out


def _is_json_number(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


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

    if wanted is None or wanted & {"deploy_signal", "speed_mps", "sample_time_s"}:
        # Present even when None: the posture unit needs to see the explicit
        # None (no signal / no velocity), which a missing key would not say.
        out["deploy_signal"], out["speed_mps"], out["sample_time_s"] = posture_inputs(evt)

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
