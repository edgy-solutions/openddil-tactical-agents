"""
Bridge between protobuf Quantity messages and pint Quantity objects.
This is the ONLY place in the codebase that crosses the proto-to-pint
boundary. Algorithms import from here; they do not touch proto types.
"""
from __future__ import annotations
from pint import UnitRegistry, Quantity as PQ
from openddil.telemetry.v1 import telemetry_pb2 as pb

# Single registry instance — pint registries are not cheap to construct
# and quantities from different registries do not interoperate.
ureg = UnitRegistry()
ureg.default_format = "~"   # short symbol form

# UCUM uses "[degF]" but pint uses "degF". Bridge the dialect.
_UCUM_TO_PINT = {
    "[degF]": "degF",
    "[kn_i]": "knot",
    "gal_us": "gallon",
    "1": "dimensionless",
}

def from_proto(q: pb.Quantity) -> PQ | None:
    """Convert a proto Quantity to a pint Quantity. Returns None if unset."""
    if q is None or (q.value == 0.0 and not q.unit):
        return None
    unit = _UCUM_TO_PINT.get(q.unit, q.unit)
    return ureg.Quantity(q.value, unit)

def to_proto(pq: PQ, ucum_unit: str) -> pb.Quantity:
    """Convert a pint Quantity back to proto, expressed in the given UCUM unit."""
    pint_unit = _UCUM_TO_PINT.get(ucum_unit, ucum_unit)
    converted = pq.to(pint_unit)
    out = pb.Quantity()
    out.value = float(converted.magnitude)
    out.unit = ucum_unit
    return out


# ---------------------------------------------------------------------------
# Structured-message adapters (ADR-0013)
#
# These wrap the `from_proto` boundary for messages whose components are
# Quantity-typed. Algorithms call these once at the wrapper layer; algorithm
# logic itself sees pint Quantities only.
# ---------------------------------------------------------------------------


def euler_to_pint(euler: pb.EulerAngles) -> dict[str, PQ | None]:
    """Convert an EulerAngles message into a dict of pint Quantities.

    Keys are always present; values are None when the component is unset.
    """
    return {
        "yaw":   from_proto(euler.yaw)   if euler.HasField("yaw")   else None,
        "pitch": from_proto(euler.pitch) if euler.HasField("pitch") else None,
        "roll":  from_proto(euler.roll)  if euler.HasField("roll")  else None,
    }


def ecef_to_pint(pos: pb.EcefPosition) -> dict[str, PQ | None]:
    """Convert an EcefPosition into a dict of pint Quantities."""
    return {
        "x": from_proto(pos.x) if pos.HasField("x") else None,
        "y": from_proto(pos.y) if pos.HasField("y") else None,
        "z": from_proto(pos.z) if pos.HasField("z") else None,
    }


def wgs84_to_pint(pos: pb.Wgs84Position) -> dict[str, PQ | None]:
    """Convert a Wgs84Position into a dict of pint Quantities."""
    return {
        "lat": from_proto(pos.lat) if pos.HasField("lat") else None,
        "lon": from_proto(pos.lon) if pos.HasField("lon") else None,
        "alt": from_proto(pos.alt) if pos.HasField("alt") else None,
    }


def local_enu_to_pint(pos: pb.LocalEnu) -> dict[str, PQ | None]:
    """Convert a LocalEnu into a dict of pint Quantities."""
    return {
        "east":  from_proto(pos.east)  if pos.HasField("east")  else None,
        "north": from_proto(pos.north) if pos.HasField("north") else None,
        "up":    from_proto(pos.up)    if pos.HasField("up")    else None,
    }


def position_to_pint(pos: pb.Position) -> dict[str, PQ | None] | None:
    """Resolve a Position oneof to a dict of pint Quantities.

    Returns None if no frame is populated; otherwise returns the dict
    produced by the appropriate frame adapter.
    """
    which = pos.WhichOneof("frame")
    if which == "ecef":
        return ecef_to_pint(pos.ecef)
    if which == "wgs84":
        return wgs84_to_pint(pos.wgs84)
    if which == "local_enu":
        return local_enu_to_pint(pos.local_enu)
    return None


def vector3_to_pint(v: pb.Vector3) -> dict[str, PQ | None] | None:
    """Convert a Vector3 (e.g., velocity ECEF / acceleration) into pint.

    Vector3 carries a single `unit` shared by x/y/z. Returns None if the
    unit is empty (i.e., the field is unset).
    """
    if not v.unit:
        return None
    pint_unit = _UCUM_TO_PINT.get(v.unit, v.unit)
    return {
        "x": ureg.Quantity(v.x, pint_unit),
        "y": ureg.Quantity(v.y, pint_unit),
        "z": ureg.Quantity(v.z, pint_unit),
    }
