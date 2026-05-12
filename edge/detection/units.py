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
