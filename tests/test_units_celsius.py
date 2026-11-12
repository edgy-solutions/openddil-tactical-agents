"""UCUM Celsius ("Cel") in the shared converter's table. Each test names the
caller of `detection.units._UCUM_TO_PINT` it pins and what that caller did
with "Cel" before the table had the entry (pint has no unit called "Cel")."""
from __future__ import annotations

import pytest
from openddil.common.v1 import quantity_pb2 as qpb
from openddil.telemetry.v1 import telemetry_pb2 as pb

from detection import units


def _q(value, unit):
    q = qpb.Quantity()
    q.value = value
    q.unit = unit
    return q


def _kelvin(value):
    return units.ureg.Quantity(value, "degC").to("kelvin").magnitude


# a. from_proto: it raised (pint undefined unit) on "Cel".
def test_from_proto_reads_cel_as_degc():
    pq = units.from_proto(_q(92.0, "Cel"))
    assert pq.check("[temperature]")
    assert pq.to("kelvin").magnitude == _kelvin(92.0)
    assert pq.to("kelvin").magnitude == pytest.approx(365.15)


# b. to_proto with "Cel": it raised; now it writes the Celsius value and round-trips with (a).
def test_to_proto_writes_cel_and_round_trips():
    kelvin = units.ureg.Quantity(365.15, "kelvin")
    out = units.to_proto(kelvin, "Cel")
    assert out.unit == "Cel" and out.value == pytest.approx(92.0)
    back = units.from_proto(out)
    assert back.to("kelvin").magnitude == pytest.approx(365.15)
    again = units.to_proto(units.from_proto(_q(92.0, "Cel")), "Cel")
    assert (again.value, again.unit) == (92.0, "Cel")


# c. vector3_to_pint with unit "Cel": it raised.
def test_vector3_to_pint_accepts_cel():
    v = pb.Vector3(x=1.0, y=2.0, z=3.0, unit="Cel")
    out = units.vector3_to_pint(v)
    assert set(out) == {"x", "y", "z"}
    for key, val in (("x", 1.0), ("y", 2.0), ("z", 3.0)):
        assert out[key].to("kelvin").magnitude == _kelvin(val)


# d. the from_proto wrappers: each raised on a "Cel" component. This pins the
# table's reach through them, not a meaningful geometry.
WRAPPERS = [
    (units.euler_to_pint, lambda: pb.EulerAngles(yaw=_q(1.0, "Cel")), "yaw"),
    (units.ecef_to_pint, lambda: pb.EcefPosition(x=_q(1.0, "Cel")), "x"),
    (units.wgs84_to_pint, lambda: pb.Wgs84Position(lat=_q(1.0, "Cel")), "lat"),
    (units.local_enu_to_pint, lambda: pb.LocalEnu(east=_q(1.0, "Cel")), "east"),
]


@pytest.mark.parametrize("fn,build,key", WRAPPERS, ids=[w[0].__name__ for w in WRAPPERS])
def test_from_proto_wrappers_accept_cel(fn, build, key):
    out = fn(build())
    assert out[key].to("kelvin").magnitude == _kelvin(1.0)


def test_position_to_pint_accepts_cel_through_the_wrappers():
    out = units.position_to_pint(pb.Position(ecef=pb.EcefPosition(x=_q(1.0, "Cel"))))
    assert out["x"].to("kelvin").magnitude == _kelvin(1.0)


# ---------------------------------------------------------------------------
# One shared table (openddil.common.v1.ucum). The service no longer keeps a copy.
# ---------------------------------------------------------------------------
from openddil.common.v1 import ucum  # noqa: E402

from detection import windows  # noqa: E402


# Identity: before the change each module held its own dict, so `is` failed.
def test_both_modules_use_the_shared_table_object():
    assert units._UCUM_TO_PINT is ucum.UCUM_TO_PINT
    assert windows._UCUM_TO_PINT is ucum.UCUM_TO_PINT


# windows.proto_to_pint: its copy lacked "Cel" and the function swallows the
# error, so it returned None.
def test_windows_proto_to_pint_reads_cel_as_degc():
    pq = windows.proto_to_pint(_q(92.0, "Cel"))
    assert pq is not None and pq.check("[temperature]")
    assert pq.to("kelvin").magnitude == pytest.approx(365.15)


# Every shared code must resolve, through each module's own table, to a unit in
# that module's own registry. Before the change windows' copy had no "Cel"
# entry, so its lookup failed.
@pytest.mark.parametrize("code", sorted(ucum.UCUM_TO_PINT))
def test_every_table_value_parses_in_each_registry(code):
    units.ureg.Unit(units._UCUM_TO_PINT[code])
    windows._ureg.Unit(windows._UCUM_TO_PINT[code])


# Old versus new, per consumer. RED-EXEMPT: the old table is frozen here
# verbatim, and the old and new code agree on it, so this passes on both.
# Cel is not in the old table and is covered by the tests above.
_OLD_TABLE = {
    "[degF]": "degF",
    "[kn_i]": "knot",
    "gal_us": "gallon",
    "1": "dimensionless",
}
_MAGNITUDES = [0.0, -40.0, 1.5, 92.0, 1234.5]


def _same(new, reg, code, value):
    old = reg.Quantity(value, _OLD_TABLE.get(code, code)).to_base_units()
    new = new.to_base_units()
    assert new.magnitude == old.magnitude
    assert new.units == old.units


@pytest.mark.parametrize("value", _MAGNITUDES)
@pytest.mark.parametrize("code", sorted(_OLD_TABLE))
def test_old_codes_convert_as_before_through_from_proto(code, value):
    _same(units.from_proto(_q(value, code)), units.ureg, code, value)


@pytest.mark.parametrize("value", _MAGNITUDES)
@pytest.mark.parametrize("code", sorted(_OLD_TABLE))
def test_old_codes_convert_as_before_through_windows(code, value):
    _same(windows.proto_to_pint(_q(value, code)), windows._ureg, code, value)
