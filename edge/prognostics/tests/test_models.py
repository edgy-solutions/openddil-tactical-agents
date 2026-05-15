"""Pure-Python unit tests for the four wear models + the public
`derive_sustainment` entry point.

Per ADR-0020: known input, known answer, assert the arithmetic. These
tests assert the *mechanism* (the engine computes its authored formula
correctly), NOT that the coefficients are *right* (that is the deferred
validation phase). The same shape as the scripted-OpenDIS scenarios,
just at the unit boundary."""
from __future__ import annotations

import math

from openddil.telemetry.v1 import telemetry_pb2 as pb

from prognostics.accumulators import AccumulatorState
from prognostics.coefficients import WearCoefficients
from prognostics.models import derive_sustainment


def _coeffs(**overrides) -> WearCoefficients:
    """Test coefficients — explicit small numbers so the assertions are
    obvious. Real defaults are placeholders anyway (ADR-0020); tests
    construct what they need rather than relying on env."""
    return WearCoefficients(
        track_life_total_km=1000.0,
        engine_life_total_hours=1000.0,
        barrel_life_total_rounds=100,
        terrain_total_deg_km=1000.0,
        **overrides,
    )


# ---------------------------------------------------------------------------
# Empty state -> empty output (plus the wildcard provenance)
# ---------------------------------------------------------------------------
def test_empty_state_emits_no_components_but_carries_provenance():
    sm = derive_sustainment(AccumulatorState(), _coeffs())
    assert len(sm.wear.components) == 0
    assert "*" in sm.value_provenance
    assert sm.value_provenance["*"].origin == pb.ORIGIN_DERIVED


# ---------------------------------------------------------------------------
# Track wear (distance-driven)
# ---------------------------------------------------------------------------
def test_track_wear_appears_after_any_distance():
    state = AccumulatorState(cumulative_distance_m=250_000.0)  # 250 km
    sm = derive_sustainment(state, _coeffs())
    assert "track" in sm.wear.components
    ws = sm.wear.components["track"]
    # Natural-unit raw measure: distance in km.
    assert math.isclose(ws.hours_in_service.value, 250.0)
    assert ws.hours_in_service.unit == "km"
    # Universal answer: percent remaining (250/1000 consumed -> 75% remaining).
    assert math.isclose(ws.remaining_useful_life.value, 75.0)
    assert ws.remaining_useful_life.unit == "%"
    # 25% consumed — NOMINAL band (< 50%).
    assert ws.status == pb.WEAR_NOMINAL


def test_track_wear_status_progresses_with_distance():
    coeffs = _coeffs()
    for distance_km, expected_status in [
        (600, pb.WEAR_DEGRADED),     # 60% consumed
        (950, pb.WEAR_SERVICE_DUE),  # 95% consumed
        (1200, pb.WEAR_CRITICAL),    # past full life
    ]:
        state = AccumulatorState(cumulative_distance_m=distance_km * 1000.0)
        sm = derive_sustainment(state, coeffs)
        assert sm.wear.components["track"].status == expected_status, distance_km


def test_track_wear_absent_at_zero_distance():
    sm = derive_sustainment(AccumulatorState(), _coeffs())
    assert "track" not in sm.wear.components


# ---------------------------------------------------------------------------
# Engine hours (observed-time)
# ---------------------------------------------------------------------------
def test_engine_hours_from_observed_window():
    # 2-hour window: 2 × 3.6e12 ns.
    state = AccumulatorState(
        observed_time_first_ns=0,
        observed_time_last_ns=7_200_000_000_000,
    )
    sm = derive_sustainment(state, _coeffs())
    assert "engine" in sm.wear.components
    ws = sm.wear.components["engine"]
    # Natural-unit raw measure: observed time in hours.
    assert math.isclose(ws.hours_in_service.value, 2.0)
    assert ws.hours_in_service.unit == "h"
    # Universal answer: percent remaining (2/1000 consumed -> 99.8% remaining).
    assert math.isclose(ws.remaining_useful_life.value, 99.8)
    assert ws.remaining_useful_life.unit == "%"


def test_engine_hours_absent_when_no_time_observed():
    sm = derive_sustainment(AccumulatorState(), _coeffs())
    assert "engine" not in sm.wear.components


# ---------------------------------------------------------------------------
# Barrel life (rounds-through-tube)
# ---------------------------------------------------------------------------
def test_barrel_life_absent_when_no_rounds_fired():
    """Fire/Detonation PDU ingestion is the follow-on wiring step
    (ADR-0020). Until rounds-fired > 0, the model emits nothing."""
    state = AccumulatorState(rounds_fired=0)
    sm = derive_sustainment(state, _coeffs())
    assert "barrel" not in sm.wear.components


def test_barrel_life_emits_after_rounds_fired():
    state = AccumulatorState(rounds_fired=25)
    sm = derive_sustainment(state, _coeffs())
    assert "barrel" in sm.wear.components
    ws = sm.wear.components["barrel"]
    # Natural-unit raw measure: round count (UCUM "1", dimensionless).
    assert ws.cycles.value == 25.0
    assert ws.cycles.unit == "1"
    # Universal answer: percent remaining (25/100 -> 75% remaining).
    assert ws.remaining_useful_life.value == 75.0
    assert ws.remaining_useful_life.unit == "%"


def test_barrel_life_critical_past_total():
    state = AccumulatorState(rounds_fired=150)
    sm = derive_sustainment(state, _coeffs())
    ws = sm.wear.components["barrel"]
    # Clamped to 0% remaining when past total life.
    assert ws.remaining_useful_life.value == 0.0
    assert ws.remaining_useful_life.unit == "%"
    assert ws.status == pb.WEAR_CRITICAL


# ---------------------------------------------------------------------------
# Suspension (terrain integral)
# ---------------------------------------------------------------------------
def test_suspension_absent_at_zero_terrain():
    sm = derive_sustainment(AccumulatorState(), _coeffs())
    assert "suspension" not in sm.wear.components


def test_suspension_consumes_proportionally():
    state = AccumulatorState(terrain_integral_deg_km=400.0)
    sm = derive_sustainment(state, _coeffs())
    ws = sm.wear.components["suspension"]
    # Natural-unit raw measure: terrain integral in deg·km.
    assert math.isclose(ws.hours_in_service.value, 400.0)
    assert ws.hours_in_service.unit == "deg.km"
    # Universal answer: percent remaining (400/1000 -> 60% remaining).
    assert math.isclose(ws.remaining_useful_life.value, 60.0)
    assert ws.remaining_useful_life.unit == "%"
    assert ws.status == pb.WEAR_NOMINAL  # 40% consumed


# ---------------------------------------------------------------------------
# Wildcard provenance contract — ADR-0020 message-level filling
# ---------------------------------------------------------------------------
def test_value_provenance_wildcard_always_set_when_state_has_anything():
    state = AccumulatorState(cumulative_distance_m=1000.0)
    sm = derive_sustainment(state, _coeffs())
    assert "*" in sm.value_provenance
    vp = sm.value_provenance["*"]
    assert vp.origin == pb.ORIGIN_DERIVED
    # Phase 5: confidence is the documented placeholder (no real meaning).
    assert vp.confidence == 0.0


def test_no_per_field_provenance_entries_in_phase_5():
    """ADR-0020 ruling: per-value map shape is forward-looking; Phase 5
    populates ONLY the wildcard. Per-field keys arrive at the validation
    phase."""
    state = AccumulatorState(
        cumulative_distance_m=100_000.0,
        observed_time_first_ns=0,
        observed_time_last_ns=3_600_000_000_000,
    )
    sm = derive_sustainment(state, _coeffs())
    # Track + engine wear should be present; provenance should still be
    # wildcard-only (no `wear.track.*` or `wear.engine.*` keys).
    keys = set(sm.value_provenance.keys())
    assert keys == {"*"}, keys
