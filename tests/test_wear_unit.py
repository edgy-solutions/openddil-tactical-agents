"""The wear unit's own tests: the 25 cases of the original engine's two
suites (accumulators, then models), ported to the declared unit with the same
assertions. Known input, known answer (ADR-0020): these check the mechanism
computes its authored formula, not that the coefficients are right."""
from __future__ import annotations

import math

from openddil.telemetry.v1 import telemetry_pb2 as pb

from algorithm_runtime import core, outputs
from unit_helpers import make_wear

SMALL = dict(track_life_total_km=1000.0, engine_life_total_hours=1000.0,
             barrel_life_total_rounds=100, terrain_total_deg_km=1000.0)


def fresh(**fields):
    unit, decl = make_wear(SMALL)
    state = core.default_state(decl)
    state.update(fields)
    return unit, state


def observe(unit, state, ecef, pitch=None, roll=None, t=0):
    sample = {"position_ecef_m": ecef, "sample_time_ns": t}
    if pitch is not None:
        sample["pitch_deg"] = pitch
    if roll is not None:
        sample["roll_deg"] = roll
    unit.step(sample, state)


def derive(unit, state) -> pb.SustainmentMetrics:
    return outputs.build_payload("sustainment", unit.derive(state))


def last_ecef(s):
    return (s["last_ecef_x"], s["last_ecef_y"], s["last_ecef_z"])


# --- accumulators: distance -------------------------------------------------
def test_first_observation_sets_state_no_distance_yet():
    u, s = fresh()
    observe(u, s, (0.0, 0.0, 0.0), 0.0, 0.0, 1_000_000_000)
    assert s["cumulative_distance_m"] == 0.0
    assert last_ecef(s) == (0.0, 0.0, 0.0)
    assert s["observed_time_first_ns"] == 1_000_000_000
    assert s["observed_time_last_ns"] == 1_000_000_000


def test_two_observations_along_x_accumulate_segment_length():
    u, s = fresh()
    observe(u, s, (0.0, 0.0, 0.0), 0.0, 0.0, 0)
    observe(u, s, (1000.0, 0.0, 0.0), 0.0, 0.0, 1_000_000_000)
    assert s["cumulative_distance_m"] == 1000.0


def test_distance_accumulates_across_segments():
    u, s = fresh()
    observe(u, s, (0.0, 0.0, 0.0), 0.0, 0.0, 0)
    observe(u, s, (1000.0, 0.0, 0.0), 0.0, 0.0, 1_000_000_000)
    observe(u, s, (1000.0, 1000.0, 0.0), 0.0, 0.0, 2_000_000_000)
    assert math.isclose(s["cumulative_distance_m"], 2000.0)


def test_distance_is_euclidean_in_3d():
    u, s = fresh()
    observe(u, s, (0.0, 0.0, 0.0), 0.0, 0.0, 0)
    observe(u, s, (3.0, 4.0, 12.0), 0.0, 0.0, 1)
    assert math.isclose(s["cumulative_distance_m"], 13.0)


# --- accumulators: observed-time window --------------------------------------
def observed_hours(s):
    return (s["observed_time_last_ns"] - s["observed_time_first_ns"]) / 3_600_000_000_000.0


def test_observed_hours_from_time_window():
    u, s = fresh()
    observe(u, s, (0.0, 0.0, 0.0), t=0)
    observe(u, s, (0.0, 0.0, 0.0), t=3_600_000_000_000)  # +1 hour
    assert math.isclose(observed_hours(s), 1.0)


def test_observed_hours_zero_before_any_observation():
    _, s = fresh()
    assert observed_hours(s) == 0.0


def test_observed_time_takes_max_not_blind_overwrite():
    u, s = fresh()
    observe(u, s, (0.0, 0.0, 0.0), t=1_000_000_000)
    observe(u, s, (1.0, 0.0, 0.0), t=10_000_000_000)
    observe(u, s, (2.0, 0.0, 0.0), t=5_000_000_000)
    # Window remains [1, 10] — 5 was older than 10 and must not overwrite.
    assert s["observed_time_first_ns"] == 1_000_000_000
    assert s["observed_time_last_ns"] == 10_000_000_000


# --- accumulators: terrain integral -------------------------------------------
def test_terrain_integral_zero_on_flat_segment():
    u, s = fresh()
    observe(u, s, (0.0, 0.0, 0.0), 0.0, 0.0, 0)
    observe(u, s, (1000.0, 0.0, 0.0), 0.0, 0.0, 1)
    assert s["terrain_integral_deg_km"] == 0.0


def test_terrain_integral_trapezoidal_over_segment():
    """1 km segment with avg |pitch|+|roll| = 10 -> 10 deg.km."""
    u, s = fresh()
    observe(u, s, (0.0, 0.0, 0.0), 10.0, 0.0, 0)
    observe(u, s, (1000.0, 0.0, 0.0), 10.0, 0.0, 1)
    assert math.isclose(s["terrain_integral_deg_km"], 10.0)


def test_terrain_integral_uses_absolute_values():
    u, s = fresh()
    observe(u, s, (0.0, 0.0, 0.0), -15.0, 20.0, 0)
    observe(u, s, (2000.0, 0.0, 0.0), -15.0, 20.0, 1)
    assert math.isclose(s["terrain_integral_deg_km"], 70.0)


def test_terrain_integral_skipped_when_attitude_missing():
    """No pitch/roll on either endpoint skips that segment's integral;
    distance still accumulates."""
    u, s = fresh()
    observe(u, s, (0.0, 0.0, 0.0), t=0)
    observe(u, s, (1000.0, 0.0, 0.0), 10.0, 10.0, 1)
    assert s["cumulative_distance_m"] == 1000.0
    assert s["terrain_integral_deg_km"] == 0.0


# --- rounds fired (shape only, ADR-0020) ----------------------------------------
def test_record_round_fired_increments_count():
    u, s = fresh()
    u.record_round_fired(s)
    u.record_round_fired(s)
    assert s["rounds_fired"] == 2


# --- models ---------------------------------------------------------------------
def test_empty_state_emits_no_components_but_carries_provenance():
    u, s = fresh()
    sm = derive(u, s)
    assert len(sm.wear.components) == 0
    assert "*" in sm.value_provenance
    assert sm.value_provenance["*"].origin == pb.ORIGIN_DERIVED


def test_track_wear_appears_after_any_distance():
    u, s = fresh(cumulative_distance_m=250_000.0)  # 250 km
    ws = derive(u, s).wear.components["track"]
    assert math.isclose(ws.hours_in_service.value, 250.0)
    assert ws.hours_in_service.unit == "km"
    assert math.isclose(ws.remaining_useful_life.value, 75.0)
    assert ws.remaining_useful_life.unit == "%"
    assert ws.status == pb.WEAR_NOMINAL


def test_track_wear_status_progresses_with_distance():
    for distance_km, expected_status in [
        (600, pb.WEAR_DEGRADED),
        (950, pb.WEAR_SERVICE_DUE),
        (1200, pb.WEAR_CRITICAL),
    ]:
        u, s = fresh(cumulative_distance_m=distance_km * 1000.0)
        assert derive(u, s).wear.components["track"].status == expected_status, distance_km


def test_track_wear_absent_at_zero_distance():
    u, s = fresh()
    assert "track" not in derive(u, s).wear.components


def test_engine_hours_from_observed_window():
    u, s = fresh(observed_time_first_ns=0, observed_time_last_ns=7_200_000_000_000)
    ws = derive(u, s).wear.components["engine"]
    assert math.isclose(ws.hours_in_service.value, 2.0)
    assert ws.hours_in_service.unit == "h"
    assert math.isclose(ws.remaining_useful_life.value, 99.8)
    assert ws.remaining_useful_life.unit == "%"


def test_engine_hours_absent_when_no_time_observed():
    u, s = fresh()
    assert "engine" not in derive(u, s).wear.components


def test_barrel_life_absent_when_no_rounds_fired():
    u, s = fresh(rounds_fired=0)
    assert "barrel" not in derive(u, s).wear.components


def test_barrel_life_emits_after_rounds_fired():
    u, s = fresh(rounds_fired=25)
    ws = derive(u, s).wear.components["barrel"]
    assert ws.cycles.value == 25.0
    assert ws.cycles.unit == "1"
    assert ws.remaining_useful_life.value == 75.0
    assert ws.remaining_useful_life.unit == "%"


def test_barrel_life_critical_past_total():
    u, s = fresh(rounds_fired=150)
    ws = derive(u, s).wear.components["barrel"]
    assert ws.remaining_useful_life.value == 0.0
    assert ws.remaining_useful_life.unit == "%"
    assert ws.status == pb.WEAR_CRITICAL


def test_suspension_absent_at_zero_terrain():
    u, s = fresh()
    assert "suspension" not in derive(u, s).wear.components


def test_suspension_consumes_proportionally():
    u, s = fresh(terrain_integral_deg_km=400.0)
    ws = derive(u, s).wear.components["suspension"]
    assert math.isclose(ws.hours_in_service.value, 400.0)
    assert ws.hours_in_service.unit == "deg.km"
    assert math.isclose(ws.remaining_useful_life.value, 60.0)
    assert ws.remaining_useful_life.unit == "%"
    assert ws.status == pb.WEAR_NOMINAL


def test_value_provenance_wildcard_always_set_when_state_has_anything():
    u, s = fresh(cumulative_distance_m=1000.0)
    vp = derive(u, s).value_provenance["*"]
    assert vp.origin == pb.ORIGIN_DERIVED
    assert vp.confidence == 0.0


def test_no_per_field_provenance_entries_in_phase_5():
    """Only the wildcard is populated; per-field keys arrive at the
    validation phase (ADR-0020)."""
    u, s = fresh(cumulative_distance_m=100_000.0,
                 observed_time_first_ns=0, observed_time_last_ns=3_600_000_000_000)
    assert set(derive(u, s).value_provenance.keys()) == {"*"}


# --- the unit's own output never carries provenance -------------------------------
def test_unit_payload_has_no_value_provenance():
    u, s = fresh(cumulative_distance_m=1000.0)
    assert "value_provenance" not in u.derive(s)
