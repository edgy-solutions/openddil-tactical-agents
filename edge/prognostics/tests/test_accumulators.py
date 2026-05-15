"""Pure-Python unit tests for the prognostics accumulators.

No faust, no Kafka. Tests the arithmetic the engine's mechanism rests on —
exactly what ADR-0020's scripted-OpenDIS scenarios assert at the unit
level (the scenario tests do the same at the integration level)."""
from __future__ import annotations

import math

from prognostics.accumulators import (
    AccumulatorState,
    observe_kinematic,
    observed_hours,
    record_round_fired,
)


# ---------------------------------------------------------------------------
# Distance accumulation
# ---------------------------------------------------------------------------
def test_first_observation_sets_state_no_distance_yet():
    """A single observation has no previous position to delta against —
    distance stays 0, the position is stored for the next call."""
    s = AccumulatorState()
    observe_kinematic(
        s, ecef_m=(0.0, 0.0, 0.0),
        pitch_deg=0.0, roll_deg=0.0, sample_time_ns=1_000_000_000,
    )
    assert s.cumulative_distance_m == 0.0
    assert s.last_ecef_m == (0.0, 0.0, 0.0)
    assert s.observed_time_first_ns == 1_000_000_000
    assert s.observed_time_last_ns == 1_000_000_000


def test_two_observations_along_x_accumulate_segment_length():
    s = AccumulatorState()
    observe_kinematic(s, ecef_m=(0.0, 0.0, 0.0),
                      pitch_deg=0.0, roll_deg=0.0, sample_time_ns=0)
    observe_kinematic(s, ecef_m=(1000.0, 0.0, 0.0),
                      pitch_deg=0.0, roll_deg=0.0, sample_time_ns=1_000_000_000)
    assert s.cumulative_distance_m == 1000.0


def test_distance_accumulates_across_segments():
    """1000 m along X, then 1000 m along Y — cumulative is 2000 m."""
    s = AccumulatorState()
    observe_kinematic(s, ecef_m=(0.0,    0.0, 0.0),
                      pitch_deg=0.0, roll_deg=0.0, sample_time_ns=0)
    observe_kinematic(s, ecef_m=(1000.0, 0.0, 0.0),
                      pitch_deg=0.0, roll_deg=0.0, sample_time_ns=1_000_000_000)
    observe_kinematic(s, ecef_m=(1000.0, 1000.0, 0.0),
                      pitch_deg=0.0, roll_deg=0.0, sample_time_ns=2_000_000_000)
    assert math.isclose(s.cumulative_distance_m, 2000.0)


def test_distance_is_euclidean_in_3d():
    """A diagonal segment in 3D: (0,0,0) → (3,4,12), euclidean = 13."""
    s = AccumulatorState()
    observe_kinematic(s, ecef_m=(0.0, 0.0, 0.0),
                      pitch_deg=0.0, roll_deg=0.0, sample_time_ns=0)
    observe_kinematic(s, ecef_m=(3.0, 4.0, 12.0),
                      pitch_deg=0.0, roll_deg=0.0, sample_time_ns=1)
    assert math.isclose(s.cumulative_distance_m, 13.0)


# ---------------------------------------------------------------------------
# Observed-time window
# ---------------------------------------------------------------------------
def test_observed_hours_from_time_window():
    s = AccumulatorState()
    observe_kinematic(s, ecef_m=(0.0, 0.0, 0.0),
                      pitch_deg=None, roll_deg=None, sample_time_ns=0)
    observe_kinematic(s, ecef_m=(0.0, 0.0, 0.0),
                      pitch_deg=None, roll_deg=None,
                      sample_time_ns=3_600_000_000_000)  # +1 hour
    assert math.isclose(observed_hours(s), 1.0)


def test_observed_hours_zero_before_any_observation():
    assert observed_hours(AccumulatorState()) == 0.0


def test_observed_time_takes_max_not_blind_overwrite():
    """Out-of-order timestamps must not collapse the observed window."""
    s = AccumulatorState()
    observe_kinematic(s, ecef_m=(0.0, 0.0, 0.0),
                      pitch_deg=None, roll_deg=None, sample_time_ns=1_000_000_000)
    observe_kinematic(s, ecef_m=(1.0, 0.0, 0.0),
                      pitch_deg=None, roll_deg=None, sample_time_ns=10_000_000_000)
    observe_kinematic(s, ecef_m=(2.0, 0.0, 0.0),
                      pitch_deg=None, roll_deg=None, sample_time_ns=5_000_000_000)
    # Window remains [1, 10] — 5 was older than 10 and must not overwrite.
    assert s.observed_time_first_ns == 1_000_000_000
    assert s.observed_time_last_ns == 10_000_000_000


# ---------------------------------------------------------------------------
# Terrain integral
# ---------------------------------------------------------------------------
def test_terrain_integral_zero_on_flat_segment():
    s = AccumulatorState()
    observe_kinematic(s, ecef_m=(0.0, 0.0, 0.0),
                      pitch_deg=0.0, roll_deg=0.0, sample_time_ns=0)
    observe_kinematic(s, ecef_m=(1000.0, 0.0, 0.0),
                      pitch_deg=0.0, roll_deg=0.0, sample_time_ns=1)
    assert s.terrain_integral_deg_km == 0.0


def test_terrain_integral_trapezoidal_over_segment():
    """1 km segment with avg |pitch|+|roll| = (10+10)/2 + (0+0)/2 = 10
    → integral = 10 deg × 1 km = 10 deg·km."""
    s = AccumulatorState()
    observe_kinematic(s, ecef_m=(0.0, 0.0, 0.0),
                      pitch_deg=10.0, roll_deg=0.0, sample_time_ns=0)
    observe_kinematic(s, ecef_m=(1000.0, 0.0, 0.0),
                      pitch_deg=10.0, roll_deg=0.0, sample_time_ns=1)
    # avg |pitch| = 10, avg |roll| = 0, sum = 10, × 1 km = 10
    assert math.isclose(s.terrain_integral_deg_km, 10.0)


def test_terrain_integral_uses_absolute_values():
    """Sign of pitch/roll doesn't matter — |·| in the integrand."""
    s = AccumulatorState()
    observe_kinematic(s, ecef_m=(0.0, 0.0, 0.0),
                      pitch_deg=-15.0, roll_deg=20.0, sample_time_ns=0)
    observe_kinematic(s, ecef_m=(2000.0, 0.0, 0.0),
                      pitch_deg=-15.0, roll_deg=20.0, sample_time_ns=1)
    # |pitch|+|roll| = 35 deg, × 2 km = 70 deg·km
    assert math.isclose(s.terrain_integral_deg_km, 70.0)


def test_terrain_integral_skipped_when_attitude_missing():
    """If pitch/roll is None on either endpoint, integration skips that
    segment — distance still accumulates."""
    s = AccumulatorState()
    observe_kinematic(s, ecef_m=(0.0, 0.0, 0.0),
                      pitch_deg=None, roll_deg=None, sample_time_ns=0)
    observe_kinematic(s, ecef_m=(1000.0, 0.0, 0.0),
                      pitch_deg=10.0, roll_deg=10.0, sample_time_ns=1)
    assert s.cumulative_distance_m == 1000.0
    assert s.terrain_integral_deg_km == 0.0


# ---------------------------------------------------------------------------
# Rounds fired (shape only — see ADR-0020)
# ---------------------------------------------------------------------------
def test_record_round_fired_increments_count():
    s = AccumulatorState()
    record_round_fired(s)
    record_round_fired(s)
    assert s.rounds_fired == 2
