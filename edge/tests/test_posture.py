"""
Unit tests for edge/posture.py's step() (ADR-0044 amendment, "posture, a
third column") -- the amendment's transition table, both negative cases, and
a replay of the fixture schedule reproducing its predicted transitions
exactly.

Pure unit tests, no Faust, no Kafka, no I/O: step() takes plain values.

Run with: py -3 -m pytest edge/tests/test_posture.py -q
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import posture  # noqa: E402
from posture import COLD_START, PostureState, Thresholds, step  # noqa: E402

DEFAULTS = Thresholds(move_speed_mps=1.0, move_hold_s=10.0, stop_hold_s=20.0)


# ---------------------------------------------------------------------------
# Transition-table rows, each in isolation
# ---------------------------------------------------------------------------

def test_cold_start_stationary_stowed_stays_unspecified():
    # "cold start (no prior state) | stationary, stowed | unspecified"
    s = step(COLD_START, launcher_raised=False, speed=0.0, now=0.0, th=DEFAULTS)
    assert s.posture == "unspecified"
    assert s.posture_since is None


def test_cold_start_stationary_raised_goes_emplaced_immediately():
    # row 3 (unspecified is in the from-set), no hold required
    s = step(COLD_START, launcher_raised=True, speed=0.0, now=5.0, th=DEFAULTS)
    assert s.posture == "emplaced"
    assert s.posture_since == 5.0


def test_any_but_moving_moving_held_enough_goes_moving():
    prev = PostureState("emplaced", 0.0, "moving", 50.0)
    # held = 60 - 50 = 10 >= move_hold_s(10)
    s = step(prev, launcher_raised=True, speed=5.0, now=60.0, th=DEFAULTS)
    assert s.posture == "moving"
    assert s.posture_since == 60.0


def test_any_but_moving_moving_held_not_enough_stays_put():
    prev = PostureState("emplaced", 0.0, "moving", 55.0)
    # held = 60 - 55 = 5 < 10
    s = step(prev, launcher_raised=True, speed=5.0, now=60.0, th=DEFAULTS)
    assert s.posture == "emplaced"
    assert s.posture_since == 0.0


def test_moving_stationary_held_enough_launcher_false_goes_emplacing():
    prev = PostureState("moving", 70.0, "stationary", 120.0)
    s = step(prev, launcher_raised=False, speed=0.0, now=140.0, th=DEFAULTS)
    assert s.posture == "emplacing"
    assert s.posture_since == 140.0


def test_moving_stationary_held_enough_launcher_true_goes_emplaced():
    prev = PostureState("moving", 70.0, "stationary", 120.0)
    s = step(prev, launcher_raised=True, speed=0.0, now=140.0, th=DEFAULTS)
    assert s.posture == "emplaced"
    assert s.posture_since == 140.0


def test_emplacing_stationary_raised_goes_emplaced_no_hold():
    prev = PostureState("emplacing", 140.0, "stationary", 120.0)
    s = step(prev, launcher_raised=True, speed=0.0, now=141.0, th=DEFAULTS)
    assert s.posture == "emplaced"
    assert s.posture_since == 141.0


def test_march_ordered_stationary_raised_goes_emplaced():
    prev = PostureState("march_ordered", 30.0, "stationary", 0.0)
    s = step(prev, launcher_raised=True, speed=0.0, now=40.0, th=DEFAULTS)
    assert s.posture == "emplaced"


def test_emplaced_launcher_false_goes_march_ordered_no_hold():
    prev = PostureState("emplaced", 0.0, "stationary", 0.0)
    s = step(prev, launcher_raised=False, speed=0.0, now=30.0, th=DEFAULTS)
    assert s.posture == "march_ordered"
    assert s.posture_since == 30.0


def test_no_launcher_bit_moves_to_moving_by_speed_rule_only():
    # "any | launcher_raised None | only moving or unspecified: moving by
    # the speed rule"
    prev = PostureState("unspecified", None, "moving", 0.0)
    s = step(prev, launcher_raised=None, speed=5.0, now=10.0, th=DEFAULTS)
    assert s.posture == "moving"


def test_no_launcher_bit_after_stophold_goes_unspecified_not_emplacing():
    prev = PostureState("moving", 10.0, "stationary", 20.0)
    s = step(prev, launcher_raised=None, speed=0.0, now=40.0, th=DEFAULTS)
    assert s.posture == "unspecified"
    assert s.posture_since == 40.0


def test_speed_none_holds_previous_motion_no_update():
    prev = PostureState("moving", 10.0, "moving", 10.0)
    s = step(prev, launcher_raised=True, speed=None, now=100.0, th=DEFAULTS)
    # motion/motion_since untouched; held grows, but since motion/_since are
    # literally carried over (not recomputed from a fresh observation), the
    # held-time check in row 2 still uses the untouched motion_since.
    assert s.motion == "moving"
    assert s.motion_since == 10.0
    # posture != "moving"? it's already moving, so row 1 doesn't re-fire;
    # row 2 requires motion == "stationary", which it isn't -- no transition.
    assert s.posture == "moving"


def test_power_plant_is_not_a_gate():
    # step() takes no power-plant input at all -- it structurally cannot
    # gate on it. This test exists to make that choice visible if someone
    # later tries to thread power_plant_on through the signature.
    import inspect
    params = list(inspect.signature(step).parameters)
    assert "power" not in " ".join(params).lower()


# ---------------------------------------------------------------------------
# Negative case: entity with only "0 stow", stationary -- stays unspecified
# ---------------------------------------------------------------------------

def test_only_stow_stays_unspecified_whole_run():
    state = COLD_START
    for t in range(0, 201):
        state = step(state, launcher_raised=False, speed=0.0, now=float(t), th=DEFAULTS)
    assert state.posture == "unspecified"
    assert state.posture_since is None


# ---------------------------------------------------------------------------
# Fixture replay: Launchers A/B, schedule 0 raise,30 stow,60 move,120 stop,
# 150 raise. Predicted (defaults): unspecified->emplaced@~0->march_ordered@
# ~30->moving@~70->emplacing@~140->emplaced@~150. 5 transitions.
# ---------------------------------------------------------------------------

_SCHEDULE = [(0, "raise"), (30, "stow"), (60, "move"), (120, "stop"), (150, "raise")]


def _schedule_state(schedule, t):
    launcher_raised = None
    moving = False
    for at, action in schedule:
        if at > t:
            break
        if action == "raise":
            launcher_raised = True
        elif action == "stow":
            launcher_raised = False
        elif action == "move":
            moving = True
        elif action == "stop":
            moving = False
    return launcher_raised, (5.0 if moving else 0.0)


def _replay(schedule, th, t_end=200):
    state = COLD_START
    transitions = []
    for t in range(0, t_end + 1):
        launcher_raised, speed = _schedule_state(schedule, t)
        new_state = step(state, launcher_raised, speed, float(t), th)
        if new_state.posture != state.posture:
            transitions.append((t, state.posture, new_state.posture))
        state = new_state
    return state, transitions


def test_fixture_replay_default_thresholds_matches_predictions():
    final, transitions = _replay(_SCHEDULE, DEFAULTS)
    assert transitions == [
        (0, "unspecified", "emplaced"),
        (30, "emplaced", "march_ordered"),
        (70, "march_ordered", "moving"),
        (140, "moving", "emplacing"),
        (150, "emplacing", "emplaced"),
    ]
    assert len(transitions) == 5
    assert final.posture == "emplaced"


# ---------------------------------------------------------------------------
# Negative case: POSTURE_STOP_HOLD_S=60 -- A goes moving at ~70, no emplacing,
# emplaced at ~180 (stationary 60s, raised).
# ---------------------------------------------------------------------------

def test_stop_hold_60_skips_emplacing_goes_straight_to_emplaced():
    th = Thresholds(move_speed_mps=1.0, move_hold_s=10.0, stop_hold_s=60.0)
    final, transitions = _replay(_SCHEDULE, th)
    assert transitions == [
        (0, "unspecified", "emplaced"),
        (30, "emplaced", "march_ordered"),
        (70, "march_ordered", "moving"),
        (180, "moving", "emplaced"),
    ]
    assert not any(to == "emplacing" for (_, _, to) in transitions)
    assert final.posture == "emplaced"


# ---------------------------------------------------------------------------
# Guard-flip evidence (left in as a permanent regression guard): the
# move-hold check must be >=, not >. Off-by-one would let held==move_hold_s
# fail to transition.
# ---------------------------------------------------------------------------

def test_move_hold_boundary_is_inclusive():
    prev = PostureState("emplaced", 0.0, "moving", 0.0)
    s = step(prev, launcher_raised=True, speed=5.0, now=DEFAULTS.move_hold_s, th=DEFAULTS)
    assert s.posture == "moving"
