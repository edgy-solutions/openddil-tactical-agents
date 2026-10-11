"""
Tests for the per-variant deployment signal feeding the posture state
machine: faust_edge._posture_inputs picks launcher_raised (launcher variants)
or emitting (sensor variants), and posture.step treats either as the
deployment signal.

No broker; the Faust app object is constructed but never started.

Run with: py -3 -m pytest edge/tests/test_deploy_signal.py -q
(contracts gen/python on PYTHONPATH, including the emitting field)
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import faust_edge  # noqa: E402
import posture as posture_mod  # noqa: E402
from openddil.telemetry.v1 import telemetry_pb2 as pb  # noqa: E402

TH = faust_edge.POSTURE_THRESHOLDS


def _evt(*, launcher=None, emitting=None, speed=None, t=0.0):
    evt = pb.EntityTelemetryEvent()
    if launcher is not None:
        evt.operational_state.launcher_raised = launcher
    if emitting is not None:
        evt.operational_state.emitting = emitting
    if speed is not None:
        evt.kinematics.velocity.ecef.x = speed
    evt.provenance.sample_time.FromNanoseconds(int(t * 1e9))
    return evt


def _run(seq, state=posture_mod.COLD_START):
    """seq: (t, kwargs) -> list of postures after each tick."""
    out = []
    for t, kw in seq:
        sig, speed, now = faust_edge._posture_inputs(_evt(t=t, **kw))
        state = posture_mod.step(state, sig, speed, now, TH)
        out.append(state.posture)
    return out


# (a) input selection
def test_inputs_emitting_true():
    assert faust_edge._posture_inputs(_evt(emitting=True))[0] is True


def test_inputs_emitting_false_is_honoured():
    assert faust_edge._posture_inputs(_evt(emitting=False))[0] is False


def test_inputs_neither_field_is_none():
    assert faust_edge._posture_inputs(_evt())[0] is None


def test_inputs_launcher_raised_value_passes_through():
    assert faust_edge._posture_inputs(_evt(launcher=True))[0] is True
    assert faust_edge._posture_inputs(_evt(launcher=False))[0] is False


# (b) sensor sequence
def test_sensor_sequence_emitting():
    seq = [
        (0, dict(emitting=True, speed=0.0)),    # cold start, radiating
        (10, dict(emitting=False, speed=0.0)),  # stops radiating
        (20, dict(emitting=False, speed=5.0)),  # starts moving
        (30, dict(emitting=False, speed=5.0)),  # moved past move hold
        (31, dict(emitting=False, speed=0.0)),  # stops
        (51, dict(emitting=False, speed=0.0)),  # stopped past stop hold
        (52, dict(emitting=True, speed=0.0)),   # radiating again
    ]
    assert _run(seq) == [
        "emplaced", "march_ordered", "march_ordered", "moving",
        "moving", "emplacing", "emplaced",
    ]


# (c) no signal declared: never a guessed emplaced
def test_no_signal_after_moving_is_unspecified_not_emplacing():
    seq = [
        (0, dict(speed=5.0)),
        (10, dict(speed=5.0)),
        (11, dict(speed=0.0)),
        (31, dict(speed=0.0)),
    ]
    assert _run(seq) == ["unspecified", "moving", "moving", "unspecified"]


# (d) launcher sequence unchanged
def test_launcher_sequence_unchanged():
    seq = [
        (0, dict(launcher=True, speed=0.0)),
        (10, dict(launcher=False, speed=0.0)),
        (20, dict(launcher=False, speed=5.0)),
        (30, dict(launcher=False, speed=5.0)),
        (31, dict(launcher=False, speed=0.0)),
        (51, dict(launcher=False, speed=0.0)),
        (52, dict(launcher=True, speed=0.0)),
    ]
    assert _run(seq) == [
        "emplaced", "march_ordered", "march_ordered", "moving",
        "moving", "emplacing", "emplaced",
    ]
