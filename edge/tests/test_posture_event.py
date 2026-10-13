"""
Tests for the posture transition event (edge/posture_event.py) and its
emission from the faust_edge `process` agent. No broker: the agent's
underlying coroutine is driven with a fake stream and fake topics.

Run with: py -3 -m pytest edge/tests/test_posture_event.py -q
(contracts gen/python on PYTHONPATH)
"""
from __future__ import annotations

import asyncio
import json
import sys
from collections import defaultdict
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import posture as posture_mod  # noqa: E402
from posture import PostureState  # noqa: E402
from posture_event import build_transition_event  # noqa: E402

EMPLACED = PostureState("emplaced", 0.0, "stationary", 0.0)
MARCH = PostureState("march_ordered", 100.0, "stationary", 0.0)
T100 = "1970-01-01T00:01:40.000000+00:00"


def _ev(prev=EMPLACED, new=MARCH, deploy=False, speed=0.0, nation="", rel=()):
    return build_transition_event(
        "asset-1", prev, new, deploy, speed, nation, rel, "edge-01", "region-01"
    )


def test_march_order_event():
    ev = _ev()
    assert ev["specversion"] == "1.0"
    assert ev["type"] == "openddil.posture.transition"
    assert ev["subject"] == "asset-1"
    assert ev["source"] == "openddil/edge/edge-01/asset-1"
    assert ev["datacontenttype"] == "application/json"
    assert ev["time"] == T100
    d = ev["data"]
    assert d["from"] == "emplaced" and d["to"] == "march_ordered"
    assert d["at"] == T100
    assert d["deploy_signal"] is False
    assert d["edge_id"] == "edge-01" and d["region_id"] == "region-01"


def test_id_deterministic_and_sensitive():
    assert _ev()["id"] == _ev()["id"]
    other_to = PostureState("moving", 100.0, "moving", 100.0)
    assert _ev(new=other_to)["id"] != _ev()["id"]
    other_at = PostureState("march_ordered", 101.0, "stationary", 0.0)
    assert _ev(new=other_at)["id"] != _ev()["id"]


def test_labels_carried_or_absent():
    d = _ev(nation="ATL", rel=["BDR"])["data"]
    assert d["originator_nation"] == "ATL"
    assert d["releasable_to"] == ["BDR"]
    d = _ev(nation="", rel=["BDR"])["data"]
    assert "originator_nation" not in d and "releasable_to" not in d


def test_none_values_stay_none():
    new = PostureState("march_ordered", 100.0, "", None)
    d = _ev(new=new, deploy=None, speed=None)["data"]
    assert "motion_since" in d and d["motion_since"] is None
    assert d["speed_mps"] is None and d["deploy_signal"] is None


def test_no_severity():
    ev = _ev()
    assert "severity" not in ev and "severity" not in ev["data"]


def test_unchanged_posture_raises():
    with pytest.raises(ValueError):
        _ev(prev=EMPLACED, new=PostureState("emplaced", 5.0, "stationary", 0.0))
    with pytest.raises(ValueError):
        _ev(new=PostureState("march_ordered", None, "", None))


# Wiring: drive the real `process` agent body with fake topics.
def test_one_send_per_transition():
    import faust_edge
    from openddil.telemetry.v1 import telemetry_pb2 as pb

    def mk(t, *, launcher, speed):
        e = pb.EntityTelemetryEvent()
        e.asset.asset_id = "a1"
        e.operational_state.launcher_raised = launcher
        e.kinematics.velocity.ecef.x = speed
        e.provenance.sample_time.FromNanoseconds(int(t * 1e9))
        e.provenance.originator_nation = "ATL"
        e.provenance.releasable_to.append("BDR")
        return e.SerializeToString()

    seq = [
        (0, True, 0.0),     # emplaced
        (10, False, 0.0),   # stow -> march_ordered
        (20, False, 5.0),   # start moving
        (32, False, 5.0),   # 12 s -> moving
        (33, False, 0.0),   # stop
        (54, False, 0.0),   # 21 s -> emplacing
        (60, True, 0.0),    # raise -> emplaced
    ]

    class Fake:
        def __init__(self):
            self.sent = []

        async def send(self, key=None, value=None, **kw):
            self.sent.append((key, value))

    ev_t, st_t, win_t = Fake(), Fake(), Fake()

    async def stream():
        for t, l, s in seq:
            yield mk(t, launcher=l, speed=s)

    mp = pytest.MonkeyPatch()
    try:
        mp.setattr(faust_edge, "events_topic", ev_t)
        mp.setattr(faust_edge, "state_topic", st_t)
        mp.setattr(faust_edge, "windows_topic", win_t)
        mp.setattr(faust_edge, "asset_state", defaultdict(faust_edge.StateRecord))
        mp.setattr(faust_edge, "REGISTERED", [])
        asyncio.run(faust_edge.process.fun(stream()))
    finally:
        mp.undo()

    sent = [json.loads(v) for _, v in ev_t.sent]
    tr = [e for e in sent if e["type"] == "openddil.posture.transition"]
    # first tick is unspecified -> emplaced, then four more transitions
    tos = [e["data"]["to"] for e in tr]
    assert tos[-4:] == ["march_ordered", "moving", "emplacing", "emplaced"]
    assert len(tr) == len(set(e["id"] for e in tr))
    assert tr[-1]["data"]["originator_nation"] == "ATL"
