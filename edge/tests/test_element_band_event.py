"""
Tests for element band events: the event builder (edge/element_band_event.py),
the Schmitt-trigger tracker (edge/element_bands.py) and the faust_edge
element-path wiring. No broker: the agent's coroutine is driven with a fake
stream and fake topics.

Run with: py -3 -m pytest edge/tests/test_element_band_event.py -q
(contracts gen/python on PYTHONPATH)
"""
from __future__ import annotations

import asyncio
import json
import logging
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from element_band_event import (  # noqa: E402
    COUNT_EVENT_TYPE, EVENT_TYPE, build_band_count_event, build_band_event)
from element_bands import BandTracker, parse_config  # noqa: E402

GOLDEN_ID = "eb39f2b1-78f8-5ec7-88fa-c6cacc881f38"
CFG = {
    "health": {
        "limits": [0.90, 0.97],
        "names": ["ok", "degraded", "critical"],
        "hysteresis": 0.02,
        "severity": {"ok": "info", "degraded": "warn", "critical": "crit"},
    }
}


def _ev(nation="", rel=(), frm="ok", to="degraded"):
    return build_band_event("asset-1", "el-7", "psu", "health", frm, to,
                            0.93, 100.0, "warn", nation, rel, "edge-01", "region-01")


def test_golden_id():
    assert _ev()["id"] == GOLDEN_ID


def test_envelope_fields():
    ev = _ev()
    assert ev["type"] == EVENT_TYPE == "openddil.element.band"
    assert ev["specversion"] == "1.0"
    assert ev["source"] == "openddil/edge/edge-01/asset-1"
    assert ev["subject"] == "asset-1"
    assert ev["time"] == ev["data"]["at"] == "1970-01-01T00:01:40.000000+00:00"
    assert ev["datacontenttype"] == "application/json"
    d = ev["data"]
    assert (d["from"], d["to"], d["value"], d["severity"]) == ("ok", "degraded", 0.93, "warn")
    assert d["element_path"] == "el-7" and d["layer_name"] == "psu"


def test_labels_present():
    d = _ev(nation="ATL", rel=("BDR", "ATL"))["data"]
    assert d["originator_nation"] == "ATL"
    assert d["releasable_to"] == ["BDR", "ATL"]


def test_labels_absent():
    d = _ev(nation="", rel=("BDR",))["data"]
    assert "originator_nation" not in d and "releasable_to" not in d


def test_same_band_raises():
    with pytest.raises(ValueError):
        _ev(frm="ok", to="ok")


def test_schmitt_behaviour():
    t = BandTracker(CFG)
    k = ("a", "e", "health")
    assert t.update(*k, 0.50) is None          # first observation: no crossing
    assert t.update(*k, 0.91) == ("ok", "degraded")
    assert t.update(*k, 0.89) is None          # inside hysteresis (>= 0.88)
    assert t.update(*k, 0.905) is None
    assert t.update(*k, 0.87) == ("degraded", "ok")
    assert t.update(*k, 0.99) == ("ok", "critical")   # multi-band jump, one transition
    assert t.update(*k, 0.96) is None
    assert t.update(*k, 0.20) == ("critical", "ok")   # multi-band fall, one transition


def test_ignored_inputs():
    t = BandTracker(CFG)
    assert t.update("a", "e", "health", "bad") is None
    assert t.update("a", "e", "health", True) is None
    assert t.update("a", "e", "temp_c", 99.0) is None
    # ignored values did not set a band: next numeric is still the first observation
    assert t.update("a", "e", "health", 0.99) is None


def _cfg(**over):
    c = json.loads(json.dumps(CFG["health"]))
    c.update(over)
    return json.dumps({"health": c})


def test_parse_config_ok():
    assert parse_config(_cfg())["health"]["limits"] == [0.90, 0.97]


@pytest.mark.parametrize("text", [
    "not json", "[]", "{}",
    _cfg(names=["ok", "degraded"]),
    _cfg(limits=[0.97, 0.90]),
    _cfg(limits=[0.9, 0.9]),
    _cfg(hysteresis=-0.1),
    _cfg(hysteresis="x"),
    _cfg(severity={"ok": "info", "degraded": "warn"}),
    _cfg(severity={"ok": "info", "degraded": "warn", "critical": ""}),
])
def test_parse_config_rejects(text):
    with pytest.raises(ValueError):
        parse_config(text)


# Wiring: drive the real `process_elements` agent body with fake topics.
class _Fake:
    def __init__(self):
        self.sent = []

    async def send(self, key=None, value=None, **kw):
        self.sent.append((key, value))


def _run(envs, *, config):
    import faust_edge
    ev_t, win_t = _Fake(), _Fake()

    async def stream():
        for e in envs:
            yield json.dumps(e).encode()

    mp = pytest.MonkeyPatch()
    try:
        mp.setattr(faust_edge, "events_topic", ev_t)
        mp.setattr(faust_edge, "windows_topic", win_t)
        mp.setattr(faust_edge, "band_config", config)
        mp.setattr(faust_edge, "band_tracker", BandTracker(config) if config else None)
        asyncio.run(faust_edge.process_elements.fun(stream()))
    finally:
        mp.undo()
    return [json.loads(v) for _, v in ev_t.sent], ev_t, win_t


def _env(t_s, health):
    return {
        "asset_id": "a1", "profile_name": "p",
        "observed_at_ns": int(t_s * 1e9),
        "originator_nation": "ATL", "releasable_to": ["BDR"],
        "elements": [{"element_id": "e1", "layer_name": "psu", "health": health}],
    }


def test_wiring_off_without_config():
    sent, ev_t, win_t = _run([_env(1, 0.5), _env(2, 0.99)], config=None)
    assert sent == [] and ev_t.sent == []
    assert len(win_t.sent) == 2        # the rollup path is untouched


@pytest.mark.parametrize("text", ["", "   "])
def test_off_logs_exactly_one_line(caplog, text):
    import faust_edge
    with caplog.at_level(logging.INFO):
        assert faust_edge.load_band_config(text) is None
    lines = [r for r in caplog.records if "band events are off" in r.getMessage()]
    assert len(lines) == 1


def test_invalid_config_fails_at_start():
    import faust_edge
    with pytest.raises(ValueError):
        faust_edge.load_band_config('{"health": 1}')
    assert faust_edge.load_band_config(_cfg())["health"]["hysteresis"] == 0.02


def test_wiring_one_event_from_two_envelopes():
    sent, ev_t, _ = _run([_env(1, 0.5), _env(2, 0.99)], config=CFG)
    assert len(sent) == 1
    ev = sent[0]
    assert ev_t.sent[0][0] == "a1"
    assert ev["type"] == EVENT_TYPE
    d = ev["data"]
    assert (d["from"], d["to"], d["severity"]) == ("ok", "critical", "crit")
    assert d["element_path"] == "e1" and d["layer_name"] == "psu"
    assert d["originator_nation"] == "ATL" and d["releasable_to"] == ["BDR"]
    assert ev["time"] == "1970-01-01T00:00:02.000000+00:00"


# build_band_count_event and BandTracker.band
COUNT_GOLDEN_ID = "9dee5152-1928-50d0-ac4a-0938a41f9c59"
_C = {"ok": 1, "degraded": 2, "critical": 0}


def _cev(nation="", rel=(), baseline=True, delta=None):
    return build_band_count_event(
        "asset-1", "health", _C, {"psu": _C}, {"ok>degraded": 2}, 3, delta,
        baseline, 100.0, "warn", nation, rel, "edge-01", "region-01")


def test_count_golden_id():
    assert _cev()["id"] == COUNT_GOLDEN_ID


def test_count_envelope_and_data():
    ev = _cev()
    assert ev["type"] == COUNT_EVENT_TYPE == "openddil.element.bandcount"
    assert ev["source"] == "openddil/edge/edge-01/asset-1"
    assert ev["time"] == ev["data"]["at"] == "1970-01-01T00:01:40.000000+00:00"
    d = ev["data"]
    assert d["baseline"] is True and "delta" not in d
    assert d["counts"] == _C and d["by_layer"] == {"psu": _C} and d["elements"] == 3
    assert "originator_nation" not in d
    d2 = _cev(nation="ATL", rel=("BDR",), baseline=False, delta={"ok": 1})["data"]
    assert d2["baseline"] is False and d2["delta"] == {"ok": 1}
    assert d2["originator_nation"] == "ATL" and d2["releasable_to"] == ["BDR"]


def test_count_needs_delta_unless_baseline():
    with pytest.raises(ValueError):
        _cev(baseline=False, delta=None)


def test_tracker_band_name():
    t = BandTracker(CFG)
    assert t.band("a", "e", "health") is None
    t.update("a", "e", "health", 0.5)
    assert t.band("a", "e", "health") == "ok"
    t.update("a", "e", "health", 0.99)
    assert t.band("a", "e", "health") == "critical"
    assert t.band("a", "e", "temp_c") is None
