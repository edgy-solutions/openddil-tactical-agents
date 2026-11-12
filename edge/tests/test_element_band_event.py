"""
Tests for the element band event builder (edge/element_band_event.py). The
agent that used to send these from faust_edge is gone: the element_bands unit
sends them (tests/test_element_bands_equivalence.py) and the element_rollup
unit sends the element window (tests/test_element_rollup_equivalence.py).

Run with: py -3 -m pytest edge/tests/test_element_band_event.py -q
(contracts gen/python on PYTHONPATH)
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from element_band_event import EVENT_TYPE, build_band_event  # noqa: E402

GOLDEN_ID = "eb39f2b1-78f8-5ec7-88fa-c6cacc881f38"
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
