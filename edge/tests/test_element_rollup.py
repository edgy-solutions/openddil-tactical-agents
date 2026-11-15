"""
Unit tests for edge/element_rollup.py and the element window the
`element_rollup` unit builds (algorithms/element_rollup, run here through the
runtime's `process_one`): the rollup's counts and means, its band edges, label
propagation, and that both window producers carry the cached rollup.

No broker and no faust_edge: the cache and the trends hook are plain dicts.

Run with: py -3 -m pytest edge/tests/test_element_rollup.py -q
(contracts gen/python on PYTHONPATH)
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from algorithm_runtime import core, declaration, guard, loader, outputs  # noqa: E402
from element_rollup import rollup_from_envelope  # noqa: E402
from openddil.telemetry.v1 import telemetry_pb2 as pb  # noqa: E402

HEALTH = [0.5, 0.91, 0.95, 0.97, 0.98, 1.0]
TEMPS = [20.0, 30.0, 40.0, 50.0, 60.0, 70.0]
LOADS = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0]
OBS_NS = 1_700_000_000_000_000_000


def _envelope(**extra) -> dict:
    env = {
        "asset_id": "asset-a",
        "platform_variant": "variant-x",
        "profile_name": "profile-1",
        "observed_at_ns": OBS_NS,
        "operational": {},
        "elements": [
            {"element_id": f"e{i}", "layer_depth": 0, "layer_name": "l",
             "health": h, "temp_c": t, "load_pct": ld,
             "tx_active": False, "rx_active": False}
            for i, (h, t, ld) in enumerate(zip(HEALTH, TEMPS, LOADS))
        ],
    }
    env.update(extra)
    return env


@pytest.fixture(autouse=True)
def _fixed_clock():
    outputs.set_clock(lambda: OBS_NS + 5)
    yield
    outputs.set_clock(None)


ROLLUP_DIR = Path(__file__).resolve().parents[2] / "algorithms" / "element_rollup"


def _rollup_unit():
    decl = declaration.load(ROLLUP_DIR / "unit.yaml")
    guard.check_unit_dir(ROLLUP_DIR)
    unit = loader.load_unit_class(ROLLUP_DIR, decl, RuntimeError)(declaration.resolve_params(decl, None))
    return unit, decl


class _LastWindow:
    """Stands in for the runner's store: the trends unit's last window payload
    per asset, as `Runner.last_window` hands it back."""

    def __init__(self, payloads=None):
        self.payloads = payloads or {}

    def last_window(self, aid):
        return self.payloads.get(aid)


@pytest.fixture
def last_windows():
    return _LastWindow()


def test_counts_means_and_band_edges():
    r = rollup_from_envelope(_envelope())
    assert r.element_count == 6
    assert r.critical_count == 2   # 0.98, 1.0
    assert r.degraded_count == 3   # 0.91, 0.95, and 0.97 (not critical)
    assert r.avg_temp_c == pytest.approx(45.0)
    assert r.avg_load_pct == pytest.approx(35.0)
    assert r.profile_name == "profile-1"
    assert r.observed_at.ToNanoseconds() == OBS_NS


def test_optional_readouts_follow_presence():
    r = rollup_from_envelope(_envelope(
        operational={"core_temp_c": 0.0, "uptime_hours": 12.5}))
    assert r.HasField("core_temp_c") and r.core_temp_c == 0.0
    assert r.HasField("uptime_hours") and r.uptime_hours == 12.5
    r = rollup_from_envelope(_envelope())
    assert not r.HasField("core_temp_c")
    assert not r.HasField("uptime_hours")


def test_non_numeric_temp_and_load_are_ignored():
    env = _envelope()
    env["elements"][0]["temp_c"] = None
    env["elements"][1]["load_pct"] = "n/a"
    r = rollup_from_envelope(env)
    assert r.avg_temp_c == pytest.approx(50.0)
    assert r.avg_load_pct == pytest.approx(38.0)


@pytest.mark.parametrize("env", [
    {"asset_id": "a"},
    {"asset_id": "a", "elements": []},
    {"asset_id": "a", "elements": None},
])
def test_empty_or_missing_elements_yield_none(env):
    assert rollup_from_envelope(env) is None


_NO_TRENDS = _LastWindow()


def _element_window(env: dict, last_windows=_NO_TRENDS):
    """The window the unit builds for one envelope, through the runtime."""
    import json
    unit, decl = _rollup_unit()
    _st, sends, _c = core.process_one(unit, decl, json.dumps(env).encode(), None, 0,
                                      window_trends=last_windows.last_window)
    assert len(sends) == 1
    return sends[0][2]


def test_element_window_carries_rollup_and_labels():
    w = _element_window(_envelope(originator_nation="XA",
                                  releasable_to=["XA", "XB"]))
    assert w.asset_id == "asset-a"
    assert w.platform_variant == "variant-x"
    assert w.element_rollup.element_count == 6
    assert w.provenance.originator_nation == "XA"
    assert list(w.provenance.releasable_to) == ["XA", "XB"]
    assert w.provenance.producer_id == "faust-edge"
    assert w.provenance.classification == "U"
    assert w.window.window_start.ToNanoseconds() == OBS_NS
    assert w.window.window_end.ToNanoseconds() == OBS_NS


def test_unlabelled_envelope_yields_unlabelled_window():
    w = _element_window(_envelope())
    assert w.provenance.originator_nation == ""
    assert list(w.provenance.releasable_to) == []
    assert w.element_rollup.element_count == 6


def _trend(latest, slope, unit="liter", n=3):
    return {"latest": latest, "latest_unit": unit, "slope": slope, "slope_unit": unit + "/h",
            "r_squared": 1.0, "latest_sample_ns": OBS_NS - 1_000_000_000,
            "window_start_ns": OBS_NS - 3_000_000_000, "sample_count": n}


def _payload(**kw):
    p = {"window_start_ns": OBS_NS - 3_000_000_000, "window_end_ns": OBS_NS, "sample_count": 3,
         "fluid_trends": {}, "consumable_trends": [], "wear_trends": [], "active_fault_codes": []}
    p.update(kw)
    return p


def test_trends_unit_window_carries_the_cached_rollup():
    """The raw path's window is now the trends unit's, built by the runtime
    with a rollup hook; here the hook is a dict's `get`, which the runner now
    fills from the element window the unit emits."""
    import shutil, tempfile  # noqa: E401
    from algorithm_runtime import core, declaration, guard, loader
    d = Path(__file__).resolve().parents[2] / "algorithms" / "trends"
    decl = declaration.load(d / "unit.yaml")
    guard.check_unit_dir(d)
    unit = loader.load_unit_class(d, decl, RuntimeError)(declaration.resolve_params(decl, {"emit_every_n_samples": 1}))
    evt = pb.EntityTelemetryEvent()
    evt.asset.asset_id = "asset-a"
    evt.asset.platform_variant = "variant-x"
    evt.sustainment.fluids.fuel_remaining.value = 50.0
    evt.sustainment.fluids.fuel_remaining.unit = "L"
    raw = evt.SerializeToString()

    rollups = {}
    _st, sends, _c = core.process_one(unit, decl, raw, None, 1_000_000_000,
                                      window_rollup=rollups.get)
    assert len(sends) == 1 and not sends[0][2].HasField("element_rollup")

    rollups["asset-a"] = rollup_from_envelope(_envelope())
    _st, sends, _c = core.process_one(unit, decl, raw, None, 2_000_000_000,
                                      window_rollup=rollups.get)
    assert sends[0][2].element_rollup.critical_count == 2


def test_element_window_carries_the_last_trends_payload(last_windows):
    last_windows.payloads["asset-a"] = _payload(fluid_trends={"fuel_remaining": _trend(98.0, -2.0)})
    w = _element_window(_envelope(), last_windows)
    assert "fuel_remaining" in w.fluid_trends
    assert w.fluid_trends["fuel_remaining"].latest.value == 98.0
    assert w.element_rollup.element_count == 6
    # the span is still the snapshot's own instant; the count is the payload's
    assert w.window.window_start.ToNanoseconds() == OBS_NS
    assert w.window.sample_count == 3


def test_element_window_without_a_trends_window_has_no_trends(last_windows):
    w = _element_window(_envelope(), last_windows)
    assert len(w.fluid_trends) == 0 and len(w.consumable_trends) == 0 and len(w.wear_trends) == 0
    assert w.window.sample_count == 0 and w.element_rollup.element_count == 6


def test_element_window_does_not_mix_assets(last_windows):
    last_windows.payloads["asset-b"] = _payload(fluid_trends={"fuel_remaining": _trend(1.0, 0.0)})
    assert len(_element_window(_envelope(), last_windows).fluid_trends) == 0


def test_element_window_survives_an_unusable_trends_payload(last_windows):
    last_windows.payloads["asset-a"] = {"not": "a window"}
    w = _element_window(_envelope(), last_windows)
    assert len(w.fluid_trends) == 0 and w.element_rollup.element_count == 6


COND = {
    "level": "CONDITION_LEVEL_DEGRADED",
    "moved_by": ["CONDITION_SOURCE_EMISSION"],
    "claims": [
        {"source": "CONDITION_SOURCE_APPEARANCE_DAMAGE", "level": "CONDITION_LEVEL_NOMINAL",
         "detail": "damage NONE", "observed_at": "2026-10-08T12:00:00Z"},
        {"source": "CONDITION_SOURCE_EMISSION", "level": "CONDITION_LEVEL_DEGRADED",
         "detail": "2/4 beams", "observed_at": "2026-10-08T11:59:58Z"},
    ],
}


def test_condition_is_carried_onto_the_rollup():
    r = rollup_from_envelope(_envelope(operational={"condition": COND}))
    assert r.HasField("condition")
    assert r.condition.level == pb.CONDITION_LEVEL_DEGRADED
    assert list(r.condition.moved_by) == [pb.CONDITION_SOURCE_EMISSION]
    assert [c.detail for c in r.condition.claims] == ["damage NONE", "2/4 beams"]
    assert r.condition.claims[1].observed_at.seconds > 0


def test_absent_or_malformed_condition_leaves_it_unset():
    assert not rollup_from_envelope(_envelope()).HasField("condition")
    bad = rollup_from_envelope(_envelope(operational={"condition": {"level": "NOT_A_LEVEL"}}))
    assert not bad.HasField("condition")
    assert bad.element_count == len(HEALTH)
