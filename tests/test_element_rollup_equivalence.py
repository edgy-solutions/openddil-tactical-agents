"""The element_rollup unit against the frozen old in-line element window path
(tests/oracle/element_rollup_v0).

Both sides see the same element envelopes in the same order. The unit runs
through Wiring exactly as the agent does; the oracle is the old agent's body
(its gates, `rollup_from_envelope` and `_window_from_envelope`). "Now" is the
same fixed instant on both sides, and what each side sends to the windows
topic is compared as (topic, key, bytes), exactly. The envelopes come from the
oracle package's synth, plus hand-built cases."""
from __future__ import annotations

import copy
import json
import os
import shutil
from collections import Counter

import faust
import pytest
from openddil.logistics.v1 import windowed_telemetry_pb2 as winpb
from openddil.telemetry.v1 import telemetry_pb2 as pb

from algorithm_runtime import core
from algorithm_runtime import declaration as d
from algorithm_runtime import outputs, replay, runner
from algorithm_runtime.contract import Unit
from element_rollup_v0 import Oracle, synth
from unit_helpers import ROLLUP_DIR, ROOT, make_element_rollup

EDGE, REGION = "edge-tst", "region-tst"
ASSETS = ("asset-a", "asset-b", "asset-c")
NOW = 9_000_000_000


def envelope(asset="a1", t_s=1.0, elements=None, **over):
    env = {"asset_id": asset, "profile_name": "p", "observed_at_ns": int(t_s * 1e9),
           "platform_variant": "v1", "originator_nation": "ATL", "releasable_to": ["BDR"],
           "elements": elements if elements is not None else [
               {"element_id": "e1", "layer_name": "psu", "health": 0.5, "temp_c": 40.0, "load_pct": 10.0}]}
    env.update(over)
    return env


def raw(env):
    return json.dumps(env).encode("utf-8")


def els(*healths):
    return [{"element_id": f"e{i}", "layer_name": "psu", "health": h, "temp_c": 40.0 + i, "load_pct": 5.0 * i}
            for i, h in enumerate(healths)]


@pytest.fixture(autouse=True)
def _clock():
    outputs.set_clock(lambda: NOW)
    yield
    outputs.set_clock(None)


class Rig:
    """The unit as the agent drives it, next to the oracle."""

    def __init__(self, overrides=None):
        self.unit, self.decl = make_element_rollup(overrides)
        self.params = d.resolve_params(self.decl, overrides)
        os.environ["OPENDDIL_EDGE_ID"], os.environ["OPENDDIL_REGION_ID"] = EDGE, REGION
        self.wiring = runner.Wiring(self.unit, self.decl, lambda _a: None, {})
        self.oracle = Oracle(self.params, EDGE, REGION, last_window=self.wiring.last_window.get)
        self.oracle.set_clock(lambda: NOW)

    def feed(self, data: bytes):
        old = self.oracle.sends(data)
        _state, sends = self.wiring.process(data, 0, None)
        return old, [(t, k, outputs.encode(m)) for t, k, m in sends]

    def counters(self):
        return self.wiring.counters


def run(rig, records):
    n_old = n_new = 0
    for i, data in enumerate(records):
        old, new = rig.feed(data)
        assert new == old, f"record {i}"
        n_old, n_new = n_old + len(old), n_new + len(new)
    return n_old, n_new


def window_of(sends):
    w = winpb.WindowedTelemetry()
    w.ParseFromString(sends[0][2])
    return w


def synth_stream(seed=11, per_asset=2000):
    fill = synth.make(seed)
    out = []
    for i in range(per_asset):
        for a in ASSETS:
            out.append(fill(1_000_000 + i * 2000, a))
    return out


def test_synth_stream_exact_on_three_assets_two_thousand_envelopes_each():
    rig = Rig()
    n_old, n_new = run(rig, synth_stream())
    assert n_old == n_new == 6000


def test_the_synth_exercises_every_branch():
    seen = Counter()
    for data in synth_stream(per_asset=400):
        env = json.loads(data)
        op = env.get("operational")
        seen["no_operational"] += op is None
        if op is not None:
            seen["core_temp"] += "core_temp_c" in op
            seen["no_core_temp"] += "core_temp_c" not in op
            seen["uptime"] += "uptime_hours" in op
            seen["no_uptime"] += "uptime_hours" not in op
            seen["condition_dict"] += isinstance(op.get("condition"), dict)
            seen["condition_other"] += "condition" in op and not isinstance(op["condition"], dict)
        seen["no_id_element"] += any("element_id" not in e for e in env["elements"])
        seen["unlabelled"] += "originator_nation" not in env
        seen["variant"] += bool(env.get("platform_variant"))
    for k in ("no_operational", "core_temp", "no_core_temp", "uptime", "no_uptime", "condition_dict",
              "condition_other", "no_id_element", "unlabelled", "variant"):
        assert seen[k] > 0, k
    # a malformed condition block is in the stream: the old parse drops it
    rig = Rig()
    dropped = 0
    for data in synth_stream(per_asset=400):
        op = json.loads(data).get("operational") or {}
        if isinstance(op.get("condition"), dict) and isinstance(op["condition"].get("level"), dict):
            _old, new = rig.feed(data)
            dropped += not window_of(new).element_rollup.HasField("condition")
    assert dropped > 0


def test_the_synth_extension_moves_nothing_the_base_synth_makes():
    """Golden hashes of `make` over two seeds, taken before the extension."""
    import hashlib
    from element_bands_v0 import synth as base
    want = {1: "465b35fea1aac8434db10398ed65a6277056d271c9abb647d3665223da1fdb3b",
            7: "66fdc16e7708b8149de5a9cdcf1977ca9089025d58bd8ab115ba62429e9de59d"}
    for seed, digest in want.items():
        fill, h = base.make(seed), hashlib.sha256()
        for i in range(2000):
            for a in ("asset-a", "asset-b", "asset-c"):
                h.update(fill(1_000_000 + i * 1000, a))
        assert h.hexdigest() == digest, seed
    # and the extension is `make`'s envelope with fields added: same elements, same ids
    plain, ext = base.make(3), base.make_extended(3)
    for i in range(50):
        a, b = json.loads(plain(i * 1000, "x")), json.loads(ext(i * 1000, "x"))
        assert [e for e in b["elements"] if "element_id" in e] == a["elements"]
        assert b["observed_at_ns"] == a["observed_at_ns"]


GATE_CASES = {
    "non_dict_json": (b"[1, 2]", "envelope_unreadable"),
    "not_json": (b"\xff\xfe", "envelope_unreadable"),
    "no_asset_id": (raw({"elements": els(0.5)}), "no_asset_id"),
    "empty_asset_id": (raw(envelope(asset="")), "no_asset_id"),
    "int_asset_id": (raw(envelope(asset=7)), "no_asset_id"),
    "empty_elements": (raw(envelope(elements=[])), "no_rollup"),
    "no_elements": (raw({k: v for k, v in envelope().items() if k != "elements"}), "no_rollup"),
}


@pytest.mark.parametrize("name", sorted(GATE_CASES))
def test_gate_case_matches_and_counts(name):
    data, counter = GATE_CASES[name]
    rig = Rig()
    old, new = rig.feed(data)
    assert old == new == []
    assert rig.counters()[counter] == 1


def test_no_observed_at_still_sends_and_counts_no_skip():
    rig = Rig()
    for env in (envelope(observed_at_ns=True), {k: v for k, v in envelope().items() if k != "observed_at_ns"},
                envelope(observed_at_ns="5")):
        old, new = rig.feed(raw(env))
        assert old == new and len(new) == 1
        w = window_of(new)
        assert not w.element_rollup.HasField("observed_at")
        assert w.window.window_start.ToNanoseconds() == NOW
    assert rig.counters().get("required_role_absent", 0) == 0


# Divergences from the old agent: it raised in rollup_from_envelope and the
# agent stopped; the unit skips and counts rollup_raised. The oracle raises.
@pytest.mark.parametrize("env", [
    envelope(elements=[5, {"element_id": "e1", "health": 0.5}]),          # a non-dict element
    envelope(elements="oops"),                                            # elements not a list
    envelope(observed_at_ns=float("inf")),                                # infinite time
], ids=["non_dict_element", "elements_not_list", "infinite_time"])
def test_old_raises_unit_skips(env):
    rig = Rig()
    with pytest.raises(Exception):
        rig.oracle.sends(raw(env))
    _state, sends = rig.wiring.process(raw(env), 0, None)
    assert sends == []
    assert rig.counters()["rollup_raised"] == 1


def test_health_exactly_on_the_limits_is_not_over_them():
    rig = Rig()
    n_old, n_new = run(rig, [raw(envelope(elements=els(0.90, 0.97, 0.9000001, 0.9700001, 0.5, 1.0)))])
    assert n_old == n_new == 1
    r = window_of(rig.feed(raw(envelope(elements=els(0.90, 0.97, 0.9000001, 0.9700001, 0.5, 1.0))))[1]).element_rollup
    assert (r.critical_count, r.degraded_count) == (2, 2)      # 0.9700001 and 1.0; 0.97 and 0.9000001 and... see below
    assert r.element_count == 6


def test_the_two_counts_at_the_boundaries():
    rig = Rig()
    new = rig.feed(raw(envelope(elements=els(0.90, 0.97))))[1]
    r = window_of(new).element_rollup
    assert (r.critical_count, r.degraded_count) == (0, 1)       # 0.97 is degraded, 0.90 is neither


def test_bool_health_and_temp_are_not_numbers():
    rig = Rig()
    el = [{"element_id": "e1", "health": True, "temp_c": True, "load_pct": False},
          {"element_id": "e2", "health": 0.99, "temp_c": 30, "load_pct": 50}]
    old, new = rig.feed(raw(envelope(elements=el)))
    assert old == new
    r = window_of(new).element_rollup
    assert (r.critical_count, r.degraded_count) == (1, 0)
    assert r.avg_temp_c == 30.0 and r.avg_load_pct == 50.0 and r.element_count == 2


def test_operational_readouts_present_only_when_numeric():
    rig = Rig()
    for op in ({}, {"core_temp_c": 41.5}, {"uptime_hours": 0}, {"core_temp_c": True, "uptime_hours": "9"},
               {"core_temp_c": 1, "uptime_hours": 2.5}, None):
        old, new = rig.feed(raw(envelope(operational=op)))
        assert old == new
        r = window_of(new).element_rollup
        want = op or {}
        assert r.HasField("core_temp_c") == (isinstance(want.get("core_temp_c"), (int, float))
                                             and not isinstance(want.get("core_temp_c"), bool))


@pytest.mark.parametrize("cond", [
    {"level": "CONDITION_LEVEL_DEGRADED", "moved_by": ["CONDITION_SOURCE_EMISSION"],
     "claims": [{"source": "CONDITION_SOURCE_EMISSION", "level": "CONDITION_LEVEL_DEGRADED", "detail": "d"}]},
    {"level": {"not": "a level"}}, "red", [], {"level": "CONDITION_LEVEL_NOMINAL", "extra": 1}, {}],
    ids=["good", "malformed", "string", "list", "unknown_field", "empty"])
def test_condition_block_matches_good_and_malformed(cond):
    rig = Rig()
    old, new = rig.feed(raw(envelope(operational={"condition": cond})))
    assert old == new and len(new) == 1
    assert window_of(new).element_rollup.element_count == 1


def test_malformed_condition_is_dropped_and_the_counts_stay():
    rig = Rig()
    new = rig.feed(raw(envelope(operational={"condition": {"level": {"x": 1}}}, elements=els(0.99, 0.5))))[1]
    r = window_of(new).element_rollup
    assert not r.HasField("condition") and r.critical_count == 1 and r.element_count == 2


def test_unlabelled_source_has_no_labels():
    rig = Rig()
    old, new = rig.feed(raw(envelope(originator_nation=None, releasable_to=None)))
    assert old == new
    w = window_of(new)
    assert w.provenance.originator_nation == "" and list(w.provenance.releasable_to) == []
    old, new = rig.feed(raw({k: v for k, v in envelope().items() if k not in ("originator_nation", "releasable_to")}))
    assert old == new and list(window_of(new).provenance.releasable_to) == []


@pytest.mark.parametrize("releasable", ["BDR", {"a": "b"}, 5, ["BDR", 3, None, "CRL"]])
def test_releasable_to_not_a_list_or_with_non_strings(releasable):
    rig = Rig()
    old, new = rig.feed(raw(envelope(releasable_to=releasable)))
    assert old == new
    want = ["BDR", "CRL"] if isinstance(releasable, list) else []
    assert list(window_of(new).provenance.releasable_to) == want


def test_non_str_nation_is_none():
    rig = Rig()
    old, new = rig.feed(raw(envelope(originator_nation=3)))
    assert old == new and window_of(new).provenance.originator_nation == ""


def test_elements_with_no_id_are_counted():
    rig = Rig()
    el = [{"layer_name": "x", "health": 0.99}, {"element_id": "", "health": 0.95}, {"element_id": 3, "health": 0.1}]
    old, new = rig.feed(raw(envelope(elements=el)))
    assert old == new
    r = window_of(new).element_rollup
    assert (r.element_count, r.critical_count, r.degraded_count) == (3, 1, 1)


def test_platform_variant_and_profile_defaults():
    rig = Rig()
    for over in ({"platform_variant": None}, {"profile_name": None}, {"platform_variant": ""}):
        old, new = rig.feed(raw(envelope(**over)))
        assert old == new
    e = envelope()
    del e["platform_variant"], e["profile_name"]
    old, new = rig.feed(raw(e))
    assert old == new and window_of(new).platform_variant == ""


def test_producer_is_faust_edge_and_there_is_no_unit_stamp_and_the_key_is_bytes():
    rig = Rig()
    _old, new = rig.feed(raw(envelope()))
    topic, key, _v = new[0]
    assert (topic, key) == ("asset-telemetry-windows", b"a1")
    w = window_of(new)
    assert w.provenance.producer_id == "faust-edge" and not w.provenance.HasField("producer_stamp")
    assert (w.provenance.edge_id, w.provenance.region_id) == (EDGE, REGION)
    _s, sends, _c = core.process_one(rig.unit, rig.decl, raw(envelope()), None, 0)
    assert sends[0][1] == b"a1"


TRENDS = {"window_start_ns": 1_000_000_000, "window_end_ns": 2_000_000_000, "sample_count": 4,
          "fluid_trends": {"oil": {"latest": 3.0, "latest_unit": "bar", "slope": 0.5, "slope_unit": "bar",
                                   "r_squared": 0.9, "latest_sample_ns": 2_000_000_000,
                                   "window_start_ns": 1_000_000_000, "sample_count": 4}},
          "consumable_trends": [], "wear_trends": [], "active_fault_codes": ["F1"]}


def test_trends_from_the_hook_match_the_oracle():
    rig = Rig()
    rig.wiring.last_window["a1"] = TRENDS
    old, new = rig.feed(raw(envelope()))
    assert old == new
    w = window_of(new)
    assert "oil" in w.fluid_trends and w.window.sample_count == 4
    assert w.window.window_start.ToNanoseconds() == 1_000_000_000


def test_an_element_window_before_any_trends_has_none_and_sample_count_zero():
    rig = Rig()
    w = window_of(rig.feed(raw(envelope()))[1])
    assert len(w.fluid_trends) == 0 and w.window.sample_count == 0


def test_unusable_trends_start_fresh_on_both_sides():
    rig = Rig()
    rig.wiring.last_window["a1"] = {"sample_count": 1}
    old, new = rig.feed(raw(envelope()))
    assert old == new and window_of(new).window.sample_count == 0


class _Trends(Unit):
    """Stands in for the trends unit: emits a fixed window payload."""

    def step(self, sample, state):
        return {"window": TRENDS}


def _trends_wiring(window_rollup):
    decl = d.load(ROOT / "algorithms" / "trends" / "unit.yaml")
    return runner.Wiring(_Trends(d.resolve_params(decl, None)), decl, window_rollup, {})


def _telemetry(asset="a1") -> bytes:
    e = pb.EntityTelemetryEvent()
    e.asset.asset_id = asset
    e.provenance.sample_time.seconds = 5
    return e.SerializeToString()


def test_a_trends_window_before_and_after_a_rollup_carries_the_latest_rollup():
    rig = Rig()
    trends = _trends_wiring(rig.wiring.last_rollup.get)
    before = trends.process(_telemetry(), 0, None)[1][0][2]
    assert not before.HasField("element_rollup")
    rig.wiring.process(raw(envelope(elements=els(0.99, 0.5), profile_name="first")), 0, None)
    after = trends.process(_telemetry(), 0, None)[1][0][2]
    assert after.element_rollup.profile_name == "first" and after.element_rollup.critical_count == 1
    rig.wiring.process(raw(envelope(elements=els(0.5), profile_name="second")), 0, None)
    latest = trends.process(_telemetry(), 0, None)[1][0][2]
    assert latest.element_rollup.profile_name == "second" and latest.element_rollup.element_count == 1
    assert not trends.process(_telemetry("other"), 0, None)[1][0][2].HasField("element_rollup")


def test_a_params_override_drives_the_counts():
    rig = Rig({"health_critical": 0.6, "health_degraded": 0.3})
    r = window_of(rig.feed(raw(envelope(elements=els(0.5, 0.7, 0.2, 0.3, 0.6))))[1]).element_rollup
    assert (r.critical_count, r.degraded_count) == (1, 2)        # strictly above: 0.7; 0.5 and... 0.6 is not


def test_params_are_bounded_and_the_yaml_says_so():
    decl = d.load(ROLLUP_DIR / "unit.yaml")
    for bad in (-0.1, 1.5):
        with pytest.raises(d.DeclarationError):
            d.resolve_params(decl, {"health_critical": bad})
    assert d.resolve_params(decl, None) == {"health_critical": 0.97, "health_degraded": 0.90}
    text = (ROLLUP_DIR / "unit.yaml").read_text(encoding="utf-8")
    assert "THREE COPIES" in text


def test_a_new_runner_starts_with_no_rollup():
    rig = Rig()
    rig.feed(raw(envelope()))
    assert rig.wiring.last_rollup["a1"].element_count == 1
    assert Rig().wiring.last_rollup == {}


def test_register_defaults_window_rollup_to_the_runners_store(tmp_path, monkeypatch):
    shutil.copytree(ROLLUP_DIR, tmp_path / "element_rollup", ignore=shutil.ignore_patterns("__pycache__"))
    monkeypatch.setenv("ALGORITHMS_DIR", str(tmp_path))
    monkeypatch.setenv("ALGORITHMS_ENABLED", "element_rollup")
    monkeypatch.setenv("OPENDDIL_EDGE_ID", EDGE)
    monkeypatch.delenv("ALGORITHM_PARAMS_JSON", raising=False)
    monkeypatch.setattr(runner, "_refuse_unrunnable", lambda loaded: None)
    app = faust.App("rollup-register-test", broker="kafka://localhost:9092", store="memory://")
    got = runner.register(app)
    w = got.wirings["element_rollup"]
    assert w.window_rollup == got._last_rollup.get
    w.process(raw(envelope()), 0, None)
    assert got.last_rollup("a1").element_count == 1
    # an explicit hook wins
    app2 = faust.App("rollup-register-test-2", broker="kafka://localhost:9092", store="memory://")
    hook = lambda _a: None  # noqa: E731
    assert runner.register(app2, window_rollup=hook).wirings["element_rollup"].window_rollup is hook


def test_compare_oracle_mode_with_synth():
    unit, decl = make_element_rollup()
    params = d.resolve_params(decl, None)
    env = {"OPENDDIL_EDGE_ID": EDGE, "OPENDDIL_REGION_ID": REGION}
    recs = []
    for i in range(600):
        e = pb.EntityTelemetryEvent()
        e.asset.asset_id = ASSETS[i % 3]
        recs.append((i, 1_000_000 + i * 2000, b"k", e.SerializeToString()))
    timing = {}
    rep = replay.compare_oracle(unit, decl, params, iter(recs), env, "element_rollup", 3, timing)
    assert rep.compared == rep.equal == 600
    assert rep.different == rep.old_only == rep.new_only == rep.stamp_failures == 0
    assert rep.unstamped_kinds == ["element_window"]
    assert set(timing) == {"old", "new"}
    # the clock is restored afterwards
    assert outputs._now_ns() != 0
    # no seed: the records are the envelopes
    envs = [(i, i, b"k", raw(envelope(t_s=i, elements=els(0.5 if i % 4 < 2 else 0.99)))) for i in range(40)]
    rep = replay.compare_oracle(unit, decl, params, iter(envs), env, "element_rollup")
    assert rep.compared == rep.equal == 40 and not rep.different


def test_compare_clears_only_the_wall_clock(monkeypatch):
    """The comparison clears computed_at and provenance.ingest_time and nothing else."""
    a = winpb.WindowedTelemetry()
    a.asset_id = "x"
    a.computed_at.FromNanoseconds(1)
    a.provenance.ingest_time.FromNanoseconds(1)
    b = winpb.WindowedTelemetry()
    b.CopyFrom(a)
    b.computed_at.FromNanoseconds(2)
    b.provenance.ingest_time.FromNanoseconds(3)
    assert replay._wire(a) == replay._wire(b)
    b.provenance.edge_id = "other"
    assert replay._wire(a) != replay._wire(b)
    c = winpb.WindowedTelemetry()
    c.CopyFrom(a)
    c.window.sample_count = 1
    assert replay._wire(a) != replay._wire(c)


def test_the_harness_counts_a_difference(monkeypatch):
    unit, decl = make_element_rollup()
    params = d.resolve_params(decl, None)
    env = {"OPENDDIL_EDGE_ID": EDGE, "OPENDDIL_REGION_ID": REGION}
    real = Oracle.sends

    def skewed(self, data):
        out = real(self, data)
        return [(t, k, v.replace(b"faust-edge", b"faust-edgX")) for t, k, v in out]

    monkeypatch.setattr(Oracle, "sends", skewed)
    envs = [(i, i, b"k", raw(envelope(t_s=i))) for i in range(5)]
    rep = replay.compare_oracle(unit, decl, params, iter(envs), env, "element_rollup")
    assert rep.different == 5 and rep.equal == 0
    assert any("provenance.producer_id" in p for f in rep.first_diffs for p in f["paths"])
