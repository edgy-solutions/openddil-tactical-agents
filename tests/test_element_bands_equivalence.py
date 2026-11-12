"""The element_bands unit against the frozen old in-line path
(tests/oracle/element_bands_v0).

Both sides see the same element envelopes in the same order. The unit runs
through Wiring exactly as the agent does; the oracle is the old agent's body
(its gates, tracker and event builder). Everything sent is compared as
(topic, key, bytes). The envelopes come from the oracle package's synth, plus
hand-built gate cases."""
from __future__ import annotations

import json
import os
from collections import Counter

import pytest
from openddil.telemetry.v1 import telemetry_pb2 as pb

from algorithm_runtime import declaration as d
from algorithm_runtime import outputs, replay, runner
from element_bands_v0 import Oracle
from element_bands_v0 import synth
from unit_helpers import BANDS_DIR, make_element_bands

EDGE, REGION = "edge-tst", "region-tst"
ASSETS = ("asset-a", "asset-b", "asset-c")


def envelope(asset="a1", t_s=1.0, elements=None, **over):
    env = {"asset_id": asset, "profile_name": "p", "observed_at_ns": int(t_s * 1e9),
           "originator_nation": "ATL", "releasable_to": ["BDR"],
           "elements": elements if elements is not None else [
               {"element_id": "e1", "layer_name": "psu", "health": 0.5}]}
    env.update(over)
    return env


def raw(env):
    return json.dumps(env).encode("utf-8")


def els(*healths, field="health"):
    return [{"element_id": f"e{i}", "layer_name": "psu", field: h} for i, h in enumerate(healths)]


class Rig:
    """The unit as the agent drives it, next to the oracle."""

    def __init__(self, overrides=None):
        self.unit, self.decl = make_element_bands(overrides)
        self.params = d.resolve_params(self.decl, overrides)
        os.environ["OPENDDIL_EDGE_ID"], os.environ["OPENDDIL_REGION_ID"] = EDGE, REGION
        self.wiring = runner.Wiring(self.unit, self.decl, lambda _a: None, {})
        self.oracle = Oracle(self.params, EDGE, REGION)

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


def synth_stream(seed=11, per_asset=2000):
    fill = synth.make(seed)
    out = []
    for i in range(per_asset):
        for a in ASSETS:
            out.append(fill(1_000_000 + i * 2000, a))
    return out


def test_synth_stream_exact_and_crosses_every_limit_both_ways():
    rig = Rig()
    records = synth_stream()
    n_old, n_new = run(rig, records)
    assert n_old == n_new > 100
    pairs = Counter()
    probe = Rig()
    for data in records:
        for _t, _k, v in probe.oracle.sends(data):
            ev = json.loads(v)["data"]
            pairs[(ev["from"], ev["to"])] += 1
    for pair in (("green", "yellow"), ("yellow", "green"), ("yellow", "red"), ("red", "yellow")):
        assert pairs[pair] > 0, pair


GATE_CASES = {
    "non_dict_json": (b"[1, 2]", "envelope_unreadable"),
    "not_json": (b"\xff\xfe", "envelope_unreadable"),
    "no_asset_id": (raw({"elements": els(0.5)}), "no_asset_id"),
    "empty_asset_id": (raw(envelope(asset="")), "no_asset_id"),
    "int_asset_id": (raw(envelope(asset=7)), "no_asset_id"),
    "no_rollup_no_time": (raw({k: v for k, v in envelope().items() if k != "observed_at_ns"}), "required_role_absent"),
    "no_rollup_bool_time": (raw(envelope(observed_at_ns=True)), "required_role_absent"),
    "no_rollup_empty_elements": (raw(envelope(elements=[])), "no_rollup"),
}


@pytest.mark.parametrize("name", sorted(GATE_CASES))
def test_gate_case_matches_and_counts(name):
    data, counter = GATE_CASES[name]
    rig = Rig()
    old, new = rig.feed(data)
    assert old == new == []
    assert rig.counters()[counter] == 1


def test_gate_cases_interleaved_with_crossings_stay_exact():
    rig = Rig()
    records = []
    for i in range(300):
        records.append(raw(envelope(t_s=i, elements=els(0.5 if i % 40 < 20 else 0.99))))
        records.append(raw(envelope(t_s=i, elements=els("x", True, None, 0.99, 0.5))))   # non-numeric, bool
        records.append(raw(envelope(t_s=i, elements=[{"element_id": "", "health": 0.1},
                                                     {"element_id": 3, "health": 0.1},
                                                     {"element_id": "e9", "health": 0.99, "other": 0.1}])))
        records.append(raw(envelope(t_s=i, originator_nation="", releasable_to=None)))
        records.append(raw(envelope(t_s=i, releasable_to="BDR")))                       # non-list releasable_to
        records.extend(c[0] for c in GATE_CASES.values())
    n_old, n_new = run(rig, records)
    assert n_old == n_new > 0
    for counter in ("envelope_unreadable", "no_asset_id", "no_rollup", "required_role_absent"):
        assert rig.counters()[counter] > 0, counter


def test_unlabelled_source_has_no_labels():
    rig = Rig()
    lab = dict(originator_nation=None, releasable_to=None)
    run(rig, [raw(envelope(t_s=1, elements=els(0.5), **lab)),
              raw(envelope(t_s=2, elements=els(0.99), **lab))])
    old, new = rig.feed(raw(envelope(t_s=3, elements=els(0.5), **lab)))
    assert old == new and len(new) == 1
    assert "originator_nation" not in json.loads(new[0][2])["data"]


def test_limit_exactly_and_limit_minus_hysteresis_exactly():
    rig = Rig()
    lo, hi, hy = 0.90, 0.97, 0.01
    healths = [0.5, lo, lo + 1e-9, lo - hy, lo - hy - 1e-9, lo, hi, hi - hy, hi - hy - 1e-9, hi]
    records = [raw(envelope(t_s=i, elements=els(h))) for i, h in enumerate(healths * 3)]
    n_old, n_new = run(rig, records)
    assert n_old == n_new


def test_overridden_bands_match():
    bands = {"health": {"limits": [0.5, 0.8], "names": ["lo", "mid", "hi"], "hysteresis": 0.05,
                        "severity": {"lo": "info", "mid": "warn", "hi": "crit"}}}
    rig = Rig({"bands": bands})
    assert rig.params["bands"]["health"]["names"] == ["lo", "mid", "hi"]
    n_old, n_new = run(rig, synth_stream(seed=5, per_asset=300))
    assert n_old == n_new > 0


def test_unconfigured_field_is_ignored():
    rig = Rig({"bands": {"temp_c": {"limits": [50.0], "names": ["a", "b"], "hysteresis": 1.0,
                                    "severity": {"a": "info", "b": "warn"}}}})
    n_old, n_new = run(rig, [raw(envelope(t_s=i, elements=els(0.5 if i % 2 else 0.99))) for i in range(20)])
    assert n_old == n_new == 0


def _good(**over):
    cfg = {"limits": [0.9, 0.97], "names": ["a", "b", "c"], "hysteresis": 0.01,
           "severity": {"a": "info", "b": "warn", "c": "crit"}}
    cfg.update(over)
    return {"health": cfg}


BAD = {
    "not_object": [],
    "empty": {},
    "names_count": _good(names=["a", "b"]),
    "limits_unordered": _good(limits=[0.97, 0.9]),
    "limits_equal": _good(limits=[0.9, 0.9]),
    "hysteresis_negative": _good(hysteresis=-0.1),
    "hysteresis_str": _good(hysteresis="x"),
    "severity_missing": _good(severity={"a": "info", "b": "warn"}),
    "severity_empty": _good(severity={"a": "info", "b": "warn", "c": ""}),
}


@pytest.mark.parametrize("name", sorted(BAD))
def test_invalid_bands_refused_as_the_old_parse_did(name):
    decl = d.load(BANDS_DIR / "unit.yaml")
    with pytest.raises(d.DeclarationError, match="bands"):
        d.resolve_params(decl, {"bands": BAD[name]})


def test_good_override_resolves():
    decl = d.load(BANDS_DIR / "unit.yaml")
    assert d.resolve_params(decl, {"bands": _good()})["bands"]["health"]["names"] == ["a", "b", "c"]


def test_override_does_not_alias_the_default():
    decl = d.load(BANDS_DIR / "unit.yaml")
    a = d.resolve_params(decl, None)
    a["bands"]["health"]["limits"][0] = 0.1
    assert d.resolve_params(decl, None)["bands"]["health"]["limits"][0] == 0.90


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


def test_state_does_not_survive_a_new_runner():
    rig = Rig()
    run(rig, [raw(envelope(t_s=1, elements=els(0.5)))])
    fresh = Rig()
    # A restart: the first observation after it is silent whatever the band.
    old, new = fresh.feed(raw(envelope(t_s=2, elements=els(0.99))))
    assert old == new == []
    # The warmed rig crosses on the same input.
    old, new = rig.feed(raw(envelope(t_s=2, elements=els(0.99))))
    assert len(new) == 1 and old == new


def test_dual_source_names_the_legacy_env():
    assert runner._LEGACY_UNIT_ENV["ELEMENT_BANDS_JSON"] == ("element_bands", "bands")


def test_compare_oracle_mode_with_synth():
    unit, decl = make_element_bands()
    params = d.resolve_params(decl, None)
    env = {"OPENDDIL_EDGE_ID": EDGE, "OPENDDIL_REGION_ID": REGION}
    recs = []
    for i in range(600):
        e = pb.EntityTelemetryEvent()
        e.asset.asset_id = ASSETS[i % 3]
        recs.append((i, 1_000_000 + i * 2000, b"k", e.SerializeToString()))
    timing = {}
    rep = replay.compare_oracle(unit, decl, params, iter(recs), env, "element_bands", 3, timing)
    assert rep.compared == rep.equal > 0
    assert rep.different == rep.old_only == rep.new_only == rep.stamp_failures == 0
    assert set(timing) == {"old", "new"}
    # No seed: a json-envelope unit's records are the envelopes.
    envs = [(i, i, b"k", raw(envelope(t_s=i, elements=els(0.5 if i % 4 < 2 else 0.99)))) for i in range(40)]
    rep = replay.compare_oracle(unit, decl, params, iter(envs), env, "element_bands")
    assert rep.compared == rep.equal > 0 and not rep.different


# The old tracker's own cases (they lived in edge/tests/test_element_band_event.py
# with the tracker), run through the unit with that file's config.
OLD_CFG = {"health": {"limits": [0.90, 0.97], "names": ["ok", "degraded", "critical"],
                      "hysteresis": 0.02, "severity": {"ok": "info", "degraded": "warn", "critical": "crit"}}}


def _band_path(rig, healths, field="health", element="e"):
    """The (from, to) crossings the unit sends for a run of values."""
    out = []
    for i, h in enumerate(healths):
        old, new = rig.feed(raw(envelope(t_s=i + 1, elements=[{"element_id": element, field: h}])))
        assert old == new
        out.extend((json.loads(v)["data"]["from"], json.loads(v)["data"]["to"]) for _t, _k, v in new)
    return out


def test_schmitt_behaviour_through_the_unit():
    rig = Rig({"bands": OLD_CFG})
    got = _band_path(rig, [0.50, 0.91, 0.89, 0.905, 0.87, 0.99, 0.96, 0.20])
    assert got == [("ok", "degraded"), ("degraded", "ok"), ("ok", "critical"), ("critical", "ok")]


def test_ignored_inputs_through_the_unit():
    rig = Rig({"bands": OLD_CFG})
    assert _band_path(rig, ["bad", True]) == []
    assert _band_path(rig, [99.0], field="temp_c") == []
    # ignored values did not set a band: the next numeric is still the first observation
    assert _band_path(rig, [0.99]) == []
