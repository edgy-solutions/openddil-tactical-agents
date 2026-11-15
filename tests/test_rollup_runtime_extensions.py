"""The runtime extensions the element-rollup port needs: six envelope roles,
the `element_window` output kind (unstamped, bytes key, host-built window with
the trends hook), the runner's rollup store and its default `window_rollup`.

A toy unit stands in here; the real unit's tests are in
test_element_rollup_equivalence.py."""
from __future__ import annotations

import copy
import json

import pytest
import yaml
from google.protobuf import json_format
from jsonschema import Draft202012Validator
from openddil.logistics.v1 import windowed_telemetry_pb2 as winpb

from algorithm_runtime import core
from algorithm_runtime import declaration as d
from algorithm_runtime import outputs, roles, runner
from algorithm_runtime.contract import ROLES, Unit

from unit_helpers import ROOT

SCHEMA = json.loads((ROOT / "algorithms" / "declaration.schema.json").read_text(encoding="utf-8"))

DOC = yaml.safe_load("""
apiVersion: openddil.io/algorithm/v1
unit: element_rollup
version: "1.0.0"
runtime: stream-parallel
entrypoint: element_rollup_unit.py:ElementRollup
key: asset_id
composition: propagating
inputs:
  topic: asset-element-telemetry
  format: json-envelope
  roles:
    - {role: element_values, required: true}
    - {role: observed_at_ns}
    - {role: profile_name}
    - {role: operational}
    - {role: envelope_labels}
    - {role: platform_variant}
params:
  health_critical: {type: number, default: 0.97, minimum: 0, maximum: 1, unit: "1"}
  health_degraded: {type: number, default: 0.90, minimum: 0, maximum: 1, unit: "1"}
state:
  persistence: memory
  fields:
    seen: {type: integer, default: 0}
outputs:
  - kind: element_window
    topic: asset-telemetry-windows
    direction: upward
    stamp: none
    labels: copy-source
resources: {memory_mi: 8, cpu_m: 10}
dependencies: []
""")

NEW_ROLES = ("element_values", "observed_at_ns", "profile_name", "operational",
             "envelope_labels", "platform_variant")


def env(**over):
    e = {"asset_id": "a1", "profile_name": "p", "observed_at_ns": 5_000_000_000,
         "platform_variant": "v1", "originator_nation": "ATL", "releasable_to": ["BDR"],
         "operational": {"core_temp_c": 40.0},
         "elements": [{"element_id": "e1", "layer_name": "psu", "health": 0.5}]}
    e.update(over)
    return e


def raw(**over):
    return json.dumps(env(**over)).encode("utf-8")


@pytest.fixture(autouse=True)
def _clock():
    outputs.set_clock(lambda: 7_000_000_000)
    yield
    outputs.set_clock(None)


# --- the declaration --------------------------------------------------------------------
def test_declaration_validates_and_matches_the_schema():
    decl = d.validate(copy.deepcopy(DOC))
    assert [o.kind for o in decl.outputs] == ["element_window"]
    assert decl.outputs[0].stamp == "none"
    assert list(Draft202012Validator(SCHEMA).iter_errors(DOC)) == []


def test_new_roles_are_in_the_vocabulary_and_the_schema():
    enum = SCHEMA["properties"]["inputs"]["properties"]["roles"]["items"]["properties"]["role"]["enum"]
    for r in NEW_ROLES:
        assert r in ROLES and r in enum and r in d.JSON_ENVELOPE_ROLES


def test_protobuf_input_may_not_name_the_new_roles():
    def make(x):
        x["inputs"]["format"] = "protobuf"
    doc = copy.deepcopy(DOC)
    make(doc)
    with pytest.raises(d.DeclarationError, match="role"):
        d.validate(doc)


def test_element_window_must_be_unstamped_and_unstamped_kinds_only():
    doc = copy.deepcopy(DOC)
    doc["outputs"][0]["stamp"] = "producer"
    with pytest.raises(d.DeclarationError, match="stamp"):
        d.validate(doc)


# --- the roles --------------------------------------------------------------------------
def test_element_values_are_unfiltered_shallow_copies():
    src = env(elements=[{"element_id": "e1", "health": 1}, {"layer_name": "x"}, {"element_id": ""}])
    got = roles.extract_envelope(src, {"element_values"})["element_values"]
    assert len(got) == 3 and got[1] == {"layer_name": "x"}
    assert got[0] == src["elements"][0] and got[0] is not src["elements"][0]


def test_roles_are_extracted_only_when_wanted():
    assert roles.extract_envelope(env(), {"profile_name"}) == {"profile_name": "p"}
    assert roles.extract_envelope(env(), {"observed_at_s"}).keys() == {"observed_at_s"}


@pytest.mark.parametrize("value,want", [
    (5_000_000_000, 5_000_000_000), (5e9, 5_000_000_000), (True, None), (None, None),
    ("5", None), ([1], None)])
def test_observed_at_ns(value, want):
    assert roles.extract_envelope(env(observed_at_ns=value), {"observed_at_ns"}) == {"observed_at_ns": want}


def test_observed_at_ns_missing_is_none():
    e = env()
    del e["observed_at_ns"]
    assert roles.extract_envelope(e, {"observed_at_ns"}) == {"observed_at_ns": None}


@pytest.mark.parametrize("value,want", [("p", "p"), (None, ""), ("", ""), (0, "")])
def test_profile_name(value, want):
    assert roles.extract_envelope(env(profile_name=value), {"profile_name"})["profile_name"] == want


def test_profile_name_missing_is_empty():
    e = env()
    del e["profile_name"]
    assert roles.extract_envelope(e, {"profile_name"})["profile_name"] == ""


def test_operational_is_a_deep_copy_and_defaults_empty():
    src = env(operational={"condition": {"level": "x"}})
    got = roles.extract_envelope(src, {"operational"})["operational"]
    assert got == src["operational"]
    got["condition"]["level"] = "changed"
    assert src["operational"]["condition"]["level"] == "x"
    e = env()
    del e["operational"]
    assert roles.extract_envelope(e, {"operational"})["operational"] == {}
    assert roles.extract_envelope(env(operational=None), {"operational"})["operational"] == {}


def test_envelope_labels():
    got = roles.extract_envelope(env(releasable_to=["BDR", 3, "ATL"]), {"envelope_labels"})
    assert got["envelope_labels"] == ("ATL", ("BDR", "ATL"))


@pytest.mark.parametrize("releasable", ["BDR", {"a": 1}, 5, None])
def test_envelope_labels_releasable_not_a_list_is_empty(releasable):
    assert roles.extract_envelope(env(releasable_to=releasable), {"envelope_labels"})["envelope_labels"] == ("ATL", ())


def test_envelope_labels_nation_not_a_string_is_empty():
    assert roles.extract_envelope(env(originator_nation=3), {"envelope_labels"})["envelope_labels"][0] == ""
    e = env()
    del e["originator_nation"], e["releasable_to"]
    assert roles.extract_envelope(e, {"envelope_labels"})["envelope_labels"] == ("", ())


def test_platform_variant():
    assert roles.extract_envelope(env(), {"platform_variant"})["platform_variant"] == "v1"
    assert roles.extract_envelope(env(platform_variant=None), {"platform_variant"})["platform_variant"] == ""


# --- the builder ------------------------------------------------------------------------
def payload(**over):
    p = {"profile_name": "p", "element_count": 3, "critical_count": 1, "degraded_count": 1,
         "avg_temp_c": 50.5, "avg_load_pct": 20.0, "observed_at_ns": 5_000_000_000,
         "originator_nation": "ATL", "releasable_to": ["BDR"], "platform_variant": "v1"}
    p.update(over)
    return p


class Toy(Unit):
    def __init__(self, params, out):
        super().__init__(params)
        self.out = out

    def step(self, sample, state):
        state["seen"] += 1
        return {"element_window": self.out}


def wiring(out, trends=None):
    decl = d.validate(copy.deepcopy(DOC))
    w = runner.Wiring(Toy(d.resolve_params(decl, None), out), decl, lambda _a: None, {})
    if trends is not None:
        w.last_window["a1"] = trends
    return w


def expected(**over):
    m = winpb.WindowedTelemetry()
    m.asset_id = "a1"
    m.platform_variant = "v1"
    m.computed_at.FromNanoseconds(7_000_000_000)
    m.provenance.producer_id = "faust-edge"
    m.provenance.edge_id = "edge-test"
    m.provenance.region_id = "region-01"
    m.provenance.ingest_time.FromNanoseconds(7_000_000_000)
    m.provenance.classification = "U"
    m.provenance.originator_nation = "ATL"
    m.provenance.releasable_to.append("BDR")
    m.window.window_start.FromNanoseconds(5_000_000_000)
    m.window.window_end.FromNanoseconds(5_000_000_000)
    m.window.sample_count = 0
    m.window.duration.SetInParent()
    r = m.element_rollup
    r.profile_name, r.element_count, r.critical_count, r.degraded_count = "p", 3, 1, 1
    r.avg_temp_c, r.avg_load_pct = 50.5, 20.0
    r.observed_at.FromNanoseconds(5_000_000_000)
    return m


@pytest.fixture(autouse=True)
def _env(monkeypatch):
    monkeypatch.setenv("OPENDDIL_EDGE_ID", "edge-test")
    monkeypatch.delenv("OPENDDIL_REGION_ID", raising=False)


def test_builder_matches_a_hand_built_message_and_sends_a_bytes_key():
    w = wiring(payload())
    _s, sends = w.process(raw(), 0, None)
    assert len(sends) == 1
    topic, key, msg = sends[0]
    assert (topic, key) == ("asset-telemetry-windows", b"a1")
    want = expected()
    assert msg == want


def test_optional_readouts_and_condition_ride_when_stated():
    cond = {"level": "LEVEL_RED", "moved_by": ["psu"]}
    w = wiring(payload(core_temp_c=41.5, uptime_hours=3.0, condition=cond))
    _s, sends = w.process(raw(), 0, None)
    r = sends[0][2].element_rollup
    assert r.core_temp_c == 41.5 and r.uptime_hours == 3.0 and r.HasField("condition")
    w2 = wiring(payload())
    r2 = w2.process(raw(), 0, None)[1][0][2].element_rollup
    assert not r2.HasField("core_temp_c") and not r2.HasField("uptime_hours")
    assert not r2.HasField("condition")


def test_malformed_condition_is_dropped_not_refused():
    w = wiring(payload(condition={"level": {"nested": 1}}))
    _s, sends = w.process(raw(), 0, None)
    assert len(sends) == 1 and not sends[0][2].element_rollup.HasField("condition")
    assert sends[0][2].element_rollup.element_count == 3


def test_no_observed_at_spans_the_clock_and_leaves_the_field_unset():
    w = wiring(payload(observed_at_ns=None))
    msg = w.process(raw(), 0, None)[1][0][2]
    assert not msg.element_rollup.HasField("observed_at")
    assert msg.window.window_start.ToNanoseconds() == 7_000_000_000 == msg.window.window_end.ToNanoseconds()


def test_unlabelled_stays_unlabelled():
    w = wiring(payload(originator_nation="", releasable_to=[]))
    msg = w.process(raw(), 0, None)[1][0][2]
    assert msg.provenance.originator_nation == "" and list(msg.provenance.releasable_to) == []


def test_region_comes_from_the_env(monkeypatch):
    monkeypatch.setenv("OPENDDIL_REGION_ID", "region-9")
    msg = wiring(payload()).process(raw(), 0, None)[1][0][2]
    assert msg.provenance.region_id == "region-9"


def test_no_unit_stamp_on_the_element_window():
    msg = wiring(payload()).process(raw(), 0, None)[1][0][2]
    assert not msg.provenance.HasField("producer_stamp")


TRENDS = {"window_start_ns": 1_000_000_000, "window_end_ns": 2_000_000_000, "sample_count": 4,
          "fluid_trends": {"oil": {"latest": 3.0, "latest_unit": "bar", "slope": 0.5, "slope_unit": "bar",
                                   "r_squared": 0.9, "latest_sample_ns": 2_000_000_000,
                                   "window_start_ns": 1_000_000_000, "sample_count": 4}},
          "consumable_trends": [], "wear_trends": [], "active_fault_codes": ["F1"]}


def test_trends_window_rides_with_its_sample_count():
    msg = wiring(payload(), trends=TRENDS).process(raw(), 0, None)[1][0][2]
    assert "oil" in msg.fluid_trends and list(msg.active_fault_codes_latest) == ["F1"]
    assert msg.window.sample_count == 4
    assert msg.window.window_start.ToNanoseconds() == 5_000_000_000


def test_no_trends_means_no_trends_and_sample_count_zero():
    msg = wiring(payload()).process(raw(), 0, None)[1][0][2]
    assert len(msg.fluid_trends) == 0 and msg.window.sample_count == 0


def test_unusable_trends_start_fresh_with_a_warning(caplog):
    bad = dict(TRENDS)
    del bad["fluid_trends"]
    msg = wiring(payload(), trends=bad).process(raw(), 0, None)[1][0][2]
    assert len(msg.fluid_trends) == 0 and msg.window.sample_count == 0
    assert any("unusable" in r.message for r in caplog.records)


@pytest.mark.parametrize("over", [
    {"element_count": True}, {"element_count": 1.5}, {"avg_temp_c": "x"}, {"profile_name": 3},
    {"platform_variant": 3}, {"releasable_to": [1]}, {"originator_nation": 3}])
def test_malformed_payload_is_refused_and_counted(over):
    w = wiring(payload(**over))
    _s, sends = w.process(raw(), 0, None)
    assert sends == [] and w.counters["output_refused"] == 1


def test_missing_required_payload_key_is_refused():
    p = payload()
    del p["avg_load_pct"]
    w = wiring(p)
    assert w.process(raw(), 0, None)[1] == [] and w.counters["output_refused"] == 1


# --- the runner's store -----------------------------------------------------------------
def test_wiring_stores_the_built_rollup():
    w = wiring(payload())
    w.process(raw(), 0, None)
    r = w.last_rollup["a1"]
    assert isinstance(r, winpb.ElementRollup) and r.element_count == 3 and r.profile_name == "p"


def test_a_refused_payload_stores_nothing():
    w = wiring(payload(element_count=True))
    w.process(raw(), 0, None)
    assert w.last_rollup == {}


def test_a_new_runner_starts_with_no_rollup():
    assert runner.Runner().last_rollup("a1") is None


def test_clock_is_restorable():
    outputs.set_clock(None)
    assert outputs._now_ns is not None and outputs._now_ns() > 7_000_000_000


def test_bytes_key_kinds_is_the_element_window_only():
    assert core.BYTES_KEY_KINDS == ("element_window",)
