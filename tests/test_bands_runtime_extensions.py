"""The runtime extensions the element-bands port needs: the json-envelope
input and its two roles, the `bands` param type, `many` outputs and the
`band_crossing` output kind (unstamped, host-supplied severity and time).

A toy unit stands in here; the real unit's tests are in
test_element_bands_equivalence.py."""
from __future__ import annotations

import copy
import json

import pytest
import yaml
from jsonschema import Draft202012Validator

from algorithm_runtime import core
from algorithm_runtime import declaration as d
from algorithm_runtime import outputs, roles, runner
from algorithm_runtime.contract import Unit

from unit_helpers import ROOT

SCHEMA = json.loads((ROOT / "algorithms" / "declaration.schema.json").read_text(encoding="utf-8"))

# A self-contained copy of what the real unit declares (algorithms/element_bands/unit.yaml),
# so these runtime tests stand without the unit.
DOC = yaml.safe_load("""
apiVersion: openddil.io/algorithm/v1
unit: element_bands
version: "1.0.0"
runtime: stream-parallel          # ADR-0034 execution class; the only one this runner accepts
entrypoint: element_bands_unit.py:ElementBands
key: asset_id
composition: propagating          # ADR-0034 composition typing
inputs:
  topic: asset-element-telemetry
  format: json-envelope           # the element snapshot; the host gates it (object, asset id, a rollup) before the step
  roles:
    - {role: observed_at_s, required: true}   # observed_at_ns / 1e9; an envelope without a numeric one yields nothing
    - {role: elements, required: true}        # (element_id, layer_name, raw element) in envelope order
params:                            # the element health bands: the numbers the chart's elementState.healthBands held
  bands:
    type: bands
    default:
      health:
        limits: [0.90, 0.97]       # health rises toward worse; the frontend's yellow > 0.90 and red > 0.97
        names: [green, yellow, red]
        hysteresis: 0.01
        severity: {green: info, yellow: warn, red: crit}
state:
  persistence: memory              # lost on a restart, as the old tracker was: the first observation after it is silent
  fields:
    bands_by_key: {type: object, default: {}}   # {element_id: {field: band index}} for this asset
outputs:
  - kind: band_crossing            # the openddil.element.band CloudEvent on tactical-events
    topic: tactical-events
    direction: local
    many: true                     # one per crossing, any number per envelope; an empty list sends nothing
    stamp: none                    # the event's shape stays what it was before this unit existed
    labels: copy-source
resources: {memory_mi: 8, cpu_m: 10}   # unmeasured; a ceiling for the render check, not a measurement
dependencies: []                   # must be empty: dependencies ship in the image, never fetched
""")


def mutated(fn):
    doc = copy.deepcopy(DOC)
    fn(doc)
    return doc


def bad(doc, match):
    with pytest.raises(d.DeclarationError, match=match):
        d.validate(doc)


def env(**over):
    e = {"asset_id": "a1", "observed_at_ns": 5_000_000_000, "originator_nation": "ATL",
         "releasable_to": ["BDR"],
         "elements": [{"element_id": "e1", "layer_name": "psu", "health": 0.5}]}
    e.update(over)
    return e


def raw(**over):
    return json.dumps(env(**over)).encode("utf-8")


# --- the declaration ------------------------------------------------------------------
def test_real_declaration_validates_and_matches_the_schema():
    decl = d.validate(copy.deepcopy(DOC))
    assert decl.input_format == "json-envelope"
    assert [o.many for o in decl.outputs] == [True]
    assert list(Draft202012Validator(SCHEMA).iter_errors(DOC)) == []


def test_default_input_format_is_protobuf():
    doc = yaml.safe_load((ROOT / "algorithms" / "posture" / "unit.yaml").read_text(encoding="utf-8"))
    assert d.validate(doc).input_format == "protobuf"


SCHEMA_REFUSALS = {
    "bad format": lambda x: x["inputs"].update(format="xml"),
    "many not bool": lambda x: x["outputs"][0].update(many="yes"),
    "bands param with a bound": lambda x: x["params"]["bands"].update(minimum=1),
    "unknown kind": lambda x: x["outputs"][0].update(kind="band_crossing2"),
}


@pytest.mark.parametrize("name", sorted(SCHEMA_REFUSALS))
def test_schema_and_validator_agree_on_refusals(name):
    doc = mutated(SCHEMA_REFUSALS[name])
    assert list(Draft202012Validator(SCHEMA).iter_errors(doc)), name
    with pytest.raises(d.DeclarationError):
        d.validate(doc)


def test_json_envelope_input_may_not_name_protobuf_roles():
    bad(mutated(lambda x: x["inputs"]["roles"].append({"role": "speed_mps"})), "speed_mps|role")


def test_protobuf_input_may_not_name_envelope_roles():
    def make(x):
        x["inputs"]["format"] = "protobuf"
    bad(mutated(make), "observed_at_s|elements|role")


def test_band_crossing_must_be_unstamped():
    bad(mutated(lambda x: x["outputs"][0].update(stamp="unit")), "stamp|unstamped")


# --- the roles --------------------------------------------------------------------------
def test_envelope_roles_extract():
    got = roles.extract_envelope(env(), {"observed_at_s", "elements"})
    assert got["observed_at_s"] == 5.0
    assert got["elements"] == (("e1", "psu", {"element_id": "e1", "layer_name": "psu", "health": 0.5}),)


@pytest.mark.parametrize("value", [None, True, "5", [1]])
def test_observed_at_s_absent_unless_a_number(value):
    assert "observed_at_s" not in roles.extract_envelope(env(observed_at_ns=value), {"observed_at_s"})


def test_elements_skip_non_dicts_and_unusable_ids():
    got = roles.extract_envelope(
        env(elements=[1, {"element_id": ""}, {"element_id": 3}, {"element_id": "ok"}]), {"elements"})
    assert [e[0] for e in got["elements"]] == ["ok"] and got["elements"][0][1] == ""


# --- bands param ------------------------------------------------------------------------
def test_bands_param_default_is_the_chart_values_and_validated():
    decl = d.validate(copy.deepcopy(DOC))
    p = d.resolve_params(decl, None)["bands"]["health"]
    assert p["limits"] == [0.90, 0.97] and p["names"] == ["green", "yellow", "red"]


def test_bands_override_replaces_whole_value():
    decl = d.validate(copy.deepcopy(DOC))
    new = {"temp_c": {"limits": [50.0], "names": ["a", "b"], "hysteresis": 1.0,
                      "severity": {"a": "info", "b": "warn"}}}
    assert d.resolve_params(decl, {"bands": new})["bands"] == new


def test_bad_default_is_refused_at_load():
    bad(mutated(lambda x: x["params"]["bands"].update(default={"health": {"limits": []}})), "bands")


# --- many outputs and the band_crossing kind ---------------------------------------------
class Toy(Unit):
    def __init__(self, params, out):
        super().__init__(params)
        self.out = out

    def step(self, sample, state):
        return {"band_crossing": self.out}


def crossing(**over):
    c = {"element_id": "e1", "layer_name": "psu", "field": "health",
         "from": "green", "to": "yellow", "value": 0.93}
    c.update(over)
    return c


def sends_for(out, **envelope):
    decl = d.validate(copy.deepcopy(DOC))
    w = runner.Wiring(Toy(d.resolve_params(decl, None), out), decl, lambda _a: None, {})
    _state, sends = w.process(raw(**envelope), 0, None)
    return w, sends


def test_many_sends_in_order_with_host_severity_time_and_labels():
    w, sends = sends_for([crossing(), crossing(element_id="e2", **{"from": "yellow", "to": "red"})])
    assert [(t, k) for t, k, _m in sends] == [("tactical-events", "a1")] * 2
    first, second = (m["data"] for _t, _k, m in sends)
    assert (first["element_path"], first["severity"]) == ("e1", "warn")
    assert (second["element_path"], second["severity"]) == ("e2", "crit")
    assert first["at"] == second["at"] == "1970-01-01T00:00:05.000000+00:00"
    assert first["originator_nation"] == "ATL" and first["releasable_to"] == ["BDR"]


def test_empty_list_sends_nothing():
    _w, sends = sends_for([])
    assert sends == []


def test_not_a_list_is_refused_not_sent():
    w, sends = sends_for(crossing())
    assert sends == [] and w.counters.get("output_refused") == 1


@pytest.mark.parametrize("item", [
    crossing(severity="crit"),           # the unit may not supply severity
    crossing(to="purple"),               # not a band of the configured names
    {"element_id": "e1"},                # fields missing
    "x",
])
def test_bad_crossing_is_refused(item):
    w, sends = sends_for([item])
    assert sends == [] and w.counters.get("output_refused") == 1


def test_producer_stamp_is_absent_on_band_crossing():
    _w, sends = sends_for([crossing()])
    data = sends[0][2]["data"]
    assert "producer_stamp" not in data and "producer_id" not in data and "source_protocol" not in data


def test_json_envelope_gates_have_counters():
    decl = d.validate(copy.deepcopy(DOC))
    unit = Toy(d.resolve_params(decl, None), [])
    for data, name in ((b"[]", "envelope_unreadable"), (json.dumps({"elements": []}).encode(), "no_asset_id")):
        _s, sends, c = core.process_one(unit, decl, data, None, 0)
        assert sends == [] and c[name] == 1
