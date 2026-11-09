"""Declaration validation: every refusal names its key, the python validator
and the JSON Schema agree, and parameter resolution is typed."""
from __future__ import annotations

import copy
import json

import pytest
import yaml
from jsonschema import Draft202012Validator

from algorithm_runtime import declaration as d
from unit_helpers import WEAR_DIR, wear_decl

SCHEMA = json.loads((WEAR_DIR.parent / "declaration.schema.json").read_text(encoding="utf-8"))
VALID = yaml.safe_load((WEAR_DIR / "unit.yaml").read_text(encoding="utf-8"))


def mutated(fn):
    doc = copy.deepcopy(VALID)
    fn(doc)
    return doc


def _set(path, value):
    def f(doc):
        node = doc
        for k in path[:-1]:
            node = node[k]
        node[path[-1]] = value
    return f


def _del(path):
    def f(doc):
        node = doc
        for k in path[:-1]:
            node = node[k]
        del node[path[-1]]
    return f


def _terminal_upward(doc):
    doc["composition"] = "terminal"
    doc["outputs"][0]["direction"] = "upward"


# (id, mutation, substring the message must carry, expressible in the schema)
REFUSALS = [
    ("unknown top-level key", _set(["surprise"], 1), "surprise", True),
    ("apiVersion", _set(["apiVersion"], "openddil.io/algorithm/v2"), "apiVersion", True),
    ("runtime", _set(["runtime"], "durable-workflow"), "runtime", True),
    ("labels", _set(["outputs", 0, "labels"], "high-water"), "labels", True),
    ("unknown role", _set(["inputs", "roles", 0, "role"], "airspeed_mps"), "role", True),
    ("missing default", _del(["params", "critical_fraction", "default"]), "default", True),
    ("default of the wrong type (string)", _set(["params", "critical_fraction", "default"], "1.0"),
     "critical_fraction.default", True),
    ("default of the wrong type (fraction for integer)",
     _set(["params", "barrel_life_total_rounds", "default"], 1000.5),
     "barrel_life_total_rounds.default", True),
    ("state default of the wrong type", _set(["state", "fields", "rounds_fired", "default"], "0"),
     "rounds_fired.default", True),
    ("null default without nullable", _set(["state", "fields", "rounds_fired", "default"], None),
     "rounds_fired.default", True),
    ("output kind not in the kind table", _set(["outputs", 0, "kind"], "forecast"), "kind", True),
    ("composition terminal with an upward output", _terminal_upward, "direction", True),
    ("non-empty dependencies", _set(["dependencies"], ["numpy"]), "dependencies", True),
    ("missing resources", _del(["resources"]), "resources", True),
    # Not expressible in JSON Schema (compares two instance values): the
    # runtime validator alone enforces these.
    ("default below minimum", _set(["params", "degraded_fraction", "default"], -0.1),
     "degraded_fraction.default", False),
    ("default above maximum", lambda doc: doc["params"]["degraded_fraction"].update(maximum=0.4),
     "degraded_fraction.default", False),
]
IDS = [r[0] for r in REFUSALS]


def schema_errors(doc):
    return list(Draft202012Validator(SCHEMA).iter_errors(doc))


def test_wear_declaration_loads_and_the_schema_accepts_it():
    decl = wear_decl()
    assert decl.unit == "wear" and decl.input_topic == "raw-sensor-stream"
    assert [f.name for f in decl.state_fields][:2] == ["cumulative_distance_m", "observed_time_first_ns"]
    assert schema_errors(VALID) == []
    Draft202012Validator.check_schema(SCHEMA)


@pytest.mark.parametrize("name,mutation,needle,expressible", REFUSALS, ids=IDS)
def test_refusal_names_the_key(name, mutation, needle, expressible):
    with pytest.raises(d.DeclarationError) as e:
        d.validate(mutated(mutation))
    assert needle in str(e.value)


@pytest.mark.parametrize("name,mutation,needle,expressible", REFUSALS, ids=IDS)
def test_schema_and_validator_agree(name, mutation, needle, expressible):
    doc = mutated(mutation)
    with pytest.raises(d.DeclarationError):
        d.validate(doc)
    if expressible:
        assert schema_errors(doc), f"schema accepted a declaration the validator refuses: {name}"


def test_composability_gate_red_test():
    """ADR-0034: a terminal operation may not be emitted upward. Local is fine."""
    with pytest.raises(d.DeclarationError, match="terminal"):
        d.validate(mutated(_terminal_upward))
    ok = mutated(lambda doc: doc.update(composition="terminal"))
    assert d.validate(ok).composition == "terminal"
    assert schema_errors(ok) == []


def test_missing_required_top_level_keys_are_refused():
    for key in ("unit", "inputs", "params", "state", "outputs", "composition"):
        with pytest.raises(d.DeclarationError, match=key):
            d.validate(mutated(_del([key])))


# --- parameter resolution ------------------------------------------------------
def test_defaults_resolve_to_the_original_values():
    p = d.resolve_params(wear_decl(), None)
    assert p == {"track_life_total_km": 5000.0, "engine_life_total_hours": 5000.0,
                 "barrel_life_total_rounds": 1000, "terrain_total_deg_km": 100000.0,
                 "critical_fraction": 1.0, "service_due_fraction": 0.9, "degraded_fraction": 0.5}
    assert isinstance(p["track_life_total_km"], float)
    assert isinstance(p["barrel_life_total_rounds"], int)


def test_override_unknown_param_refused():
    with pytest.raises(d.DeclarationError, match="no_such_param"):
        d.resolve_params(wear_decl(), {"no_such_param": 1})


@pytest.mark.parametrize("override", [{"track_life_total_km": "10"}, {"barrel_life_total_rounds": "5"},
                                      {"track_life_total_km": True}, {"barrel_life_total_rounds": 2.5}])
def test_override_wrong_type_refused(override):
    with pytest.raises(d.DeclarationError):
        d.resolve_params(wear_decl(), override)


def test_override_integer_for_number_coerced_and_integral_float_for_integer_accepted():
    p = d.resolve_params(wear_decl(), {"track_life_total_km": 10, "barrel_life_total_rounds": 50.0})
    assert p["track_life_total_km"] == 10.0 and isinstance(p["track_life_total_km"], float)
    assert p["barrel_life_total_rounds"] == 50 and isinstance(p["barrel_life_total_rounds"], int)


def test_override_below_minimum_refused():
    with pytest.raises(d.DeclarationError, match="below minimum"):
        d.resolve_params(wear_decl(), {"track_life_total_km": -1})


def test_config_hash_is_canonical_and_tracks_every_input():
    p = d.resolve_params(wear_decl(), None)
    h = d.config_hash("wear", "1.0.0", p)
    assert len(h) == 64 and h == d.config_hash("wear", "1.0.0", dict(reversed(list(p.items()))))
    assert h != d.config_hash("wear", "1.0.1", p)
    assert h != d.config_hash("wear", "1.0.0", {**p, "critical_fraction": 0.99})
    # an override equal to the default is the same effective config
    assert h == d.config_hash("wear", "1.0.0", d.resolve_params(wear_decl(), {"track_life_total_km": 5000}))


# ---- the measured block ----------------------------------------------------
MEASURED = {
    "date": "2026-11-09", "recording": "second lab, rev 30, t0 2026-11-08T02:12:42Z, through the 2026-11-09T02:23Z pull",
    "basis": "recorded", "host": "lane 03 dev host, not the edge node; python 3.12",
    "records": 100, "assets": 3, "rate_per_s": 1.5,
    "per_sample_us": {"mean": 100.0, "p99": 250.0, "max": 9000.0},
    "peak_python_mib": 0.12, "state_bytes_per_asset": 375,
}


def with_measured(mutation=None):
    doc = copy.deepcopy(VALID)
    doc["measured"] = copy.deepcopy(MEASURED)
    if mutation:
        mutation(doc)
    return doc


def test_a_unit_without_a_measured_block_loads():
    doc = copy.deepcopy(VALID)
    doc.pop("measured", None)
    assert d.validate(doc).measured is None
    assert schema_errors(doc) == []


def test_a_measured_block_loads_and_the_schema_accepts_it():
    decl = d.validate(with_measured())
    assert decl.measured == MEASURED
    assert schema_errors(with_measured()) == []
    ok = with_measured(lambda doc: doc["measured"].update(basis="recorded-timing-synthetic-values"))
    assert d.validate(ok).measured["basis"] == "recorded-timing-synthetic-values"


MEASURED_REFUSALS = [
    ("unknown key", lambda m: m.update(extra=1), "extra", True),
    ("missing key", lambda m: m.pop("host"), "host", True),
    ("missing per_sample_us.p99", lambda m: m["per_sample_us"].pop("p99"), "p99", True),
    ("unknown per_sample_us key", lambda m: m["per_sample_us"].update(p50=1.0), "p50", True),
    ("date not a date", lambda m: m.update(date="09-11-2026"), "measured.date", True),
    ("date not a calendar date", lambda m: m.update(date="2026-13-45"), "measured.date", False),
    ("date left unquoted (yaml makes it a date)", lambda m: m.update(date=__import__("datetime").date(2026, 11, 9)),
     "measured.date", True),
    ("basis", lambda m: m.update(basis="estimated"), "measured.basis", True),
    ("records not an integer", lambda m: m.update(records=1.5), "measured.records", True),
    ("negative rate", lambda m: m.update(rate_per_s=-1.0), "measured.rate_per_s", True),
    ("per_sample_us negative", lambda m: m["per_sample_us"].update(mean=-1.0), "per_sample_us.mean", True),
]


@pytest.mark.parametrize("name,mutation,needle,expressible", MEASURED_REFUSALS, ids=[r[0] for r in MEASURED_REFUSALS])
def test_measured_refusals_name_the_key_and_the_schema_agrees(name, mutation, needle, expressible):
    doc = with_measured(lambda x: mutation(x["measured"]))
    with pytest.raises(d.DeclarationError) as e:
        d.validate(doc)
    assert needle in str(e.value)
    if expressible and name != "date left unquoted (yaml makes it a date)":
        assert schema_errors(doc), f"schema accepted a measured block the validator refuses: {name}"


def test_a_measured_block_and_changed_resources_leave_config_hash_where_it_was():
    from algorithm_runtime import core

    class U:  # `_stamp_hash` reads only `params`
        pass
    plain = copy.deepcopy(VALID)
    plain.pop("measured", None)
    changed = with_measured(lambda doc: doc["resources"].update(memory_mi=1, cpu_m=1))
    hashes = []
    for doc in (plain, changed):
        decl = d.validate(doc)
        u = U()
        u.params = d.resolve_params(decl, None)
        hashes.append((d.config_hash(decl.unit, decl.version, u.params), core._stamp_hash(u, decl)))
    assert hashes[0] == hashes[1] and hashes[0][0] == hashes[0][1]
