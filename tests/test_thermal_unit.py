"""The thermal unit and what the anomaly output kind adds to the contract:
declaration keys, payload refusals, the CloudEvent envelope, wire bytes, the
temperature roles, the guard and the count-only replay."""
from __future__ import annotations

import base64
import copy
import gzip
import json
import socket
import uuid

import pytest
import yaml
from jsonschema import Draft202012Validator
from openddil.telemetry.v1 import telemetry_pb2 as pb

from algorithm_runtime import core, declaration as d, guard, outputs, replay, roles
from algorithm_runtime.contract import Unit
from test_thermal_equivalence import make_event
from unit_helpers import THERMAL_DIR, WEAR_DIR, make_thermal, thermal_decl, wear_decl

SCHEMA = json.loads((WEAR_DIR.parent / "declaration.schema.json").read_text(encoding="utf-8"))
THERMAL = yaml.safe_load((THERMAL_DIR / "unit.yaml").read_text(encoding="utf-8"))
WEAR = yaml.safe_load((WEAR_DIR / "unit.yaml").read_text(encoding="utf-8"))
OUT = ["outputs", 0]


@pytest.fixture(autouse=True)
def pinned(monkeypatch):
    def no_socket(*a, **k):
        raise AssertionError("network access attempted")
    monkeypatch.setattr(socket, "socket", no_socket)
    monkeypatch.setenv("OPENDDIL_EDGE_ID", "edge-tst")
    monkeypatch.setenv("OPENDDIL_REGION_ID", "region-tst")


def mutated(base, fn):
    doc = copy.deepcopy(base)
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


# --- the declaration --------------------------------------------------------------
def test_thermal_declaration_loads_and_the_schema_accepts_it():
    decl = thermal_decl()
    assert (decl.unit, decl.version, decl.state_table) == ("thermal", "1.0.0", "thermal_runaway_state")
    assert [f.name for f in decl.state_fields] == ["last_temp_k", "temp_ewma_k"]
    assert [(r.role, r.required) for r in decl.roles] == [("component_temp_k", True), ("ambient_temp_k", True)]
    o = decl.outputs[0]
    assert (o.kind, o.topic, o.event_type, o.severity) == (
        "anomaly", "tactical-events", "openddil.anomaly.thermal.runaway.v1", "critical")
    assert d.resolve_params(decl, None) == {
        "delta_threshold_k": 80.0, "rate_threshold_k_per_sample": 2.0, "ewma_alpha": 0.2}
    assert list(Draft202012Validator(SCHEMA).iter_errors(THERMAL)) == []
    # both units declare the same partitions, composition and labels
    assert decl.state_partitions == wear_decl().state_partitions
    assert decl.composition == wear_decl().composition == "propagating"


# (id, base document, mutation, substring the message must carry, expressible in the schema)
REFUSALS = [
    ("anomaly without event_type", THERMAL, _del(OUT + ["event_type"]), "event_type", True),
    ("anomaly without severity", THERMAL, _del(OUT + ["severity"]), "severity", True),
    ("event_type not under openddil.anomaly", THERMAL, _set(OUT + ["event_type"], "openddil.posture.x.v1"),
     "event_type", True),
    ("event_type with a capital", THERMAL, _set(OUT + ["event_type"], "openddil.anomaly.Thermal"),
     "event_type", True),
    ("event_type empty after the prefix", THERMAL, _set(OUT + ["event_type"], "openddil.anomaly."),
     "event_type", True),
    ("event_type not a string", THERMAL, _set(OUT + ["event_type"], 7), "event_type", True),
    ("empty severity", THERMAL, _set(OUT + ["severity"], ""), "severity", True),
    ("sustainment with severity", WEAR, _set(OUT + ["severity"], "critical"), "severity", True),
    ("sustainment with event_type", WEAR, _set(OUT + ["event_type"], "openddil.anomaly.x"), "event_type", True),
    ("unknown role", THERMAL, _set(["inputs", "roles", 0, "role"], "temp_f"), "role", True),
    ("alpha default above maximum", THERMAL, _set(["params", "ewma_alpha", "default"], 1.5),
     "ewma_alpha.default", False),
]
IDS = [r[0] for r in REFUSALS]


@pytest.mark.parametrize("name,base,mutation,needle,expressible", REFUSALS, ids=IDS)
def test_anomaly_declaration_refusals_name_the_key(name, base, mutation, needle, expressible):
    doc = mutated(base, mutation)
    with pytest.raises(d.DeclarationError) as e:
        d.validate(doc)
    assert needle in str(e.value)
    if expressible:
        assert list(Draft202012Validator(SCHEMA).iter_errors(doc)), "schema accepted what the validator refuses"


def test_ewma_alpha_above_its_maximum_is_refused_as_an_override():
    with pytest.raises(d.DeclarationError, match="above maximum"):
        d.resolve_params(thermal_decl(), {"ewma_alpha": 1.5})
    with pytest.raises(d.DeclarationError, match="below minimum"):
        d.resolve_params(thermal_decl(), {"delta_threshold_k": -1})
    with pytest.raises(d.DeclarationError, match="no such param"):
        d.resolve_params(thermal_decl(), {"delta_threshold": 90})
    assert d.resolve_params(thermal_decl(), {"ewma_alpha": 1})["ewma_alpha"] == 1.0


# --- payloads and envelopes -------------------------------------------------------
GOOD = {"summary": "s", "evidence": {"a": 1.5, "b": 2}}


@pytest.mark.parametrize("extra", ["severity", "labels", "provenance", "edge_id", "region_id",
                                   "originator_nation", "producer_stamp", "value_provenance"])
def test_an_anomaly_payload_with_another_key_is_refused(extra):
    with pytest.raises(outputs.OutputError, match=extra):
        outputs.build_payload("anomaly", {**GOOD, extra: "x"})


@pytest.mark.parametrize("payload", [
    {"summary": "s"}, {"evidence": {}}, {"summary": 1, "evidence": {}},
    {"summary": "s", "evidence": {"a": "hot"}}, {"summary": "s", "evidence": {"a": True}},
    {"summary": "s", "evidence": {1: 2.0}}, {"summary": "s", "evidence": [1.0]},
])
def test_a_malformed_anomaly_payload_is_refused(payload):
    with pytest.raises(outputs.OutputError):
        outputs.build_payload("anomaly", payload)


def test_a_good_anomaly_payload_is_passed_through_as_it_is():
    assert outputs.build_payload("anomaly", GOOD) == GOOD


def _anomaly_out():
    return thermal_decl().outputs[0]


def _event(src, **kw):
    return outputs.build_event(src, outputs.build_payload("anomaly", GOOD), _anomaly_out(),
                               unit="thermal", version="1.0.0", config_hash="h" * 64, tier="edge-tst", **kw)


def test_the_anomaly_envelope_for_an_unlabelled_source():
    src = make_event("a-1")
    ev = _event(src)
    assert set(ev) == {"specversion", "id", "source", "type", "subject", "time", "datacontenttype", "data"}
    assert (ev["specversion"], ev["source"], ev["type"], ev["subject"], ev["datacontenttype"]) == (
        "1.0", "openddil/edge/edge-tst/a-1", "openddil.anomaly.thermal.runaway.v1", "a-1", "application/json")
    assert uuid.UUID(ev["id"])
    assert ev["data"] == {
        "severity": "critical", "summary": "s", "evidence": {"a": 1.5, "b": 2},
        "edge_id": "edge-tst", "region_id": "region-tst",
        "provenance": {"origin": "DERIVED", "confidence": 0.0},
        "producer_stamp": {"unit": "thermal", "version": "1.0.0", "config_hash": "h" * 64, "tier": "edge-tst"},
    }


def test_the_labels_are_copied_only_from_a_labelled_source_and_never_from_the_environment(monkeypatch):
    monkeypatch.setenv("OPENDDIL_ORIGINATOR_NATION", "ZZZ")
    unlabelled = _event(make_event("a-1"))["data"]
    assert "originator_nation" not in unlabelled and "releasable_to" not in unlabelled
    labelled = _event(make_event("a-1", nation="AAA", releasable=["AAA", "BBB"]))["data"]
    assert (labelled["originator_nation"], labelled["releasable_to"]) == ("AAA", ["AAA", "BBB"])
    # releasable_to without a nation is no label at all
    src = make_event("a-1")
    src.provenance.releasable_to.extend(["AAA"])
    assert "releasable_to" not in _event(src)["data"]


def test_region_defaults_as_the_old_code_did(monkeypatch):
    monkeypatch.delenv("OPENDDIL_REGION_ID")
    assert _event(make_event("a-1"))["data"]["region_id"] == "region-01"


def test_encode_round_trips_both_kinds():
    ev = _event(make_event("a-1"))
    wire = outputs.encode(ev)
    assert isinstance(wire, bytes) and json.loads(wire) == ev
    assert wire == json.dumps(ev).encode("utf-8")

    msg = outputs.build_event(
        make_event("a-1"), outputs.build_payload("sustainment", {}), wear_decl().outputs[0],
        unit="wear", version="1.0.0", config_hash="h" * 64, tier="edge-tst")
    wire = outputs.encode(msg)
    assert wire == msg.SerializeToString()
    assert pb.EntityTelemetryEvent.FromString(wire) == msg


class Rogue(Unit):
    """A unit that tries to state what is the contract's."""
    def step(self, sample, state):
        return {"anomaly": {"summary": "s", "evidence": {}, "severity": "info"}}


def test_a_unit_stating_its_own_severity_is_refused_and_counted_but_its_state_is_kept():
    decl = thermal_decl()
    evt = make_event("a-1", (300.0, "K"), (290.0, "K"))
    new_state, sends, counters = core.process_one(Rogue({}), decl, evt.SerializeToString(), None)
    assert sends == [] and counters["output_refused"] == 1
    assert new_state is not None


# --- the unit, the guard, the roles ------------------------------------------------
def test_thermal_source_passes_the_guard():
    guard.check_unit_dir(THERMAL_DIR)


def test_thresholds_are_the_declared_params():
    def fires(params):
        unit, decl = make_thermal(params)
        state = core.default_state(decl)
        assert unit.step({"component_temp_k": 380.0, "ambient_temp_k": 300.0}, state) is None
        return unit.step({"component_temp_k": 385.0, "ambient_temp_k": 300.0}, state)

    out = fires({})
    assert out["anomaly"]["summary"] == "Component 85.0K above ambient, rising 5.00K/sample"
    assert list(out["anomaly"]["evidence"]) == [
        "component_temp_k", "ambient_temp_k", "delta_k", "rate_k_per_sample"]
    assert fires({"delta_threshold_k": 90}) is None
    assert fires({"rate_threshold_k_per_sample": 5.0}) is None   # strict: a rate of exactly 5.0 does not fire
    assert fires({"rate_threshold_k_per_sample": 4.9}) is not None


def test_the_zero_kelvin_write_back_is_the_stored_or_zero():
    unit, decl = make_thermal()
    state = core.default_state(decl)
    unit.step({"component_temp_k": 0.0, "ambient_temp_k": 300.0}, state)
    assert state == {"last_temp_k": 0.0, "temp_ewma_k": 0.0}
    unit.step({"component_temp_k": -0.0, "ambient_temp_k": 300.0}, state)
    assert str(state["last_temp_k"]) == "0.0"   # -0.0 is stored as 0.0, as `x or 0.0` did


def test_temperatures_are_converted_only_when_a_unit_declares_them(monkeypatch):
    import detection.units as units

    def boom(q):
        raise AssertionError("pint was used")
    monkeypatch.setattr(units, "from_proto", boom)
    evt = make_event("a-1", (300.0, "K"), (290.0, "K"))
    out = roles.extract(evt, {"pitch_deg", "position_ecef_m", "sample_time_ns"})
    assert "component_temp_k" not in out and "ambient_temp_k" not in out
    out = roles.extract(evt, {r.role for r in wear_decl().roles})  # wear's own path
    assert "component_temp_k" not in out


def test_a_temperature_role_is_absent_unless_the_source_set_it():
    evt = make_event("a-1", (0.0, ""), (0.0, "degC"))
    out = roles.extract(evt, {"component_temp_k", "ambient_temp_k"})
    assert "component_temp_k" not in out and out["ambient_temp_k"] == 273.15
    assert "role_unconvertible" not in out


# --- count-only replay ---------------------------------------------------------------
def _raw_rows(n):
    rows = []
    comp = {"a-1": 300.0, "a-2": 300.0}
    for i in range(n):
        a = "a-1" if i % 2 == 0 else "a-2"
        if i % 7 == 6:
            value = b"\xff\xff\xff\xff"
        elif i % 11 == 10:
            value = make_event(a, (5.0, "no_such_unit"), (300.0, "K")).SerializeToString()
        elif i % 13 == 12:
            value = make_event("").SerializeToString()
        else:
            comp[a] += 5.0 if a == "a-1" else 0.5
            value = make_event(a, (comp[a], "K"), (290.0, "K"), "AAA", ["AAA"]).SerializeToString()
        rows.append((i, 1000 + i, a.encode(), value))
    return rows


def test_a_unit_without_a_sustainment_output_is_replayed_count_only():
    unit, decl = make_thermal()
    rows = _raw_rows(80)
    report = replay.replay(unit, decl, unit.params, iter(rows), {}, iter(()),
                           {"OPENDDIL_EDGE_ID": "edge-tst", "OPENDDIL_REGION_ID": "region-tst"})
    assert report.mode == "count-only"
    assert report.topics == {"raw-sensor-stream": 80}
    assert report.compared == report.equal == report.different == 0
    assert report.emitted["anomaly"] > 0 and "sustainment" not in report.emitted
    c = report.core_counters
    assert c["parse_failed"] == len([i for i in range(80) if i % 7 == 6])
    assert c["role_unconvertible"] > 0 and c["no_asset_id"] > 0
    assert report.end_state == {"assets": 2}
    # the same rows through process_one by hand give the same number of events
    states, fired = {}, 0
    for _o, _t, _k, v in rows:
        evt = pb.EntityTelemetryEvent()
        try:
            evt.ParseFromString(v)
        except Exception:  # noqa: BLE001
            evt = None
        key = evt.asset.asset_id if evt else ""
        new, sends, _ = core.process_one(unit, decl, v, states.get(key) if key else None)
        if new is not None:
            states[key] = new
        fired += len(sends)
    assert fired == report.emitted["anomaly"]


def test_the_cli_says_it_is_count_only(tmp_path, capsys, monkeypatch):
    rows = _raw_rows(40)
    raw = tmp_path / f"raw-sensor-stream.0-{len(rows) - 1}.gz"
    with gzip.open(raw, "wb") as fh:
        for o, ts, k, v in rows:
            fh.write(b" ".join([str(o).encode(), str(ts).encode(),
                                base64.b64encode(k), base64.b64encode(v)]) + b"\n")
    out = tmp_path / "report.json"
    rc = replay.main(["--unit-dir", str(THERMAL_DIR), "--edge-id", "edge-tst", "--region-id", "region-tst",
                      "--raw", str(raw), "--out", str(out)])
    text = capsys.readouterr().out
    assert rc == 0
    assert text.splitlines()[0].startswith("mode: COUNT-ONLY, no recorded-side compare")
    assert any(l.startswith("emitted: ") for l in text.splitlines())
    assert json.loads(out.read_text(encoding="utf-8"))["mode"] == "count-only"


def test_a_sustainment_unit_still_needs_its_recorded_side(tmp_path, capsys):
    raw = tmp_path / "raw-sensor-stream.0-0.gz"
    with gzip.open(raw, "wb") as fh:
        fh.write(b"0 1000 " + base64.b64encode(b"k") + b" " + base64.b64encode(b"v") + b"\n")
    rc = replay.main(["--unit-dir", str(WEAR_DIR), "--edge-id", "e", "--region-id", "r",
                      "--raw", str(raw), "--out", str(tmp_path / "o.json")])
    assert rc == 2
    assert "--recorded is required for wear" in capsys.readouterr().err
