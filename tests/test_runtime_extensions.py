"""The runtime extensions the posture port needs: the three posture roles
(always present, None for absent), the string state type, per-field memory
persistence inside a durable state, the transition output class, the two
unstamped output kinds, and declaration order of a step's outputs.

A toy unit stands in for posture here; the real unit's tests are in
test_posture_equivalence.py."""
from __future__ import annotations

import copy
import json

import pytest
import yaml
from jsonschema import Draft202012Validator
from openddil.telemetry.v1 import telemetry_pb2 as pb

from algorithm_runtime import declaration as d
from algorithm_runtime import outputs, roles, runner
from algorithm_runtime.contract import Unit
from test_ports_runtime import event

from unit_helpers import ROOT

SCHEMA = json.loads((ROOT / "algorithms" / "declaration.schema.json").read_text(encoding="utf-8"))

DOC = yaml.safe_load("""
apiVersion: openddil.io/algorithm/v1
unit: toy
version: "1.0.0"
runtime: stream-parallel
entrypoint: toy.py:Toy
key: asset_id
composition: propagating
inputs:
  topic: raw-sensor-stream
  roles:
    - {role: sample_time_s, required: true}
    - {role: deploy_signal}
    - {role: speed_mps}
params: {}
state:
  persistence: durable
  table: toy_state
  partitions: 1
  record_namespace: faust_edge.StateRecord
  fields:
    last_temp_k:      {type: number, default: 0.0}
    temp_ewma_k:      {type: number, default: 0.0}
    temp_ewma_alpha:  {type: number, default: 0.2}
    posture:          {type: string, default: unspecified}
    posture_since:    {type: number, nullable: true, default: null}
    motion:           {type: string, default: ""}
    motion_since:     {type: number, nullable: true, default: null}
    ever:             {type: boolean, default: false, persistence: memory}
outputs:
  - {kind: latest_state, topic: telemetry-latest-state, direction: local, stamp: none, labels: copy-source}
  - {kind: posture_transition, topic: tactical-events, direction: local, class: transition, on_change_of: posture, stamp: none, labels: copy-source}
resources: {memory_mi: 8, cpu_m: 10}
dependencies: []
""")

# What the retired in-line StateRecord (namespace faust_edge.StateRecord) wrote.
OLD_DEFAULT = (b'{"last_temp_k":0.0,"temp_ewma_k":0.0,"temp_ewma_alpha":0.2,"posture":"unspecified",'
               b'"posture_since":null,"motion":"","motion_since":null,'
               b'"__faust":{"ns":"faust_edge.StateRecord"}}')
OLD_FILLED = (b'{"last_temp_k":1.5,"temp_ewma_k":0.0,"temp_ewma_alpha":0.2,"posture":"moving",'
              b'"posture_since":12.5,"motion":"moving","motion_since":3.0,'
              b'"__faust":{"ns":"faust_edge.StateRecord"}}')


def mutated(fn):
    doc = copy.deepcopy(DOC)
    fn(doc)
    return doc


class Toy(Unit):
    """deploy_signal True -> emplaced, False -> moving, None -> unchanged."""

    seen = []

    def step(self, sample, state):
        sig = sample["deploy_signal"]
        Toy.seen.append(state["ever"])
        state["ever"] = state["ever"] or sig is not None
        prev = {k: state[k] for k in ("posture", "posture_since", "motion", "motion_since")}
        if sig is not None:
            new = "emplaced" if sig else "moving"
            if new != state["posture"]:
                state["posture"], state["posture_since"] = new, sample["sample_time_s"]
        cur = {k: state[k] for k in prev}
        out = {"latest_state": ({"untouched": True} if cur == prev else
                                {"posture": cur["posture"], "posture_since": cur["posture_since"]})}
        out["posture_transition"] = {"prev": prev, "new": cur, "deploy_signal": sig, "speed": None}
        return out


def decl():
    return d.validate(copy.deepcopy(DOC))


def wiring():
    dc = decl()
    return runner.Wiring(Toy(d.resolve_params(dc, None)), dc, lambda _a: None, {}), dc


def raw(asset="a-1", *, launcher=None, t=100.0, nation=""):
    e = event(asset, nation=nation)
    if launcher is not None:
        e.operational_state.launcher_raised = launcher
    e.provenance.sample_time.FromNanoseconds(int(t * 1e9))
    return e.SerializeToString()


# --- roles: always present, None for absent -----------------------------------------
def test_posture_roles_are_present_as_none_when_the_source_is_silent():
    e = pb.EntityTelemetryEvent()
    e.asset.asset_id = "a"
    e.provenance.sample_time.FromNanoseconds(5_000_000_000)
    got = roles.extract(e, {"deploy_signal", "speed_mps", "sample_time_s"})
    assert got["deploy_signal"] is None and got["speed_mps"] is None
    assert got["sample_time_s"] == 5.0


def test_a_required_posture_role_that_is_none_is_still_present():
    """A key present with None is present: the unit decides what absent means."""
    dc = decl()
    unit = Toy(d.resolve_params(dc, None))
    from algorithm_runtime import core
    state, sends, c = core.process_one(unit, dc, raw(), None, 0)
    assert state is not None and not c.get("required_role_absent")


def test_posture_roles_are_not_extracted_unless_named():
    e = pb.EntityTelemetryEvent()
    e.asset.asset_id = "a"
    assert "deploy_signal" not in roles.extract(e, {"fuel_remaining"})


# --- the string state type ------------------------------------------------------------
def test_generated_record_is_byte_identical_to_the_retired_state_record():
    cls = runner.make_record_class(decl())
    assert cls().dumps() == OLD_DEFAULT
    filled = cls(last_temp_k=1.5, posture="moving", posture_since=12.5, motion="moving", motion_since=3.0)
    assert filled.dumps() == OLD_FILLED
    assert cls.loads(OLD_FILLED, serializer="json").dumps() == OLD_FILLED


def test_record_leaves_the_memory_field_out():
    cls = runner.make_record_class(decl())
    assert "ever" not in json.loads(cls().dumps())


def test_a_string_default_must_be_a_string_and_is_never_nullable():
    with pytest.raises(d.DeclarationError, match="posture.default"):
        d.validate(mutated(lambda x: x["state"]["fields"]["posture"].update(default=1)))
    with pytest.raises(d.DeclarationError, match="nullable"):
        d.validate(mutated(lambda x: x["state"]["fields"]["posture"].update(nullable=True, default="a")))
    with pytest.raises(d.DeclarationError, match="ever.default"):
        d.validate(mutated(lambda x: x["state"]["fields"]["ever"].update(default="no")))


# --- per-field memory persistence -------------------------------------------------------
def test_memory_field_is_held_per_asset_and_not_written_to_the_record():
    w, _ = wiring()
    Toy.seen.clear()
    st, sends = w.process(raw("a-1", launcher=True), 0, None)
    assert "ever" not in st and set(st) == {"last_temp_k", "temp_ewma_k", "temp_ewma_alpha", "posture",
                                            "posture_since", "motion", "motion_since"}
    assert w.field_memory["a-1"] == {"ever": True}
    w.process(raw("a-1", launcher=True), 0, st)
    w.process(raw("a-2"), 0, None)
    assert Toy.seen == [False, True, False]      # a-2 has its own, a cold one
    assert "a-2" in w.field_memory and w.field_memory["a-2"] == {"ever": False}


def test_memory_field_returns_to_its_default_after_a_restart():
    w, _ = wiring()
    st, _ = w.process(raw(launcher=True), 0, None)
    Toy.seen.clear()
    w2, _ = wiring()                              # a new process: no memory, the record survives
    w2.process(raw(launcher=True), 0, st)
    assert Toy.seen == [False]


def test_a_durable_unit_needs_a_durable_field():
    def f(x):
        for v in x["state"]["fields"].values():
            v["persistence"] = "memory"
    with pytest.raises(d.DeclarationError, match="durable field"):
        d.validate(mutated(f))


def test_boolean_and_object_fields_must_be_memory_in_a_durable_unit():
    with pytest.raises(d.DeclarationError, match="type"):
        d.validate(mutated(lambda x: x["state"]["fields"]["ever"].pop("persistence")))


def test_a_memory_unit_refuses_a_durable_field():
    def f(x):
        x["state"] = {"persistence": "memory",
                      "fields": {"n": {"type": "integer", "default": 0, "persistence": "durable"}}}
    with pytest.raises(d.DeclarationError, match="persistence"):
        d.validate(mutated(f))


# --- transition outputs ---------------------------------------------------------------------
def test_transition_is_delivered_only_when_the_field_changed_and_counted_when_not():
    w, _ = wiring()
    st, sends = w.process(raw(launcher=True), 0, None)           # unspecified -> emplaced
    assert [s[0] for s in sends] == ["telemetry-latest-state", "tactical-events"]
    assert w.counters.get("transition_suppressed", 0) == 0
    st, sends = w.process(raw(launcher=True), 0, st)             # unchanged
    assert [s[0] for s in sends] == ["telemetry-latest-state"]
    assert w.counters["transition_suppressed"] == 1
    st, sends = w.process(raw(launcher=False, t=101.0), 0, st)   # emplaced -> moving
    assert [s[0] for s in sends] == ["telemetry-latest-state", "tactical-events"]
    assert w.counters["transition_suppressed"] == 1


def test_a_silent_signal_is_no_transition():
    w, _ = wiring()
    _, sends = w.process(raw(), 0, None)                         # signal absent: posture unchanged
    assert [s[0] for s in sends] == ["telemetry-latest-state"]
    assert w.counters["transition_suppressed"] == 1


@pytest.mark.parametrize("name,fn,needle", [
    ("no on_change_of", lambda x: x["outputs"][1].pop("on_change_of"), "on_change_of"),
    ("not a field", lambda x: x["outputs"][1].update(on_change_of="nope"), "on_change_of"),
    ("on a value output", lambda x: x["outputs"][0].update(on_change_of="posture"), "on_change_of"),
    ("unknown class", lambda x: x["outputs"][1].update({"class": "edge"}), "class"),
])
def test_transition_declaration_errors(name, fn, needle):
    doc = mutated(fn)
    with pytest.raises(d.DeclarationError, match=needle):
        d.validate(doc)
    if name != "not a field":      # naming a declared field is not expressible in the schema
        assert list(Draft202012Validator(SCHEMA).iter_errors(doc))


# --- stamp: none ---------------------------------------------------------------------------------
@pytest.mark.parametrize("name,fn,needle", [
    ("unstamped kind without stamp none", lambda x: x["outputs"][0].pop("stamp"), "stamp"),
    ("stamp none on a stamped kind",
     lambda x: x["outputs"][0].update(kind="window", producer_id="p", source_protocol="s"), "stamp"),
    ("producer field under stamp none", lambda x: x["outputs"][0].update(producer_id="p"), "producer_id"),
    ("stamped kind without producer fields",
     lambda x: x["outputs"][0].update(kind="window", stamp="producer"), "producer_id"),
    ("unknown stamp", lambda x: x["outputs"][0].update(stamp="maybe"), "stamp"),
])
def test_stamp_rules(name, fn, needle):
    doc = mutated(fn)
    with pytest.raises(d.DeclarationError, match=needle):
        d.validate(doc)
    assert list(Draft202012Validator(SCHEMA).iter_errors(doc)), name


def test_the_toy_declaration_is_accepted_by_both_validators():
    assert list(Draft202012Validator(SCHEMA).iter_errors(DOC)) == []
    d.validate(copy.deepcopy(DOC))


# --- the two builders --------------------------------------------------------------------------------
def test_latest_state_untouched_sends_the_input_bytes():
    w, _ = wiring()
    st, _ = w.process(raw(launcher=True), 0, None)
    r = raw(launcher=True, t=101.0)
    _, sends = w.process(r, 0, st)
    (topic, key, msg), = sends
    assert msg == r and key == "a-1" and isinstance(key, str)


def test_latest_state_touched_sets_the_posture_fields_on_the_event():
    w, _ = wiring()
    _, sends = w.process(raw(launcher=True, t=100.5), 0, None)
    got = pb.EntityTelemetryEvent()
    got.ParseFromString(sends[0][2])
    assert got.operational_state.posture_status == pb.POSTURE_STATUS_EMPLACED
    assert got.operational_state.posture_since.ToNanoseconds() == 100_500_000_000
    assert not got.provenance.HasField("producer_stamp")


def test_posture_transition_event_copies_labels_and_carries_no_stamp():
    w, _ = wiring()
    _, sends = w.process(raw(launcher=True, nation="AAA"), 0, None)
    ev = sends[1][2]
    assert ev["type"] == "openddil.posture.transition" and ev["subject"] == "a-1"
    assert "producer_stamp" not in (ev.get("data") or {})
    assert sends[1][1] == "a-1"


@pytest.mark.parametrize("kind,payload", [
    ("latest_state", {"posture": "flying", "posture_since": 1.0}),
    ("latest_state", {"posture": "moving"}),
    ("posture_transition", {"prev": {}, "new": {}, "deploy_signal": None, "speed": None}),
])
def test_builders_refuse_a_bad_payload(kind, payload):
    with pytest.raises(outputs.OutputError):
        outputs.build_payload(kind, payload)


# --- output order ---------------------------------------------------------------------------------------
def test_outputs_go_out_in_declaration_order_whatever_order_the_unit_returned_them():
    class Rev(Toy):
        def step(self, sample, state):
            out = super().step(sample, state)
            return dict(reversed(list(out.items())))
    dc = decl()
    w = runner.Wiring(Rev(d.resolve_params(dc, None)), dc, lambda _a: None, {})
    _, sends = w.process(raw(launcher=True), 0, None)
    assert [s[0] for s in sends] == ["telemetry-latest-state", "tactical-events"]


# --- legacy env ----------------------------------------------------------------------------------------------
@pytest.mark.parametrize("name,param", [("POSTURE_MOVE_SPEED_MPS", "move_speed_mps"),
                                        ("POSTURE_MOVE_HOLD_S", "move_hold_s"),
                                        ("POSTURE_STOP_HOLD_S", "stop_hold_s")])
def test_legacy_posture_env_is_refused_by_name(name, param, monkeypatch):
    monkeypatch.setenv(name, "3")
    with pytest.raises(runner.StartupRefusal) as e:
        runner._refuse_legacy_env()
    msg = str(e.value)
    assert name in msg and param in msg and "ALGORITHM_PARAMS_JSON" in msg and '"posture"' in msg
