"""Runtime extensions the ports need: the record_time_ns role, the sustainment
roles, memory state, the `window` output kind, the window rollup hook, the
legacy-env refusal and the runner's per-process state."""
from __future__ import annotations

import shutil
import textwrap
from pathlib import Path

import faust
import pytest
import yaml
from openddil.logistics.v1 import windowed_telemetry_pb2 as winpb
from openddil.telemetry.v1 import telemetry_pb2 as pb

from algorithm_runtime import core, declaration, outputs, roles, runner
from algorithm_runtime.contract import ROLES
from unit_helpers import ROOT

TRENDS_DIR = ROOT / "algorithms" / "trends"


# --- a small memory unit, enough to exercise the runner without the trends -----
MEM_YAML = {
    "apiVersion": "openddil.io/algorithm/v1", "unit": "memo", "version": "1.0.0",
    "runtime": "stream-parallel", "entrypoint": "memo.py:Memo", "key": "asset_id",
    "composition": "propagating",
    "inputs": {"topic": "raw-sensor-stream",
               "roles": [{"role": "record_time_ns", "required": True}, {"role": "fuel_remaining"}]},
    "params": {"every": {"type": "integer", "default": 2, "minimum": 1}},
    "state": {"persistence": "memory",
              "fields": {"seen": {"type": "object", "default": {}}, "n": {"type": "integer", "default": 0}}},
    "outputs": [{"kind": "window", "topic": "asset-telemetry-windows", "direction": "local",
                 "producer_id": "faust-edge", "source_protocol": "openddil.test.v1", "labels": "copy-source"}],
    "resources": {"memory_mi": 8, "cpu_m": 10},
}
MEMO_PY = textwrap.dedent('''
    from __future__ import annotations
    from algorithm_runtime.contract import Unit

    class Memo(Unit):
        def step(self, sample, state):
            state["seen"]["t"] = sample["record_time_ns"]
            state["n"] += 1
            if state["n"] % self.params["every"]:
                return None
            return {"window": {"window_start_ns": sample["record_time_ns"] - 10,
                               "window_end_ns": sample["record_time_ns"], "sample_count": 3,
                               "fluid_trends": {}, "consumable_trends": [], "wear_trends": [],
                               "active_fault_codes": ["P1"]}}
''')


def memo_dir(tmp_path: Path, doc: dict | None = None) -> Path:
    d = tmp_path / "memo"
    d.mkdir()
    (d / "unit.yaml").write_text(yaml.safe_dump(doc or MEM_YAML), encoding="utf-8")
    (d / "memo.py").write_text(MEMO_PY, encoding="utf-8")
    return d


def event(asset="a-1", *, fuel=None, items=None, hours=None, rul=None, faults=(), nation="", sample_time=None):
    e = pb.EntityTelemetryEvent()
    e.asset.asset_id = asset
    e.asset.platform_variant = "variant-x"
    if fuel is not None:
        e.sustainment.fluids.fuel_remaining.value, e.sustainment.fluids.fuel_remaining.unit = fuel
    for slot, (rem, cap, nsn) in (items or {}).items():
        it = e.sustainment.consumables.items[slot]
        it.quantity_remaining, it.quantity_capacity, it.nsn = rem, cap, nsn
    for comp, (v, u) in (hours or {}).items():
        c = e.sustainment.wear.components[comp]
        c.hours_in_service.value, c.hours_in_service.unit = v, u
    for comp, (v, u) in (rul or {}).items():
        c = e.sustainment.wear.components[comp]
        c.remaining_useful_life.value, c.remaining_useful_life.unit = v, u
    e.sustainment.health.active_fault_codes.extend(faults)
    if nation:
        e.provenance.originator_nation = nation
        e.provenance.releasable_to.extend(["AAA", "BBB"])
    if sample_time:
        e.provenance.sample_time.FromNanoseconds(sample_time)
    return e


# --- the roles --------------------------------------------------------------------
def test_new_roles_are_in_the_vocabulary():
    for r in ("record_time_ns", "fuel_remaining", "consumables", "wear_hours", "wear_rul",
              "active_fault_codes"):
        assert r in ROLES


def test_sustainment_roles_extract_with_units_and_the_protos_map_order():
    e = event(fuel=(40.0, "L"),
              items={"b_slot": (7, 10, "n-b"), "a_slot": (0, 0, "")},
              hours={"engine": (12.5, "h"), "bad": (3.0, "no such unit"), "zero": (0.0, "")},
              rul={"engine": (900.0, "h"), "orphan": (50.0, "%")},
              faults=("P2", "P1"))
    out = roles.extract(e)
    assert out["fuel_remaining"] == (40.0, "liter")
    assert out["consumables"] == {"b_slot": {"remaining": 7.0, "capacity": 10, "nsn": "n-b"},
                                  "a_slot": {"remaining": 0.0, "capacity": 0, "nsn": ""}}
    assert list(out["consumables"]) == list(e.sustainment.consumables.items)
    assert out["wear_hours"] == {"engine": (12.5, "hour")}      # unparseable and zero-no-unit are absent
    assert set(out["wear_rul"]) == {"engine", "orphan"}
    assert out["wear_rul"]["engine"] == (900.0, "hour")
    assert out["active_fault_codes"] == ("P2", "P1")


def test_fuel_role_absent_for_zero_with_no_unit_and_for_an_unparseable_unit():
    assert "fuel_remaining" not in roles.extract(event(fuel=(0.0, "")))
    assert "fuel_remaining" not in roles.extract(event(fuel=(5.0, "no such unit")))
    assert "fuel_remaining" not in roles.extract(event())


def test_sustainment_roles_are_computed_only_when_declared():
    out = roles.extract(event(fuel=(40.0, "L"), faults=("P1",)), {"sample_time_ns"})
    assert not ({"fuel_remaining", "consumables", "wear_hours", "wear_rul", "active_fault_codes"} & set(out))


# --- record_time_ns through process_one ---------------------------------------------
@pytest.fixture
def edge_env(monkeypatch):
    monkeypatch.setenv("OPENDDIL_EDGE_ID", "edge-tst")
    monkeypatch.setenv("OPENDDIL_REGION_ID", "region-tst")


def load_memo(tmp_path, doc=None, params=None):
    d = memo_dir(tmp_path, doc)
    decl = declaration.load(d / "unit.yaml")
    unit = runner_load(d, decl, params)
    return unit, decl


def runner_load(d, decl, params=None):
    from algorithm_runtime import loader
    return loader.load_unit_class(d, decl, RuntimeError)(declaration.resolve_params(decl, params))


def test_record_time_ns_is_present_only_when_given(tmp_path, edge_env):
    unit, decl = load_memo(tmp_path)
    raw = event().SerializeToString()
    state, sends, counters = core.process_one(unit, decl, raw, None, 5_000_000_000)
    assert state["seen"] == {"t": 5_000_000_000} and not sends
    state2, sends2, counters2 = core.process_one(unit, decl, raw, None)
    assert state2 is None and sends2 == [] and counters2["required_role_absent"] == 1


# --- memory state ----------------------------------------------------------------------
def test_memory_declaration_loads_and_has_no_table():
    d = declaration.validate(MEM_YAML)
    assert d.state_persistence == "memory"
    assert (d.state_table, d.state_partitions, d.state_record_namespace) == (None, None, None)
    assert [f.type for f in d.state_fields] == ["object", "integer"]


def test_durable_is_the_default_and_unchanged():
    from unit_helpers import wear_decl
    w = wear_decl()
    assert w.state_persistence == "durable" and w.state_table and w.state_partitions


def test_object_fields_are_refused_under_durable():
    import copy
    doc = copy.deepcopy(yaml.safe_load((ROOT / "algorithms" / "wear" / "unit.yaml").read_text(encoding="utf-8")))
    doc["state"]["fields"]["blob"] = {"type": "object", "default": {}}
    with pytest.raises(declaration.DeclarationError, match="blob.type"):
        declaration.validate(doc)


def test_memory_with_a_table_key_is_refused_naming_it():
    import copy
    for key, val in (("table", "t"), ("partitions", 1), ("record_namespace", "a.B")):
        doc = copy.deepcopy(MEM_YAML)
        doc["state"][key] = val
        with pytest.raises(declaration.DeclarationError, match=f"state.{key}"):
            declaration.validate(doc)


def test_unknown_persistence_is_refused():
    import copy
    doc = copy.deepcopy(MEM_YAML)
    doc["state"]["persistence"] = "disk"
    with pytest.raises(declaration.DeclarationError, match="state.persistence"):
        declaration.validate(doc)


def test_object_default_must_be_a_mapping():
    import copy
    doc = copy.deepcopy(MEM_YAML)
    doc["state"]["fields"]["seen"]["default"] = 3
    with pytest.raises(declaration.DeclarationError, match="seen.default"):
        declaration.validate(doc)


def test_default_state_gives_each_asset_its_own_object():
    d = declaration.validate(MEM_YAML)
    a, b = core.default_state(d), core.default_state(d)
    a["seen"]["x"] = 1
    assert b["seen"] == {} and d.state_fields[0].default == {}


def test_process_one_does_not_copy_memory_state(tmp_path, edge_env):
    unit, decl = load_memo(tmp_path)
    inner = {}
    state = {"seen": inner, "n": 0}
    new, _s, _c = core.process_one(unit, decl, event().SerializeToString(), state, 1_000_000_000)
    assert new["seen"] is inner and inner == {"t": 1_000_000_000}   # in place
    assert state["n"] == 0 and new["n"] == 1                       # the top level is a new dict


def test_a_step_that_raises_may_leave_a_partial_update_in_memory_state(tmp_path, edge_env):
    """The documented limit: the nested object is shared, so what a step wrote
    before raising stays (a durable unit's state is copied; the old agent had no
    handler at all)."""
    unit, decl = load_memo(tmp_path)
    inner = {}
    real = unit.step

    def boom(sample, state):
        state["seen"]["partial"] = True
        raise RuntimeError("x")
    unit.step = boom
    new, sends, counters = core.process_one(unit, decl, event().SerializeToString(),
                                            {"seen": inner, "n": 0}, 1_000_000_000)
    assert new is None and counters["step_failed"] == 1 and inner == {"partial": True}
    unit.step = real


# --- the window output kind -----------------------------------------------------------
def trend(latest=10.0, slope=-2.0, unit="liter", n=4, t0=1_000, t1=9_000):
    return {"latest": latest, "latest_unit": unit, "slope": slope, "slope_unit": unit,
            "r_squared": 0.5, "latest_sample_ns": t1, "window_start_ns": t0, "sample_count": n}


def window_payload():
    return {
        "window_start_ns": 1_000, "window_end_ns": 9_000, "sample_count": 4,
        "fluid_trends": {"fuel_remaining": trend()},
        "consumable_trends": [{"slot_key": "main", "remaining": trend(unit="count"),
                               "capacity": 40, "nsn": "n-1"},
                              {"slot_key": "orphan", "remaining": trend(unit="count")}],
        "wear_trends": [{"component_key": "engine", "hours_in_service": trend(unit="hour"),
                         "remaining_useful_life": trend(unit="percent")},
                        {"component_key": "track", "hours_in_service": trend(unit="hour")}],
        "active_fault_codes": ["P1", "P2"],
    }


def window_out():
    return declaration.validate(MEM_YAML).outputs[0]


def test_window_kind_is_in_the_kind_table():
    assert "window" in declaration.OUTPUT_KINDS


def test_window_builds_the_wire_message_with_pint_formatted_units(edge_env):
    msg = outputs.build_payload("window", window_payload())
    assert isinstance(msg, winpb.WindowedTelemetry)
    f = msg.fluid_trends["fuel_remaining"]
    assert (f.latest.value, f.latest.unit) == (10.0, "liter")
    assert (f.slope.value, f.slope.unit) == (-2.0, "liter / hour")      # unit + "/h" through pint
    assert f.r_squared == 0.5 and f.latest_sample_at.ToNanoseconds() == 9_000
    assert msg.window.sample_count == 4
    assert msg.window.window_start.ToNanoseconds() == 1_000 and msg.window.window_end.ToNanoseconds() == 9_000
    assert msg.window.duration.ToNanoseconds() == 8_000
    c = msg.consumable_trends
    assert [(t.slot_key, t.capacity, t.nsn) for t in c] == [("main", 40, "n-1"), ("orphan", 0, "")]
    w = msg.wear_trends
    assert [t.component_key for t in w] == ["engine", "track"]
    assert w[0].remaining_useful_life.latest.unit == "percent"
    assert not w[1].HasField("remaining_useful_life")
    assert list(msg.active_fault_codes_latest) == ["P1", "P2"]


def test_window_payload_that_is_not_a_window_is_refused():
    bad = window_payload()
    del bad["window_end_ns"]
    with pytest.raises(outputs.OutputError):
        outputs.build_payload("window", bad)
    bad = window_payload()
    bad["fluid_trends"]["fuel_remaining"]["latest_unit"] = "no such unit"
    with pytest.raises(outputs.OutputError):
        outputs.build_payload("window", bad)


def test_window_event_carries_identity_provenance_labels_and_the_stamp(edge_env):
    src = event(nation="XXX", sample_time=123_456_789_000)
    msg = outputs.build_payload("window", window_payload())
    ev = outputs.build_event(src, msg, window_out(), unit="memo", version="1.0.0",
                             config_hash="h", tier="edge-tst")
    assert isinstance(ev, winpb.WindowedTelemetry)
    assert ev.asset_id == "a-1" and ev.platform_variant == "variant-x"
    assert ev.computed_at.ToNanoseconds() > 1_600_000_000 * 10**9
    p = ev.provenance
    assert p.ingest_time.ToNanoseconds() == ev.computed_at.ToNanoseconds()
    assert p.sample_time.ToNanoseconds() == 123_456_789_000
    assert (p.producer_id, p.edge_id, p.region_id, p.classification) == (
        "faust-edge", "edge-tst", "region-tst", "U")
    assert p.source_protocol == ""                      # the old window carried none
    assert p.originator_nation == "XXX" and list(p.releasable_to) == ["AAA", "BBB"]
    s = p.producer_stamp
    assert (s.unit, s.version, s.config_hash, s.tier) == ("memo", "1.0.0", "h", "edge-tst")
    assert not ev.HasField("element_rollup")


def test_window_event_without_a_source_sample_time_or_labels_has_neither(edge_env):
    msg = outputs.build_payload("window", window_payload())
    ev = outputs.build_event(event(), msg, window_out(), unit="memo", version="1.0.0",
                             config_hash="h", tier="edge-tst")
    assert not ev.provenance.HasField("sample_time")
    assert ev.provenance.originator_nation == "" and not ev.provenance.releasable_to


def test_window_rollup_hook_attaches_the_asset_rollup(edge_env):
    ru = winpb.ElementRollup()
    ru.profile_name = "prof"
    msg = outputs.build_payload("window", window_payload())
    ev = outputs.build_event(event(), msg, window_out(), unit="memo", version="1.0.0",
                             config_hash="h", tier="edge-tst", rollup=ru)
    assert ev.element_rollup.profile_name == "prof"


def test_process_one_sends_a_window_keyed_by_asset_and_reports_the_payload(tmp_path, edge_env):
    unit, decl = load_memo(tmp_path, params={"every": 1})
    seen = []
    rolls = []

    def rollup(asset):
        rolls.append(asset)
        r = winpb.ElementRollup()
        r.profile_name = "p"
        return r
    _st, sends, _c = core.process_one(unit, decl, event().SerializeToString(), None, 9_000_000_000,
                                      window_rollup=rollup, on_payload=lambda *a: seen.append(a))
    assert [(t, k) for t, k, _m in sends] == [("asset-telemetry-windows", b"a-1")]
    assert sends[0][2].element_rollup.profile_name == "p" and rolls == ["a-1"]
    assert [(a, k) for a, k, _p in seen] == [("a-1", "window")]
    assert seen[0][2]["active_fault_codes"] == ["P1"]


def test_no_rollup_hook_means_no_rollup(tmp_path, edge_env):
    unit, decl = load_memo(tmp_path, params={"every": 1})
    _st, sends, _c = core.process_one(unit, decl, event().SerializeToString(), None, 9_000_000_000)
    assert not sends[0][2].HasField("element_rollup")


# --- the runner -------------------------------------------------------------------------
@pytest.fixture
def renv(monkeypatch, tmp_path):
    shutil.copytree(TRENDS_DIR, tmp_path / "trends", ignore=shutil.ignore_patterns("__pycache__"))
    for k in ("FLUID_WINDOW_MIN", "WEAR_WINDOW_MIN", "WINDOW_EMIT_EVERY_N", "WINDOW_DEQUE_CAP",
              "ALGORITHM_PARAMS_JSON"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("ALGORITHMS_DIR", str(tmp_path))
    monkeypatch.setenv("ALGORITHMS_ENABLED", "trends")
    monkeypatch.setenv("OPENDDIL_EDGE_ID", "edge-tst")
    monkeypatch.setenv("OPENDDIL_REGION_ID", "region-tst")
    return monkeypatch


def new_app():
    return faust.App("ports-runtime-test", broker="kafka://localhost:9092", store="memory://")


@pytest.mark.parametrize("name,param", [
    ("FLUID_WINDOW_MIN", "fluid_window_min"), ("WEAR_WINDOW_MIN", "wear_window_min"),
    ("WINDOW_EMIT_EVERY_N", "emit_every_n_samples"), ("WINDOW_DEQUE_CAP", "window_cap")])
def test_runner_refuses_each_legacy_window_env_and_names_the_param(renv, name, param):
    renv.setenv(name, "7")
    with pytest.raises(runner.StartupRefusal, match=name) as e:
        runner.register(new_app())
    assert param in str(e.value) and "ALGORITHM_PARAMS_JSON" in str(e.value)


def test_runner_registers_a_memory_unit_with_an_agent_and_no_table(renv):
    app = new_app()
    runner.register(app)
    assert len(app.agents) == 1 and not app.tables


def test_runner_hands_back_the_last_window_payload_per_asset(renv):
    r = runner.register(new_app())
    assert r.last_window("a-1") is None
    r.wirings["trends"].process(event("a-1", fuel=(10.0, "L")).SerializeToString(), 1_000_000_000)
    r.wirings["trends"].process(event("a-1", fuel=(9.0, "L")).SerializeToString(), 2_000_000_000)
    assert r.last_window("a-1") is None                   # N = 5: nothing emitted yet
    for i in range(3, 6):
        r.wirings["trends"].process(event("a-1", fuel=(10.0 - i, "L")).SerializeToString(), i * 1_000_000_000)
    w = r.last_window("a-1")
    assert w is not None and w["window_end_ns"] == 5_000_000_000 and "fuel_remaining" in w["fluid_trends"]
    assert r.last_window("other") is None


def test_memory_state_does_not_survive_a_new_runner(renv):
    r1 = runner.register(new_app())
    for i in range(1, 5):
        r1.wirings["trends"].process(event("a-1", fuel=(10.0, "L")).SerializeToString(), i * 10**9)
    assert r1.wirings["trends"].memory["a-1"]["samples_since_emit"] == 4
    r2 = runner.register(new_app())
    assert "a-1" not in r2.wirings["trends"].memory
    _st, sends = r2.wirings["trends"].process(event("a-1", fuel=(10.0, "L")).SerializeToString(), 5 * 10**9)
    assert sends == []                                    # a fresh counter: the 5th record is the 1st


def test_the_window_rollup_hook_reaches_the_sends(renv):
    ru = winpb.ElementRollup()
    ru.profile_name = "prof"
    r = runner.register(new_app(), window_rollup={"a-1": ru}.get)
    sends = []
    for i in range(1, 6):
        sends = r.wirings["trends"].process(event("a-1", fuel=(10.0 - i, "L")).SerializeToString(), i * 10**9)[1]
    assert len(sends) == 1 and sends[0][2].element_rollup.profile_name == "prof"


def test_two_units_may_not_both_emit_windows(renv, tmp_path):
    d = memo_dir(tmp_path)
    renv.setenv("ALGORITHMS_ENABLED", "trends,memo")
    with pytest.raises(runner.StartupRefusal, match="window"):
        runner.register(new_app())
    assert d.is_dir()
