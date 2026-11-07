"""The runner's startup contract and the state record it generates.

Every deployment mistake the runner knows about must stop it at register()
with the fix named; a runner that came up on defaults after being handed a
setting it no longer reads would emit plausible wrong numbers. The apps here
use faust's unconnected kafka:// URL (register() never connects): nothing touches a network."""
from __future__ import annotations

import json
import shutil
import types

import faust
import pytest

from algorithm_runtime import runner
from unit_helpers import WEAR_DIR, wear_decl

# A changelog value as the original engine wrote it (synthetic numbers).
CHANGELOG_VALUE = (
    b'{"cumulative_distance_m":12345.5,"observed_time_first_ns":1700000000000000000,'
    b'"observed_time_last_ns":1700000003600000000,"terrain_integral_deg_km":7.25,'
    b'"rounds_fired":3,"last_ecef_x":1.5,"last_ecef_y":-2.5,"last_ecef_z":3.0,'
    b'"last_pitch_deg":0.5,"last_roll_deg":null,'
    b'"__faust":{"ns":"prognostics.agent._AccumulatorRecord"}}'
)


@pytest.fixture
def algorithms_dir(tmp_path):
    shutil.copytree(WEAR_DIR, tmp_path / "wear", ignore=shutil.ignore_patterns("__pycache__"))
    return tmp_path


@pytest.fixture
def env(monkeypatch, algorithms_dir):
    """A valid startup environment; each refusal test breaks one thing."""
    for k in list(__import__("os").environ):
        if k.startswith("PROGNOSTICS_"):
            monkeypatch.delenv(k)
    monkeypatch.delenv("ALGORITHM_PARAMS_JSON", raising=False)
    monkeypatch.setenv("ALGORITHMS_DIR", str(algorithms_dir))
    monkeypatch.setenv("ALGORITHMS_ENABLED", "wear")
    monkeypatch.setenv("OPENDDIL_EDGE_ID", "edge-tst")
    return monkeypatch


def new_app():
    return faust.App("algorithm-runner-test", broker="kafka://localhost:9092", store="memory://")


def test_register_builds_the_declared_table_agent_and_topic(env):
    app = new_app()
    runner.register(app)
    table = app.tables["prognostics_accumulators"]
    assert table.changelog_topic.get_topic_name().endswith("prognostics_accumulators-changelog")
    assert len(app.agents) == 1


def test_register_logs_unit_version_hash_and_params(env, caplog):
    env.setenv("ALGORITHM_PARAMS_JSON", json.dumps({"wear": {"track_life_total_km": 10}}))
    with caplog.at_level("INFO", logger="algorithm_runtime"):
        runner.register(new_app())
    line = next(r.getMessage() for r in caplog.records if "registered" in r.getMessage())
    assert "unit=wear" in line and "version=1.0.0" in line and "config_hash=" in line
    assert "'track_life_total_km': 10.0" in line


def refuse(match):
    return pytest.raises(runner.StartupRefusal, match=match)


def test_refuses_when_algorithms_enabled_is_unset(env):
    env.delenv("ALGORITHMS_ENABLED")
    with refuse("ALGORITHMS_ENABLED"):
        runner.register(new_app())


def test_refuses_when_algorithms_dir_is_unset(env):
    env.delenv("ALGORITHMS_DIR")
    with refuse("ALGORITHMS_DIR"):
        runner.register(new_app())


def test_refuses_when_algorithms_dir_is_missing(env, tmp_path):
    env.setenv("ALGORITHMS_DIR", str(tmp_path / "nowhere"))
    with refuse("ALGORITHMS_DIR"):
        runner.register(new_app())


def test_refuses_when_a_named_unit_dir_is_missing(env):
    env.setenv("ALGORITHMS_ENABLED", "wear,ghost")
    with refuse("ghost"):
        runner.register(new_app())


def test_refuses_legacy_prognostics_env_and_names_the_param(env):
    env.setenv("PROGNOSTICS_TRACK_LIFE_TOTAL_KM", "10")
    with refuse("track_life_total_km.*ALGORITHM_PARAMS_JSON|ALGORITHM_PARAMS_JSON.*track_life_total_km"):
        runner.register(new_app())


def test_refuses_params_json_naming_a_unit_that_is_not_enabled(env):
    env.setenv("ALGORITHM_PARAMS_JSON", json.dumps({"other": {"x": 1}}))
    with refuse("other"):
        runner.register(new_app())


def test_refuses_params_json_that_is_not_json_or_not_an_object(env):
    env.setenv("ALGORITHM_PARAMS_JSON", "{nope")
    with refuse("ALGORITHM_PARAMS_JSON"):
        runner.register(new_app())
    env.setenv("ALGORITHM_PARAMS_JSON", "[1]")
    with refuse("ALGORITHM_PARAMS_JSON"):
        runner.register(new_app())


def test_refuses_an_override_for_a_param_the_unit_does_not_declare(env):
    env.setenv("ALGORITHM_PARAMS_JSON", json.dumps({"wear": {"track_life": 10}}))
    with pytest.raises(ValueError, match="track_life"):
        runner.register(new_app())


def test_refuses_when_edge_id_is_unset(env):
    env.delenv("OPENDDIL_EDGE_ID")
    with refuse("OPENDDIL_EDGE_ID"):
        runner.register(new_app())


def test_refuses_bindings_without_producer_stamp(env):
    class Descriptor:
        fields_by_name = {"producer_id": object()}
    fake_pb = types.SimpleNamespace(Provenance=types.SimpleNamespace(DESCRIPTOR=Descriptor))
    env.setattr(runner, "pb", fake_pb)
    with refuse("producer_stamp"):
        runner.register(new_app())


def test_refuses_two_units_claiming_one_table(env, algorithms_dir):
    twin = algorithms_dir / "wear2"
    shutil.copytree(algorithms_dir / "wear", twin)
    text = (twin / "unit.yaml").read_text(encoding="utf-8").replace("unit: wear\n", "unit: wear2\n")
    (twin / "unit.yaml").write_text(text, encoding="utf-8")
    env.setenv("ALGORITHMS_ENABLED", "wear,wear2")
    with refuse("prognostics_accumulators"):
        runner.register(new_app())


def test_refuses_a_unit_whose_source_fails_the_guard(env, algorithms_dir):
    with open(algorithms_dir / "wear" / "wear.py", "a", encoding="utf-8") as fh:
        fh.write("\nimport socket\n")
    with pytest.raises(ValueError, match=r"wear\.py:\d+"):
        runner.register(new_app())


def test_refuses_a_directory_whose_declaration_names_another_unit(env, algorithms_dir):
    (algorithms_dir / "other").mkdir()
    shutil.copy(algorithms_dir / "wear" / "unit.yaml", algorithms_dir / "other" / "unit.yaml")
    env.setenv("ALGORITHMS_ENABLED", "other")
    with refuse("expected 'other'"):
        runner.register(new_app())


# --- the generated state record -----------------------------------------------
def test_changelog_value_loads_into_the_generated_class_and_dumps_byte_identical():
    cls = runner.make_record_class(wear_decl())
    rec = cls.loads(CHANGELOG_VALUE, serializer="json")
    assert isinstance(rec, cls)
    assert (rec.cumulative_distance_m, rec.rounds_fired, rec.last_ecef_y, rec.last_roll_deg) == (
        12345.5, 3, -2.5, None)
    assert rec.dumps() == CHANGELOG_VALUE


def test_generated_class_fields_follow_the_declaration_in_order():
    decl = wear_decl()
    cls = runner.make_record_class(decl)
    assert list(json.loads(cls().dumps()))[:-1] == [f.name for f in decl.state_fields]
    assert cls().last_ecef_x is None and cls().cumulative_distance_m == 0.0
