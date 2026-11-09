"""The replay harness's --measure mode: the keys of its result, the cpu
formula, the basis, the steps_run rule and the thermal synth. Timings and
memory sizes are never asserted, only what is derived from them."""
from __future__ import annotations

import base64
import gzip
import json
import random
import sys
from pathlib import Path

import pytest
from openddil.telemetry.v1 import telemetry_pb2 as pb

from algorithm_runtime import core, declaration, replay
from unit_helpers import ROOT, THERMAL_DIR, WEAR_DIR

ALGS = ROOT / "algorithms"
EDGE = {"OPENDDIL_EDGE_ID": "edge-tst", "OPENDDIL_REGION_ID": "region-tst"}
KEYS = {"unit", "basis", "synth", "synth_seed", "python", "records_read", "steps_run", "records", "assets",
        "span_s", "rate_per_s", "per_sample_ns", "cpu_millicores", "peak_python_mib", "end_python_mib",
        "state_bytes_per_asset", "core_counters"}


def raw_file(tmp: Path, n=60, n_assets=3, step_ms=500, position=True) -> str:
    """A small recording: n records round-robin over the assets, step_ms apart."""
    rng = random.Random(3)
    d = tmp / "raw"
    d.mkdir()
    path = d / f"raw-sensor-stream.0-{n - 1}.gz"
    with gzip.open(path, "wb") as fh:
        for i in range(n):
            a = f"asset-{i % n_assets}"
            evt = pb.EntityTelemetryEvent()
            evt.asset.asset_id = a
            if position:
                evt.kinematics.position.ecef.x.value = 6_371_000.0 + rng.uniform(-50, 50)
                evt.kinematics.position.ecef.y.value = rng.uniform(-50, 50)
                evt.kinematics.position.ecef.z.value = rng.uniform(-50, 50)
                evt.kinematics.position.ecef.x.unit = "m"
                evt.kinematics.position.valid_at.seconds = 1_800_000_000 + i
            evt.provenance.sample_time.seconds = 1_800_000_000 + i
            raw = evt.SerializeToString()
            fh.write(b" ".join([str(i).encode(), str(1_000_000 + i * step_ms).encode(),
                                base64.b64encode(a.encode()), base64.b64encode(raw)]) + b"\n")
    return str(d / "raw-sensor-stream.*.gz")


def run(unit_dir, glob, **kw):
    decl = declaration.load(unit_dir / "unit.yaml")
    params = declaration.resolve_params(decl, None)
    return replay.measure(unit_dir, decl, params, replay._expand(glob, "raw"), EDGE, **kw), decl


def test_keys_formula_and_a_recorded_basis(tmp_path):
    res, _ = run(WEAR_DIR, raw_file(tmp_path, n=60, step_ms=500))
    assert set(res) == KEYS
    assert set(res["per_sample_ns"]) == {"mean", "p50", "p99", "max"}
    assert res["basis"] == "recorded" and res["synth"] is None
    assert res["records_read"] == 60 and res["steps_run"] == 60 and res["records"] == res["steps_run"]
    assert res["assets"] == 3
    # span from the first and last ts_ms: 59 steps of 500 ms
    assert res["span_s"] == pytest.approx(29.5)
    assert res["rate_per_s"] == pytest.approx(60 / 29.5)
    t = res["per_sample_ns"]
    assert t["mean"] > 0 and t["p50"] <= t["p99"] <= t["max"]
    assert res["cpu_millicores"] == pytest.approx(t["mean"] * res["rate_per_s"] / 1e6)
    assert res["peak_python_mib"] >= res["end_python_mib"] >= 0
    assert isinstance(res["state_bytes_per_asset"], int) and res["state_bytes_per_asset"] > 0


def test_durable_state_bytes_are_the_encoded_record_length(tmp_path):
    res, decl = run(WEAR_DIR, raw_file(tmp_path, n=9, n_assets=1))
    state = {f.name: f.default for f in decl.state_fields}
    assert res["assets"] == 1
    # the record carries every field and the namespace; the numbers differ, so
    # compare the shape's lower bound: the defaults' own encoding
    floor = len(json.dumps({**state, "__faust": {"ns": decl.state_record_namespace}}, separators=(",", ":")))
    assert res["state_bytes_per_asset"] >= floor - 20


def test_memory_state_bytes_come_from_the_memory_pass(tmp_path):
    res, decl = run(ALGS / "trends", raw_file(tmp_path, n=60), synth="trends")
    assert decl.state_persistence == "memory"
    assert res["state_bytes_per_asset"] == round(res["end_python_mib"] * 1024 * 1024 / res["assets"])


def test_a_synth_makes_the_basis_synthetic_and_is_repeatable(tmp_path):
    g = raw_file(tmp_path)
    a, _ = run(ALGS / "trends", g, synth="trends", synth_seed=5)
    b, _ = run(ALGS / "trends", g, synth="trends", synth_seed=5)
    assert a["basis"] == "recorded-timing-synthetic-values" and a["synth"] == "trends" and a["synth_seed"] == 5
    assert (a["steps_run"], a["assets"], a["state_bytes_per_asset"]) == (b["steps_run"], b["assets"], b["state_bytes_per_asset"])


def test_a_run_that_reaches_no_step_fails_instead_of_writing_a_number(tmp_path):
    # thermal's two required roles are not in the recording
    with pytest.raises(replay.ReplayInputError, match="steps_run is 0"):
        run(THERMAL_DIR, raw_file(tmp_path))


def test_the_thermal_synth_runs_the_whole_unit(tmp_path):
    g = raw_file(tmp_path, n=3000, n_assets=3, step_ms=100)
    res, _ = run(THERMAL_DIR, g, synth="thermal")
    assert res["basis"] == "recorded-timing-synthetic-values"
    assert res["steps_run"] == 3000 and res["core_counters"].get("required_role_absent", 0) == 0


def test_the_thermal_synth_crosses_the_delta_threshold_both_ways_and_fires(monkeypatch):
    sys.path.insert(0, str(ROOT / "tests" / "oracle"))
    from thermal_v0 import synth
    decl = declaration.load(THERMAL_DIR / "unit.yaml")
    from unit_helpers import make_thermal
    unit, _ = make_thermal({})
    fill = synth.make(1)
    state, up, down, fired, above = None, 0, 0, 0, None
    monkeypatch.setenv("OPENDDIL_EDGE_ID", "edge-tst")
    for i in range(6000):
        evt = pb.EntityTelemetryEvent()
        evt.asset.asset_id = "a"
        fill(evt)
        th = evt.sustainment.thermal
        assert th.component_temperature.unit == th.ambient_temperature.unit == "K"
        now = th.component_temperature.value - th.ambient_temperature.value > synth.DELTA_THRESHOLD_K
        if above is not None and now != above:
            up, down = up + now, down + (not now)
        above = now
        state, sends, _c = core.process_one(unit, decl, evt.SerializeToString(), state, i)
        fired += bool(sends)
    assert up >= 3 and down >= 3 and fired >= 1


def test_the_thermal_synth_is_seeded():
    sys.path.insert(0, str(ROOT / "tests" / "oracle"))
    from thermal_v0 import synth

    def vals(seed):
        f, out = synth.make(seed), []
        for _ in range(50):
            e = pb.EntityTelemetryEvent()
            e.asset.asset_id = "x"
            f(e)
            out.append(e.sustainment.thermal.component_temperature.value)
        return out
    assert vals(2) == vals(2) and vals(2) != vals(3)


def test_the_cli_writes_the_result(tmp_path, capsys):
    out = tmp_path / "m.json"
    rc = replay.main(["--measure", "--unit-dir", str(WEAR_DIR), "--raw", raw_file(tmp_path), "--out", str(out)])
    assert rc == 0
    assert set(json.loads(out.read_text(encoding="utf-8"))) == KEYS
    assert "MEASURE wear" in capsys.readouterr().out


def test_the_cli_refuses_synth_without_measure(tmp_path, capsys):
    rc = replay.main(["--unit-dir", str(WEAR_DIR), "--edge-id", "e", "--region-id", "r", "--synth", "trends",
                      "--raw", raw_file(tmp_path), "--out", str(tmp_path / "x.json")])
    assert rc == 2 and "--synth needs --measure" in capsys.readouterr().err
