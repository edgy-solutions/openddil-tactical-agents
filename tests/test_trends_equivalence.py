"""The trends unit against the frozen old path (tests/oracle/trends_v0), through
the replay harness's --compare-oracle mode: same raw records in the same order
from a cold start, the same record time as the clock, outputs compared exactly
per asset (the wear_trends order aside, which both sides are normalised to)."""
from __future__ import annotations

import importlib.util
import json
import random
import sys
from collections import Counter

import pytest
from openddil.logistics.v1 import windowed_telemetry_pb2 as winpb
from openddil.telemetry.v1 import telemetry_pb2 as pb

from algorithm_runtime import declaration, guard, replay
from unit_helpers import ROOT

TRENDS_DIR = ROOT / "algorithms" / "trends"
ENV = {"OPENDDIL_EDGE_ID": "edge-tst", "OPENDDIL_REGION_ID": "region-tst"}


def make_trends(overrides=None):
    decl = declaration.load(TRENDS_DIR / "unit.yaml")
    guard.check_unit_dir(TRENDS_DIR)
    name = "openddil_algorithms.trends"
    spec = importlib.util.spec_from_file_location(name, TRENDS_DIR / "trends.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    params = declaration.resolve_params(decl, overrides)
    return module.Trends(params), decl, params


def records(seed, n_assets=3, per_asset=2000, step_ms=(0, 1, 3_000), backwards_at=None):
    """[(offset, ts_ms, key, value)] round-robin over assets; some record times
    repeat (step 0) and one goes back by a record. Values come from synth."""
    from trends_v0 import synth
    fill = synth.make(seed)
    rng = random.Random(seed ^ 0xBEEF)
    out, t, off = [], 1_700_000_000_000, 0
    for i in range(n_assets * per_asset):
        t += rng.choice(step_ms) if rng.random() < 0.9 else 20_000
        if backwards_at is not None and i == backwards_at:
            t -= 40_000
        e = pb.EntityTelemetryEvent()
        e.asset.asset_id = f"asset-{i % n_assets}"
        fill(e)
        out.append((off, t, b"k", e.SerializeToString()))
        off += 1
    return out


def run(recs, overrides=None):
    unit, decl, params = make_trends(overrides)
    return replay.compare_oracle(unit, decl, params, recs, ENV, "trends")


def assert_exact(r, min_emitted=20):
    assert r.different == 0 and r.old_only == 0 and r.new_only == 0 and r.stamp_failures == 0, r.to_dict()
    assert r.emitted_old == r.emitted_new and r.emitted_new.get("window", 0) >= min_emitted
    assert r.compared == r.equal == r.emitted_new["window"]


def test_unit_equals_oracle_on_three_assets_two_thousand_records_each():
    r = run(records(11))
    assert_exact(r, 1000)
    assert r.assets == 3


def test_equal_record_times_and_a_time_that_goes_backwards_by_one_record():
    r = run(records(5, per_asset=800, step_ms=(0, 0, 1), backwards_at=900))
    assert_exact(r, 300)


def test_the_equivalence_exercises_every_branch():
    """Guard against a degenerate stream: the oracle's outputs carry fluid, ammo,
    wear (hours-only, RUL-only and both) and fault codes."""
    from trends_v0 import Oracle
    unit, decl, params = make_trends()
    o = Oracle(params, "edge-tst", "region-tst")
    seen = Counter()
    for _off, ts, _k, v in records(3, per_asset=600):
        for _kind, w in o.feed(v, ts * 1_000_000):
            seen["window"] += 1
            seen["fluid"] += bool(w.fluid_trends)
            seen["ammo"] += bool(w.consumable_trends)
            seen["wear"] += bool(w.wear_trends)
            seen["wear_h_only"] += any(t.HasField("hours_in_service") and not t.HasField("remaining_useful_life")
                                       for t in w.wear_trends)
            seen["wear_r_only"] += any(t.HasField("remaining_useful_life") and not t.HasField("hours_in_service")
                                       for t in w.wear_trends)
            seen["faults"] += bool(w.active_fault_codes_latest)
            seen["ammo_cap"] += any(t.capacity > 0 for t in w.consumable_trends)
    assert all(seen[k] > 0 for k in ("window", "fluid", "ammo", "wear", "wear_h_only", "faults", "ammo_cap")), seen


def test_params_from_the_yaml_change_the_behaviour_on_both_sides():
    recs = records(7, per_asset=500)
    base = run(recs)
    tight = run(recs, {"fluid_window_min": 1, "wear_window_min": 1, "emit_every_n_samples": 2})
    assert_exact(tight, 500)
    assert tight.emitted_new["window"] > base.emitted_new["window"] * 2


def test_cap_of_two_matches():
    assert_exact(run(records(9, per_asset=400), {"window_cap": 2}), 100)


def test_ammo_only_stream_matches_and_emits_ammo_trends():
    """The old path built a UnitRegistry per push; the unit shares one. Equal
    protos on a stream of nothing but ammunition shows that changes nothing."""
    recs = []
    for i in range(600):
        e = pb.EntityTelemetryEvent()
        e.asset.asset_id = "a"
        it = e.sustainment.consumables.items["main_gun"]
        it.quantity_remaining, it.quantity_capacity, it.nsn = 100 - i // 7, 120, "nsn-1"
        recs.append((i, 1_700_000_000_000 + i * 2_000, b"k", e.SerializeToString()))
    r = run(recs)
    assert_exact(r, 100)


def test_a_stream_with_nothing_to_trend_emits_nothing_on_both_sides():
    recs = []
    for i in range(50):
        e = pb.EntityTelemetryEvent()
        e.asset.asset_id = "a"
        recs.append((i, 1_700_000_000_000 + i * 1_000, b"k", e.SerializeToString()))
    r = run(recs)
    assert r.emitted_old == {} and r.emitted_new == {} and r.different == 0


def test_the_harness_counts_a_difference(monkeypatch):
    """The compare is not vacuous: a unit with a different window disagrees."""
    unit, decl, params = make_trends()
    r = replay.compare_oracle(unit, decl, dict(params, fluid_window_min=1), records(2, per_asset=300), ENV, "trends")
    # the oracle was built from params with a 1 min window, the unit from 15
    assert r.different > 0 and r.diff_by_field


def test_cli_compare_oracle_writes_a_report_and_exits_zero(tmp_path, capsys):
    import base64
    import gzip
    d = tmp_path / "raw-sensor-stream"
    d.mkdir()
    b64 = lambda b: base64.b64encode(b)  # noqa: E731
    recs = records(4, per_asset=300)
    with gzip.open(d / "raw-sensor-stream.0-899.gz", "wb") as fh:
        for off, ts, key, value in recs:
            fh.write(b" ".join([str(off).encode(), str(ts).encode(), b64(key), b64(value)]) + b"\n")
    out = tmp_path / "report.json"
    rc = replay.main(["--unit-dir", str(TRENDS_DIR), "--edge-id", "edge-tst", "--region-id", "region-tst",
                      "--raw", str(d / "*.gz"), "--out", str(out), "--compare-oracle", "trends"])
    assert rc == 0
    rep = json.loads(out.read_text())
    assert rep["mode"] == "compare-oracle" and rep["different"] == 0 and rep["emitted_new"]["window"] > 0
    assert "COMPARE-ORACLE" in capsys.readouterr().out
    rc = replay.main(["--unit-dir", str(TRENDS_DIR), "--edge-id", "edge-tst", "--region-id", "region-tst",
                      "--raw", str(d / "*.gz"), "--out", str(out), "--compare-oracle", "trends", "--synth-seed", "3"])
    assert rc == 0


def test_synth_seed_works_in_a_fresh_interpreter(tmp_path):
    """--synth-seed must not depend on anything having imported the oracle's
    synth module already (a fresh process, as the CLI is run)."""
    import base64
    import gzip
    import os
    import subprocess
    d = tmp_path / "raw"
    d.mkdir()
    with gzip.open(d / "raw-sensor-stream.0-199.gz", "wb") as fh:
        for i in range(200):
            e = pb.EntityTelemetryEvent()
            e.asset.asset_id = "a"
            fh.write(b" ".join([str(i).encode(), str(1_700_000_000_000 + i * 1000).encode(),
                                base64.b64encode(b"k"), base64.b64encode(e.SerializeToString())]) + b"\n")
    env = dict(os.environ, PYTHONPATH=os.pathsep.join([str(ROOT / "edge"), os.environ.get("PYTHONPATH", "")]))
    out = subprocess.run(
        [sys.executable, "-m", "algorithm_runtime.replay", "--unit-dir", str(TRENDS_DIR), "--edge-id", "e",
         "--region-id", "r", "--raw", str(d / "*.gz"), "--out", str(tmp_path / "o.json"),
         "--compare-oracle", "trends", "--synth-seed", "3"],
        capture_output=True, text=True, env=env)
    assert out.returncode == 0, out.stdout[-400:] + out.stderr[-400:]
    assert json.loads((tmp_path / "o.json").read_text())["emitted_new"].get("window", 0) > 0
