"""The replay harness (algorithm_runtime.replay) on synthetic recordings.

Each case builds a recording with the frozen oracle playing production: raw
telemetry, the oracle's derived events, and the changelog in faust JSON, all
in the recorded line format. Synthetic only; sockets are disabled.
"""
from __future__ import annotations

import base64
import gzip
import json
import math
import random
import socket
import types
import tracemalloc
from pathlib import Path

import pytest
from openddil.telemetry.v1 import telemetry_pb2 as pb

from algorithm_runtime import replay, roles
from prognostics_v0 import agent as old
from prognostics_v0.coefficients import WearCoefficients
from test_wear_equivalence import FAKE_NOW_S, OldEngine, json_state
from unit_helpers import WEAR_DIR, make_wear

EDGE, REGION = "edge-tst", "region-tst"
OVERLAP = 40
TRACK_PATH = "sustainment.wear.components[track].hours_in_service.value"


@pytest.fixture(autouse=True)
def pinned(monkeypatch):
    def no_socket(*a, **k):
        raise AssertionError("network access attempted")
    monkeypatch.setattr(socket, "socket", no_socket)
    fake = types.SimpleNamespace(time=lambda: FAKE_NOW_S)
    monkeypatch.setattr(roles, "time", fake)
    monkeypatch.setattr(old, "time", fake)
    monkeypatch.setenv("OPENDDIL_EDGE_ID", EDGE)
    monkeypatch.setenv("OPENDDIL_REGION_ID", REGION)


def b64(b: bytes) -> bytes:
    return base64.b64encode(b)


class Recording:
    """A synthetic recording: the oracle is production. Rows are
    (offset, ts_ms, key, value) lists a case may edit before `write`."""

    def __init__(self, n=300, n_assets=5, snap_at=150, seed=7, valid_k=None, zero_valid=(), lead=OVERLAP):
        self.n, self.snap_at, self.start = n, snap_at, snap_at - lead
        self.ns = "prognostics.agent._AccumulatorRecord"
        rng = random.Random(seed)
        self.assets = [f"asset-{i}" for i in range(n_assets)]
        seq = [rng.choice(self.assets) for _ in range(n)]
        self.seq = seq
        count: dict[str, int] = {}
        kth = []
        for a in seq:
            kth.append(count.get(a, 0))
            count[a] = kth[-1] + 1
        valid_k = valid_k(seq, kth) if callable(valid_k) else (valid_k or {})
        pos = {a: [6_371_000.0 + 100.0 * i, 1_000.0 * i, 500.0 * i] for i, a in enumerate(self.assets)}

        unit, _decl = make_wear({})
        p = unit.params
        oracle = OldEngine(WearCoefficients(
            track_life_total_km=float(p["track_life_total_km"]),
            engine_life_total_hours=float(p["engine_life_total_hours"]),
            barrel_life_total_rounds=int(p["barrel_life_total_rounds"]),
            terrain_total_deg_km=float(p["terrain_total_deg_km"])), {})
        self.raw_rows, self.rec_rows = [], []
        self.early_states = self.snap_states = self.end_states = None
        for i, a in enumerate(seq):
            for k in range(3):
                pos[a][k] += rng.uniform(-300.0, 300.0)
            evt = pb.EntityTelemetryEvent()
            evt.asset.asset_id = a
            evt.asset.callsign = f"cs-{a}"
            evt.asset.platform_type = "tracked"
            evt.kinematics.position.ecef.x.value = pos[a][0]
            evt.kinematics.position.ecef.y.value = pos[a][1]
            evt.kinematics.position.ecef.z.value = pos[a][2]
            evt.kinematics.position.ecef.x.unit = "m"
            if i not in zero_valid:
                t = 1_000_000_000 * (10 + 5 * valid_k.get(i, kth[i])) + self.assets.index(a)
                evt.kinematics.position.valid_at.seconds = t // 1_000_000_000
                evt.kinematics.position.valid_at.nanos = t % 1_000_000_000
            evt.kinematics.attitude.euler.pitch.value = rng.uniform(-20, 20)
            evt.kinematics.attitude.euler.pitch.unit = "deg"
            evt.provenance.sample_time.seconds = 1_800_000_000 + i
            evt.provenance.sample_time.nanos = 1000 + i
            evt.provenance.edge_id, evt.provenance.region_id = "edge-src", "region-src"
            evt.provenance.originator_nation = "AAA"
            evt.provenance.releasable_to.extend(["AAA", "BBB"])
            raw = evt.SerializeToString()
            _asset, _rec, out, _src = oracle.feed(raw)
            if i == snap_at // 2:
                self.early_states = {x: json_state(r) for x, r in oracle.table.items()}
            if i == snap_at - 1:  # the snapshot is the state after record snap_at - 1
                self.snap_states = {x: json_state(r) for x, r in oracle.table.items()}
            if i >= self.start:
                self.raw_rows.append((5000 + i, 1000 + i, a.encode(), raw))
                self.rec_rows.append((7000 + i, 2000 + i, a.encode(), out.SerializeToString()))
        self.end_states = {x: json_state(r) for x, r in oracle.table.items()}

    def _log(self, early, states, base):
        def row(offset, asset, state):
            value = b"" if state is None else json.dumps({**state, "__faust": {"ns": self.ns}}).encode()
            return offset, offset, json.dumps(asset).encode(), value
        rows = [row(base, "ghost", early.get("asset-0"))]       # a key that is later deleted
        rows += [row(base + 1 + i, a, early[a]) for i, a in enumerate(sorted(early))]
        rows.append(row(base + 50, "ghost", None))              # tombstone
        rows += [row(base + 60 + i, a, states[a]) for i, a in enumerate(sorted(states))]
        return rows

    def write(self, tmp: Path, raw_split=None) -> dict[str, str]:
        out = {}
        for name, rows, split in (
            ("raw-sensor-stream", self.raw_rows, raw_split),
            ("derived-sustainment", self.rec_rows, None),
            ("snapshot", self._log(self.early_states, self.snap_states, 100), None),
            ("end", self._log(self.early_states, self.end_states, 100), None),
        ):
            d = tmp / name
            d.mkdir()
            parts = [rows[:split], rows[split:]] if split else [rows]
            for part in parts:
                path = d / f"{name}.{part[0][0]}-{part[-1][0]}.gz"
                with gzip.open(path, "wb") as fh:
                    for off, ts, key, value in part:
                        fh.write(b" ".join([str(off).encode(), str(ts).encode(), b64(key), b64(value)]) + b"\n")
            out[name] = str(d / f"{name}.*.gz")
        return out


def run_api(globs, **kw):
    unit, decl = make_wear({})
    snapshot = replay.load_snapshot(sorted(Path(globs["snapshot"]).parent.glob("*.gz")), decl)
    end = replay.load_snapshot(sorted(Path(globs["end"]).parent.glob("*.gz")), decl)
    raw = replay.read_records(sorted(Path(globs["raw-sensor-stream"]).parent.glob("*.gz")))
    rec = replay.read_records(sorted(Path(globs["derived-sustainment"]).parent.glob("*.gz")))
    kw.setdefault("overlap", OVERLAP)
    return replay.replay(unit, decl, unit.params, raw, snapshot, rec,
                         {"OPENDDIL_EDGE_ID": EDGE, "OPENDDIL_REGION_ID": REGION},
                         end_snapshot=end, **kw)


def run_cli(globs, tmp, *extra):
    out = tmp / "report.json"
    rc = replay.main(["--unit-dir", str(WEAR_DIR), "--params-json", "{}", "--edge-id", EDGE,
                      "--region-id", REGION, "--raw", globs["raw-sensor-stream"],
                      "--recorded", globs["derived-sustainment"], "--snapshot", globs["snapshot"],
                      "--end-snapshot", globs["end"], "--out", str(out), "--overlap", str(OVERLAP), *extra])
    return rc, out


def test_clean_recording_has_no_differences(tmp_path):
    rec = Recording()
    r = run_api(rec.write(tmp_path))
    assert (r.aligned, r.aligned_ambiguous, r.unaligned, r.fresh) == (5, 0, 0, 0)
    assert r.compared > 100 and r.equal == r.compared and r.different == 0
    assert (r.replay_only, r.recorded_only, r.stamp_failures, r.fallback_rows) == (0, 0, 0, 0)
    assert r.end_state == {"equal": 5, "different": 0, "missing": 0, "new": 0, "excluded": 0}
    assert r.topics == {"raw-sensor-stream": rec.n - rec.start, "derived-sustainment": rec.n - rec.start}


def test_callsign_cleared_mid_recording_is_not_a_difference(tmp_path):
    """A recording may clear the callsign from some pull on. Raw and derived are pulled
    separately, so one side can carry it where the other doesn't, in either direction."""
    rec = Recording()

    def cleared(rows, from_k):
        out = []
        for k, (off, ts, key, value) in enumerate(rows):
            if k >= from_k:
                msg = pb.EntityTelemetryEvent.FromString(value)
                msg.asset.ClearField("callsign")
                value = msg.SerializeToString()
            out.append((off, ts, key, value))
        return out

    rec.rec_rows = cleared(rec.rec_rows, 100)
    rec.raw_rows = cleared(rec.raw_rows, 120)
    r = run_api(rec.write(tmp_path))
    assert r.compared > 100 and r.different == 0 and r.unaligned == 0
    assert (r.replay_only, r.recorded_only) == (0, 0)


def test_one_ulp_names_field_path_and_offset(tmp_path):
    rec = Recording()
    k = 110  # well after the alignment point
    off, ts, key, value = rec.rec_rows[k]
    msg = pb.EntityTelemetryEvent.FromString(value)
    v = msg.sustainment.wear.components["track"].hours_in_service
    v.value = math.nextafter(v.value, math.inf)
    rec.rec_rows[k] = (off, ts, key, msg.SerializeToString())
    globs = rec.write(tmp_path)
    rc, out = run_cli(globs, tmp_path, "--oracle")
    assert rc == 1
    rep = json.loads(out.read_text())
    assert rep["different"] == 1 and rep["diff_by_field"] == {TRACK_PATH: 1}
    assert rep["first_diffs"] == [{"raw_offset": 5000 + rec.start + k, "recorded_offset": off,
                                   "paths": [TRACK_PATH], "oracle": "agrees_with_replay"}]
    assert rep["oracle"] == {"agrees_with_replay": 1}


def test_duplicated_recorded_event_is_recorded_only(tmp_path):
    rec = Recording()
    k = 100
    rows = rec.rec_rows[: k + 1] + [rec.rec_rows[k]] + rec.rec_rows[k + 1:]
    rec.rec_rows = [(7000 + i, ts, key, value) for i, (_o, ts, key, value) in enumerate(rows)]
    r = run_api(rec.write(tmp_path))
    assert r.recorded_only == 1 and r.replay_only == 0 and r.different == 0 and r.compared > 100


def test_duplicate_valid_at_at_the_alignment_point_is_ambiguous(tmp_path):
    def last_two(seq, kth):
        """The asset's last two records before the snapshot share a valid_at."""
        a = seq[149]
        p = [i for i in range(150) if seq[i] == a][-2:]
        assert p[0] >= 110
        return {p[0]: kth[p[1]]}
    rec = Recording(valid_k=last_two)
    r = run_api(rec.write(tmp_path))
    assert r.aligned_ambiguous == 1 and r.aligned == 4 and r.unaligned == 0
    assert r.different == 0 and r.compared > 100 and r.end_state["different"] == 0


def test_snapshot_asset_without_a_match_is_unaligned_and_excluded(tmp_path):
    rec = Recording()
    rec.snap_states["asset-0"]["observed_time_last_ns"] += 1
    r = run_api(rec.write(tmp_path))
    assert r.unaligned == 1 and r.aligned == 4
    entry = r.assets["asset-0"]
    assert entry["status"] == "unaligned" and "observed_time_last_ns" in entry["reason"]
    assert entry["compared"] == 0 and r.different == 0 and r.compared > 80
    assert r.end_state["excluded"] == 1 and r.end_state["equal"] == 4


def test_fallback_row_is_counted_and_later_comparisons_excluded(tmp_path):
    z = 200
    rec = Recording(zero_valid={z})
    r = run_api(rec.write(tmp_path))
    asset = rec.seq[z]
    assert r.fallback_rows == 1 and r.fallback_excluded >= 1 and r.different == 0
    assert r.assets[asset]["fallback_excluded"] == r.fallback_excluded
    assert r.end_state["excluded"] == 1 and r.end_state["equal"] == 4


def test_gap_between_files_and_wrong_namespace_exit_2(tmp_path, capsys):
    rec = Recording()
    rec.raw_rows = rec.raw_rows[:50] + rec.raw_rows[60:]  # offsets 5000+... jump by 10
    globs = rec.write(tmp_path, raw_split=50)
    second = f"raw-sensor-stream.{rec.raw_rows[50][0]}-{rec.raw_rows[-1][0]}.gz"
    rc, _ = run_cli(globs, tmp_path)
    assert rc == 2 and second in capsys.readouterr().err

    tmp2 = tmp_path / "ns"
    tmp2.mkdir()
    bad = Recording()
    bad.ns = "prognostics.agent.SomethingElse"
    globs = bad.write(tmp2)
    rc, _ = run_cli(globs, tmp2)
    err = capsys.readouterr().err
    assert rc == 2 and "snapshot." in err and "__faust.ns" in err


def test_memory_stays_bounded_on_200k_records(tmp_path):
    n = 200_000
    rec = Recording(n=n, n_assets=20, snap_at=450, lead=400)
    globs = rec.write(tmp_path)
    del rec
    tracemalloc.start()
    try:
        r = run_api(globs, overlap=400)
        _cur, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    print(f"peak_tracemalloc_mb={peak / 1e6:.1f}")
    assert r.different == 0 and r.compared > 150_000 and r.aligned == 20
    assert peak < 50 * 1024 * 1024
