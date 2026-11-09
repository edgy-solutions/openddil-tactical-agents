"""Offline replay of recorded production input through a declared unit.

The harness feeds recorded raw records to `core.process_one` exactly as the
runner does, then diffs what the unit emits against what production emitted
(the old code). It is generic over units: nothing here knows what wear is,
apart from the optional oracle diagnostic. No faust import, so it runs from a
plain venv.

Record line format (gzip, one record per line), the fields of
`rpk topic consume -f '%o %d %k{base64} %v{base64}\\n'`:

    offset ts_ms key_b64 value_b64

Files are named `<topic>.<first>-<last>.gz`. A changelog snapshot is read as a
whole log: the state is the LAST value per key (JSON-encoded asset id), an
empty value is a tombstone.

Every record is given to the unit with its recording time (`ts_ms * 1_000_000`)
as the `record_time_ns` role, as the runner gives the Faust message time.

`--compare-oracle <name>` is a different mode: no recorded output side at all.
It runs the frozen old path `tests/oracle/<name>_v0` and the unit over the same
raw records, in order, from a cold start, with the same record time, and
compares what each emits per asset and kind, exactly. `--synth-seed N` first
lets the oracle package's `synth` fill values the recording lacks into each
decoded event, so both sides see them.

A unit with no `sustainment` output (an anomaly unit, say) has no recorded
side this harness can align and compare, so it runs in COUNT-ONLY mode: every
raw record goes through `process_one`, and the report carries the counters and
the outputs emitted per kind. It says so in its header; "equal" means nothing
there, because nothing was compared.

Memory stays bounded however long the recording is: raw and recorded records
are streamed, each asset holds small deques, and the recorded reader advances
only as far as the replay needs it. Never hold the week in memory.
"""
from __future__ import annotations

import argparse
import base64
import copy
import glob
import gzip
import json
import logging
import os
import re
import sys
from collections import Counter, deque
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Iterable, Iterator, Mapping

from openddil.telemetry.v1 import telemetry_pb2 as pb

from algorithm_runtime import core, declaration, guard, loader, roles
from algorithm_runtime.contract import Unit
from algorithm_runtime.declaration import Declaration

log = logging.getLogger("algorithm_runtime.replay")

_NAME = re.compile(r"\.(\d+)-(\d+)\.gz$")
_TRIAL_SAMPLES = 50        # samples replayed per candidate when alignment is ambiguous
_LOOKAHEAD = 5000          # recorded records read per demand before the replay waits
_SKIP_PATIENCE = 1000      # replay outputs held while the aligned sample is not yet found
_RESOLVE_CAP = 1000        # recorded records one ambiguous asset may hold while resolving
_MAX_FIRST_DIFFS = 20
_OVERLAP = 2000            # the pulls start this many records before the snapshot's end


class ReplayInputError(ValueError):
    """Bad input: a malformed or inconsistent recording, snapshot or setting."""


# --------------------------------------------------------------------------
# Reading
# --------------------------------------------------------------------------
def _ordered(paths: Iterable) -> list[tuple[int, int, Path]]:
    files = []
    for p in paths:
        p = Path(p)
        m = _NAME.search(p.name)
        if not m:
            raise ReplayInputError(f"{p}: name is not <topic>.<first>-<last>.gz")
        files.append((int(m.group(1)), int(m.group(2)), p))
    files.sort(key=lambda f: (f[0], f[1]))
    for prev, cur in zip(files, files[1:]):
        if cur[0] != prev[1] + 1:
            raise ReplayInputError(
                f"{cur[2]}: starts at offset {cur[0]} but the previous file {prev[2].name} "
                f"ends at {prev[1]} (gap or overlap between files)")
    return files


def _read(paths: Iterable) -> Iterator[tuple[Path, int, int, bytes, bytes]]:
    last = -1
    for _first, _last, path in _ordered(paths):
        with gzip.open(path, "rb") as fh:
            for n, line in enumerate(fh, 1):
                parts = line.rstrip(b"\r\n").split(b" ")
                if len(parts) != 4:
                    raise ReplayInputError(f"{path}: line {n} does not have 4 fields")
                try:
                    offset, ts = int(parts[0]), int(parts[1])
                    key, value = base64.b64decode(parts[2], validate=True), base64.b64decode(parts[3], validate=True)
                except ValueError as exc:
                    raise ReplayInputError(f"{path}: line {n} is malformed: {exc}") from None
                if offset <= last:
                    raise ReplayInputError(f"{path}: line {n} has offset {offset}, not above {last}")
                last = offset
                yield path, offset, ts, key, value


def read_records(paths: Iterable) -> Iterator[tuple[int, int, bytes, bytes]]:
    """(offset, ts_ms, key, value) in offset order across the files."""
    for _path, offset, ts, key, value in _read(paths):
        yield offset, ts, key, value


def load_snapshot(paths: Iterable, decl: Declaration) -> dict[str, dict]:
    """The last value per asset of a changelog, as the state dicts
    `process_one` takes."""
    fields = {f.name: f for f in decl.state_fields}
    out: dict[str, dict] = {}
    for path, _offset, _ts, key, value in _read(paths):
        try:
            asset = json.loads(key)
        except ValueError:
            raise ReplayInputError(f"{path}: offset {_offset}: key is not JSON") from None
        if not isinstance(asset, str):
            raise ReplayInputError(f"{path}: offset {_offset}: key is not a JSON string")
        if not value:
            out.pop(asset, None)
            continue
        try:
            obj = json.loads(value)
        except ValueError:
            raise ReplayInputError(f"{path}: offset {_offset}: value is not JSON") from None
        meta = obj.pop("__faust", None) if isinstance(obj, dict) else None
        ns = meta.get("ns") if isinstance(meta, dict) else None
        if ns != decl.state_record_namespace:
            raise ReplayInputError(
                f"{path}: offset {_offset}: __faust.ns is {ns!r}, expected {decl.state_record_namespace!r}")
        unknown, missing = sorted(set(obj) - set(fields)), sorted(set(fields) - set(obj))
        if unknown or missing:
            raise ReplayInputError(f"{path}: offset {_offset}: unknown fields {unknown}, missing fields {missing}")
        state = {}
        for name, f in fields.items():
            v = obj[name]
            if v is None:
                if not f.nullable:
                    raise ReplayInputError(f"{path}: offset {_offset}: field {name} is null but not nullable")
            elif isinstance(v, bool) or not isinstance(v, (int, float)):
                raise ReplayInputError(f"{path}: offset {_offset}: field {name} is not a number")
            elif f.type == "integer":
                if isinstance(v, float):
                    raise ReplayInputError(f"{path}: offset {_offset}: field {name} is not an integer")
            else:
                v = float(v)
            state[name] = v
        out[asset] = state
    return out


# --------------------------------------------------------------------------
# Comparison
# --------------------------------------------------------------------------
def _norm(msg: pb.EntityTelemetryEvent, src_has_sample_time: bool) -> pb.EntityTelemetryEvent:
    """Copy with what is allowed to differ cleared: ids, wall-clock stamps,
    the new producer_stamp, sample_time only where the source had none, and the
    callsign, a label no unit reads that a recording may clear from some pull on."""
    out = pb.EntityTelemetryEvent()
    out.CopyFrom(msg)
    out.ClearField("event_id")
    out.asset.ClearField("callsign")
    out.provenance.ClearField("ingest_time")
    out.provenance.ClearField("producer_stamp")
    if not src_has_sample_time:
        out.provenance.ClearField("sample_time")
    return out


def _is_repeated(fd) -> bool:
    return fd.is_repeated if hasattr(fd, "is_repeated") else fd.label == fd.LABEL_REPEATED


def _same(a, b) -> bool:
    return a == b or (a != a and b != b)


def _diff_paths(a, b, path: str = "") -> list[str]:
    """Field paths where two messages of one type differ. Floats are exact."""
    out: list[str] = []
    for fd in a.DESCRIPTOR.fields:
        p = f"{path}.{fd.name}" if path else fd.name
        va, vb = getattr(a, fd.name), getattr(b, fd.name)
        is_msg = fd.message_type is not None
        if is_msg and fd.message_type.GetOptions().map_entry:
            vfd = fd.message_type.fields_by_name["value"]
            for k in sorted(set(va) | set(vb), key=str):
                pk = f"{p}[{k}]"
                if k not in va or k not in vb:
                    out.append(pk)
                elif vfd.message_type is not None:
                    out.extend(_diff_paths(va[k], vb[k], pk))
                elif not _same(va[k], vb[k]):
                    out.append(pk)
        elif _is_repeated(fd):
            if len(va) != len(vb):
                out.append(p)
            else:
                for i, (x, y) in enumerate(zip(va, vb)):
                    if is_msg:
                        out.extend(_diff_paths(x, y, f"{p}[{i}]"))
                    elif not _same(x, y):
                        out.append(f"{p}[{i}]")
        elif is_msg:
            ha, hb = a.HasField(fd.name), b.HasField(fd.name)
            if ha != hb:
                out.append(p)
            elif ha:
                out.extend(_diff_paths(va, vb, p))
        else:
            if fd.has_presence and a.HasField(fd.name) != b.HasField(fd.name):
                out.append(p)
            elif not _same(va, vb):
                out.append(p)
    return out or (["<unknown-fields>"] if a != b else [])


# --------------------------------------------------------------------------
# Report
# --------------------------------------------------------------------------
@dataclass
class Report:
    mode: str = "compare"                                  # compare | count-only (no recorded-side compare)
    emitted: dict = field(default_factory=dict)            # count-only: output kind -> messages emitted
    topics: dict = field(default_factory=dict)             # topic -> record count
    snapshot_assets: int = 0
    aligned: int = 0
    aligned_ambiguous: int = 0
    fresh: int = 0                                         # not in the snapshot: started from None
    unaligned: int = 0
    compared: int = 0
    equal: int = 0
    different: int = 0
    diff_by_field: dict = field(default_factory=dict)
    first_diffs: list = field(default_factory=list)        # offsets and field paths, no values
    replay_only: int = 0
    recorded_only: int = 0
    recorded_unparseable: int = 0
    fallback_rows: int = 0
    fallback_excluded: int = 0                             # replay outputs not compared after a fallback
    stamp_failures: int = 0
    end_state: dict = field(default_factory=dict)          # equal, different, missing, new, excluded
    core_counters: dict = field(default_factory=dict)
    oracle: dict | None = None
    assets: dict = field(default_factory=dict)             # per-asset detail; the only place ids appear

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=1, sort_keys=True)


class _Asset:
    __slots__ = ("id", "status", "reason", "snap", "state", "buf", "cands", "closed", "ambiguous",
                 "skip_target", "rep", "rec", "last_rec", "excluded", "n", "orc")

    def __init__(self, asset_id: str, snap: dict | None):
        self.id = asset_id
        self.status = "pending" if snap is not None else "active"   # pending | collect | active | unaligned
        self.reason = ""
        self.snap = snap
        self.state = None
        self.buf = None
        self.cands = None
        self.closed = False
        self.ambiguous = False
        self.skip_target = None
        self.rep: deque = deque()      # (raw offset, message, source had sample_time, oracle message)
        self.rec: deque = deque()      # (recorded offset, message)
        self.last_rec = None
        self.excluded = False
        self.n: Counter = Counter()
        self.orc = None


class _Replay:
    def __init__(self, unit, decl, params, raw_records, snapshot, recorded_outputs, end_snapshot,
                 tier, overlap, oracle):
        if len(decl.outputs) != 1:
            raise ReplayInputError(
                f"{decl.unit} declares {len(decl.outputs)} outputs; the replay compares exactly one topic")
        self.unit, self.decl, self.params = unit, decl, params
        self.raw = iter(raw_records)
        self.rec_iter = iter(recorded_outputs)
        self.rec_done = False
        self.snapshot, self.end_snapshot = snapshot, end_snapshot
        self.overlap = overlap
        self.stamp = (decl.unit, decl.version, declaration.config_hash(decl.unit, decl.version, params), tier)
        self.assets: dict[str, _Asset] = {}
        self.r = Report()
        self.n_raw = 0
        self.n_rec = 0
        self.counters: Counter = Counter()
        self.diff_fields: Counter = Counter()
        self.oracle = _Oracle(params) if oracle else None
        self.oracle_counts: Counter = Counter()

    # -- assets ----------------------------------------------------------
    def _asset(self, asset_id: str) -> _Asset:
        a = self.assets.get(asset_id)
        if a is None:
            a = _Asset(asset_id, self.snapshot.get(asset_id))
            self.assets[asset_id] = a
            if a.status == "pending" and self.n_raw > self.overlap:
                self._unalign(a, self._no_match_reason())
        return a

    def _no_match_reason(self) -> str:
        return (f"no raw record carries the snapshot's observed_time_last_ns within the first "
                f"{self.overlap} raw records (or the snapshot sample took the wall-clock fallback)")

    def _unalign(self, a: _Asset, reason: str) -> None:
        a.status, a.reason = "unaligned", reason
        a.rep.clear()
        a.rec.clear()
        a.buf = a.cands = None

    # -- raw side --------------------------------------------------------
    def run(self) -> Report:
        for offset, ts_ms, _key, value in self.raw:
            self.n_raw += 1
            if self.n_raw == self.overlap + 1:
                self._end_overlap()
            self._on_raw(offset, value, ts_ms)
        self._finish()
        return self.r

    def _end_overlap(self) -> None:
        for a in self.assets.values():
            if a.status == "pending":
                self._unalign(a, self._no_match_reason())

    def _on_raw(self, offset: int, value: bytes, ts_ms: int) -> None:
        evt = pb.EntityTelemetryEvent()
        try:
            evt.ParseFromString(value)
            asset_id = evt.asset.asset_id
        except Exception:  # noqa: BLE001 - counted by process_one
            asset_id = ""
        if not asset_id:
            self.counters.update(core.process_one(self.unit, self.decl, value, None, ts_ms * 1_000_000)[2])
            return
        a = self._asset(asset_id)
        s = evt.provenance.sample_time
        st = (s.seconds, s.nanos) if (s.seconds or s.nanos) else None
        entry = (offset, value, st is not None, st, ts_ms)
        if a.status == "active":
            self._step(a, entry)
        elif a.status in ("pending", "collect"):
            ext = roles.extract(evt)
            ns = None if ext.get("sample_time_fallback") else ext["sample_time_ns"]
            hit = ns is not None and ns == a.snap["observed_time_last_ns"]
            if a.status == "pending":
                if hit:
                    a.status, a.buf, a.cands, a.closed = "collect", [entry], [0], False
                return
            a.buf.append(entry)
            idx = len(a.buf) - 1
            if not a.closed and hit and a.cands[-1] == idx - 1:
                a.cands.append(idx)
            else:
                a.closed = True
            if a.closed and (len(a.cands) == 1 or idx - a.cands[-1] >= _TRIAL_SAMPLES):
                self._resolve(a)

    # -- alignment -------------------------------------------------------
    def _resolve(self, a: _Asset) -> None:
        if len(a.cands) == 1:
            self._activate(a, 0, False)
            return
        ci = self._trial(a)
        if ci is None:
            self._unalign(a, "ambiguous alignment: no candidate reproduced the next samples")
        else:
            self._activate(a, ci, True)

    def _trial(self, a: _Asset) -> int | None:
        """Replay the next samples from each candidate; the first whose outputs
        all match the recording wins."""
        plans = []
        for ci in a.cands:
            st = a.buf[ci][3]
            if st is None:
                continue
            state, outs = copy.deepcopy(a.snap), []
            for entry in a.buf[ci + 1: ci + 1 + _TRIAL_SAMPLES]:
                new, sends, counters = core.process_one(self.unit, self.decl, entry[1], state,
                                                        entry[4] * 1_000_000)
                if counters.get("sample_time_fallback"):
                    outs = None
                    break
                state = new if new is not None else state
                for _topic, _key, msg in sends:
                    outs.append(_norm(msg, entry[2]))
            if outs is not None:
                plans.append((ci, st, outs))
        while plans:
            verdict = None
            pending = False
            for ci, st, outs in plans:
                j = next((i for i, (_o, m) in enumerate(a.rec)
                          if (m.provenance.sample_time.seconds, m.provenance.sample_time.nanos) == st), None)
                if j is None or len(a.rec) - (j + 1) < len(outs):
                    pending = True
                    continue
                recs = list(a.rec)[j + 1: j + 1 + len(outs)]
                if all(_norm(m, True) == o or _norm(m, False) == o for (_o, m), o in zip(recs, outs)):
                    verdict = ci
                    break
            if verdict is not None:
                return verdict
            if not pending or self.rec_done or len(a.rec) > _RESOLVE_CAP or not self._read_one():
                return None
        return None

    def _activate(self, a: _Asset, ci: int, ambiguous: bool) -> None:
        st = a.buf[ci][3]
        if st is None:
            self._unalign(a, "the matched raw record carries no provenance.sample_time")
            return
        a.state, a.status, a.ambiguous, a.skip_target = dict(a.snap), "active", ambiguous, st
        buf, a.buf, a.cands = a.buf, None, None
        if self.oracle:
            a.orc = self.oracle.start(a.state)
        self._skip(a)
        for entry in buf[ci + 1:]:
            self._step(a, entry)

    def _skip(self, a: _Asset) -> None:
        while a.skip_target is not None and a.rec:
            _o, m = a.rec.popleft()
            self._skip_one(a, m)

    def _skip_one(self, a: _Asset, m) -> None:
        s = m.provenance.sample_time
        if (s.seconds, s.nanos) == a.skip_target:
            a.skip_target = None
            a.last_rec = _norm(m, True)

    # -- the step --------------------------------------------------------
    def _step(self, a: _Asset, entry) -> None:
        offset, value, has_st, _st, ts_ms = entry
        new_state, sends, counters = core.process_one(self.unit, self.decl, value, a.state,
                                                      ts_ms * 1_000_000)
        self.counters.update(counters)
        if counters.get("sample_time_fallback"):
            self.r.fallback_rows += 1
            a.excluded = True
        if new_state is not None:
            a.state = new_state
        orc = self.oracle.feed(a.orc, value) if self.oracle and a.orc is not None else None
        for _topic, _key, msg in sends:
            ps = msg.provenance.producer_stamp
            if (ps.unit, ps.version, ps.config_hash, ps.tier) != self.stamp:
                self.r.stamp_failures += 1
            if a.excluded:
                self.r.fallback_excluded += 1
                a.n["fallback_excluded"] += 1
                continue
            a.rep.append((offset, msg, has_st, orc))
        if a.rep:
            self._fill(a)
            self._compare(a)
            if a.skip_target is not None and len(a.rep) > _SKIP_PATIENCE:
                self._unalign(a, "the recorded output for the aligned sample was not found")

    # -- recorded side ---------------------------------------------------
    def _read_one(self) -> bool:
        try:
            offset, _ts, key, value = next(self.rec_iter)
        except StopIteration:
            self.rec_done = True
            return False
        self.n_rec += 1
        msg = pb.EntityTelemetryEvent()
        try:
            msg.ParseFromString(value)
        except Exception:  # noqa: BLE001
            self.r.recorded_unparseable += 1
            return True
        a = self._asset(key.decode("utf-8", "replace"))
        if a.status == "unaligned" or a.excluded:
            return True
        if a.status == "active" and a.skip_target is not None:
            self._skip_one(a, msg)
            return True
        a.rec.append((offset, msg))
        if a.status == "active":
            self._compare(a)
        return True

    def _fill(self, a: _Asset) -> None:
        reads = 0
        while not a.rec and not self.rec_done and reads < _LOOKAHEAD and a.status == "active":
            self._read_one()
            reads += 1

    def _compare(self, a: _Asset) -> None:
        while a.rep and a.rec:
            off_r, msg_r, has_st, orc = a.rep[0]
            off_c, msg_c = a.rec[0]
            nr, nc = _norm(msg_r, has_st), _norm(msg_c, has_st)
            if nr == nc:
                a.rep.popleft()
                a.rec.popleft()
                a.n["compared"] += 1
                a.n["equal"] += 1
                a.last_rec = _norm(msg_c, True)
                continue
            if a.last_rec is not None and _norm(msg_c, True) == a.last_rec:
                a.rec.popleft()            # a restart replayed this output: recorded-only
                a.n["recorded_only"] += 1
                continue
            a.rep.popleft()
            a.rec.popleft()
            a.n["compared"] += 1
            a.n["different"] += 1
            a.last_rec = _norm(msg_c, True)
            paths = _diff_paths(nr, nc)
            self.diff_fields.update(paths)
            if len(self.r.first_diffs) < _MAX_FIRST_DIFFS:
                d = {"raw_offset": off_r, "recorded_offset": off_c, "paths": paths}
                if orc is not None:
                    no = _norm(orc, has_st)
                    d["oracle"] = ("agrees_with_recording" if no == nc
                                   else "agrees_with_replay" if no == nr else "agrees_with_neither")
                self.r.first_diffs.append(d)
            if orc is not None:
                no = _norm(orc, has_st)
                self.oracle_counts["agrees_with_recording" if no == nc
                                   else "agrees_with_replay" if no == nr else "agrees_with_neither"] += 1

    # -- the end ---------------------------------------------------------
    def _finish(self) -> None:
        for a in list(self.assets.values()):
            if a.status == "collect":
                self._resolve(a)
        for a in self.assets.values():
            if a.status == "pending":
                self._unalign(a, self._no_match_reason())
        while self._read_one():
            pass
        r = self.r
        for a in self.assets.values():
            if a.status == "active" and a.skip_target is not None:
                self._unalign(a, "the recorded output for the aligned sample was not found")
            if a.status == "active":
                a.n["replay_only"] += len(a.rep)
                a.n["recorded_only"] += len(a.rec)
                if a.snap is None:
                    r.fresh += 1
                elif a.ambiguous:
                    r.aligned_ambiguous += 1
                else:
                    r.aligned += 1
                for k in ("compared", "equal", "different", "replay_only", "recorded_only"):
                    setattr(r, k, getattr(r, k) + a.n[k])
            elif a.status == "unaligned":
                r.unaligned += 1
            r.assets[a.id] = {
                "status": a.status if a.status != "active" else
                ("fresh" if a.snap is None else "aligned_ambiguous" if a.ambiguous else "aligned"),
                "reason": a.reason or ("fallback timestamp: comparisons excluded" if a.excluded else ""),
                **{k: a.n[k] for k in ("compared", "equal", "different", "replay_only",
                                       "recorded_only", "fallback_excluded")},
            }
        r.topics = {self.decl.input_topic: self.n_raw, self.decl.outputs[0].topic: self.n_rec}
        r.snapshot_assets = len(self.snapshot)
        r.diff_by_field = dict(sorted(self.diff_fields.items()))
        r.core_counters = dict(self.counters)
        if self.oracle:
            r.oracle = dict(self.oracle_counts)
        if self.end_snapshot is not None:
            self._end_state()

    def _end_state(self) -> None:
        eq = diff = missing = new = excluded = 0
        seen = set()
        for asset_id, a in self.assets.items():
            seen.add(asset_id)
            if a.status != "active" or a.excluded:
                excluded += 1
                continue
            final = a.state if a.state is not None else a.snap
            if asset_id not in self.end_snapshot:
                new += 1
            elif final == self.end_snapshot[asset_id]:
                eq += 1
            else:
                diff += 1
        for asset_id, end in self.end_snapshot.items():
            if asset_id in seen:
                continue
            if asset_id in self.snapshot:        # never seen in the pull: unchanged
                eq, diff = (eq + 1, diff) if self.snapshot[asset_id] == end else (eq, diff + 1)
            else:
                missing += 1
        self.r.end_state = {"equal": eq, "different": diff, "missing": missing, "new": new,
                            "excluded": excluded}


class _Oracle:
    """The frozen engine, driven as its agent drove it, over the same aligned
    stream. Diagnostic only; imported lazily because it imports faust."""

    def __init__(self, params: Mapping):
        root = Path(__file__).resolve().parents[2] / "tests" / "oracle"
        if str(root) not in sys.path:
            sys.path.insert(0, str(root))
        try:
            from prognostics_v0 import agent as old
            from prognostics_v0.accumulators import observe_kinematic
            from prognostics_v0.coefficients import WearCoefficients
            from prognostics_v0.models import derive_sustainment
        except ImportError as exc:
            raise ReplayInputError(f"--oracle needs tests/oracle and faust importable: {exc}") from None
        try:
            self.coeffs = WearCoefficients(
                track_life_total_km=float(params["track_life_total_km"]),
                engine_life_total_hours=float(params["engine_life_total_hours"]),
                barrel_life_total_rounds=int(params["barrel_life_total_rounds"]),
                terrain_total_deg_km=float(params["terrain_total_deg_km"]),
            )
        except KeyError as exc:
            raise ReplayInputError(f"--oracle is the wear engine; the unit has no param {exc}") from None
        self.old, self.observe, self.derive = old, observe_kinematic, derive_sustainment

    def start(self, state: dict | None):
        return [self.old._AccumulatorRecord(**(state or {}))]

    def feed(self, holder, raw: bytes):
        """`holder` is a one-item list holding the asset's record, replaced in
        place; returns the oracle's output (or None)."""
        old = self.old
        evt = pb.EntityTelemetryEvent()
        try:
            evt.ParseFromString(raw)
        except Exception:  # noqa: BLE001
            return None
        kin = old._extract_kinematic(evt)
        if kin is None:
            return None
        ecef_m, pitch_deg, roll_deg, sample_time_ns = kin
        state = old._record_to_state(holder[0])
        self.observe(state, ecef_m=ecef_m, pitch_deg=pitch_deg, roll_deg=roll_deg,
                     sample_time_ns=sample_time_ns)
        holder[0] = old._state_to_record(state)
        return old._build_derived_event(evt, self.derive(state, self.coeffs))


# --------------------------------------------------------------------------
# API
# --------------------------------------------------------------------------
def _count_only(unit, decl, raw_records) -> Report:
    """Feed every raw record through `process_one`, a fresh state per asset,
    and report counters and emitted outputs per kind. No recorded side."""
    r = Report(mode="count-only")
    states: dict[str, dict] = {}
    counters: Counter = Counter()
    emitted: Counter = Counter()
    n = 0
    kinds = {o.topic: o.kind for o in decl.outputs}
    for _offset, ts_ms, _key, value in raw_records:
        n += 1
        try:
            evt = pb.EntityTelemetryEvent()
            evt.ParseFromString(value)
            asset_id = evt.asset.asset_id
        except Exception:  # noqa: BLE001 - counted by process_one
            asset_id = ""
        new_state, sends, c = core.process_one(unit, decl, value, states.get(asset_id) if asset_id else None,
                                               ts_ms * 1_000_000)
        counters.update(c)
        if new_state is not None:
            states[asset_id] = new_state
        for topic, _key, _msg in sends:
            emitted[kinds[topic]] += 1
    r.topics = {decl.input_topic: n}
    r.core_counters = dict(counters)
    r.emitted = dict(emitted)
    r.end_state = {"assets": len(states)}
    return r


def replay(unit, decl, params, raw_records, snapshot, recorded_outputs, env,
           end_snapshot=None, overlap: int = _OVERLAP, oracle: bool = False) -> Report:
    """Replay `raw_records` through `unit` from `snapshot` and compare with
    `recorded_outputs` (both iterators of (offset, ts_ms, key, value)). A unit
    with no sustainment output is replayed count-only instead (see the module
    docstring); `snapshot` and `recorded_outputs` are then unused.
    `env` supplies OPENDDIL_EDGE_ID (the stamped tier) and OPENDDIL_REGION_ID."""
    tier = env.get("OPENDDIL_EDGE_ID", "")
    if not tier:
        raise ReplayInputError("env has no OPENDDIL_EDGE_ID: every output is stamped with its tier")
    saved = {k: os.environ.get(k) for k in ("OPENDDIL_EDGE_ID", "OPENDDIL_REGION_ID")}
    try:
        for k in saved:
            if env.get(k):
                os.environ[k] = env[k]
            else:
                os.environ.pop(k, None)
        if not any(o.kind == "sustainment" for o in decl.outputs):
            return _count_only(unit, decl, raw_records)
        return _Replay(unit, decl, params, raw_records, snapshot, recorded_outputs, end_snapshot,
                       tier, overlap, oracle).run()
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


@dataclass
class OracleReport:
    """The `--compare-oracle` result. Counts and field paths only; asset ids
    appear nowhere but `assets` (a count)."""
    mode: str = "compare-oracle"
    oracle: str = ""
    topics: dict = field(default_factory=dict)
    assets: int = 0
    emitted_old: dict = field(default_factory=dict)        # kind -> count
    emitted_new: dict = field(default_factory=dict)
    compared: int = 0
    equal: int = 0
    different: int = 0
    old_only: int = 0
    new_only: int = 0
    first_diffs: list = field(default_factory=list)
    diff_by_field: dict = field(default_factory=dict)
    stamp_failures: int = 0
    unstamped_kinds: list = field(default_factory=list)   # stamp_failures stay 0 for these
    core_counters: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=1, sort_keys=True)


def _norm_out(msg):
    """A copy with what may differ cleared: the host wall clock and the new
    stamp (the stamp is asserted on its own)."""
    if isinstance(msg, dict):
        out = copy.deepcopy(msg)
        for k in ("id", "time"):
            out.pop(k, None)
        (out.get("data") or {}).pop("producer_stamp", None)
        return out
    out = type(msg)()
    out.CopyFrom(msg)
    if isinstance(msg, pb.EntityTelemetryEvent):
        return _norm(out, True)
    out.ClearField("computed_at")
    out.provenance.ClearField("ingest_time")
    out.provenance.ClearField("producer_stamp")
    return out


def _stamp_of(msg):
    if isinstance(msg, dict):
        ps = (msg.get("data") or {}).get("producer_stamp") or {}
        return (ps.get("unit"), ps.get("version"), ps.get("config_hash"), ps.get("tier"))
    ps = msg.provenance.producer_stamp
    return (ps.unit, ps.version, ps.config_hash, ps.tier)


def _wire(msg):
    """An unstamped kind as it goes on the wire: bytes as they are, a CloudEvent
    dict through one JSON round trip (the encoding the send does)."""
    return json.loads(json.dumps(msg)) if isinstance(msg, dict) else msg


def _diff_any(a, b) -> list[str]:
    if isinstance(a, (bytes, bytearray)) or isinstance(b, (bytes, bytearray)):
        # An unstamped kind's bytes: name the proto paths when both parse as
        # telemetry, else just say bytes (never the content).
        try:
            pa, pb_ = pb.EntityTelemetryEvent(), pb.EntityTelemetryEvent()
            pa.ParseFromString(a)
            pb_.ParseFromString(b)
            return _diff_paths(pa, pb_) or ["<bytes>"]
        except Exception:  # noqa: BLE001
            return ["<bytes>"]
    if isinstance(a, dict):
        return sorted(k for k in set(a) | set(b) if a.get(k) != b.get(k)) or ["<dict>"]
    return _diff_paths(a, b)


def _load_oracle(name: str):
    root = Path(__file__).resolve().parents[2] / "tests" / "oracle"
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
    import importlib
    try:
        return importlib.import_module(f"{name}_v0")
    except ImportError as exc:
        raise ReplayInputError(f"--compare-oracle {name}: tests/oracle/{name}_v0 will not import: {exc}") from None


def compare_oracle(unit, decl, params, raw_records, env, name: str, synth_seed: int | None = None,
                   timing: dict | None = None) -> OracleReport:
    """Run the frozen oracle `<name>_v0` and `unit` over the same raw records.

    The oracle package offers `Oracle(params, edge_id, region_id)` with
    `feed(raw, record_time_ns) -> [(kind, message)]`, optionally `normalize(kind,
    message)` (applied to both sides) and `synth.make(seed)`. Both sides start
    cold and see every record in order with the same record time; the unit's
    state is kept per asset between records as the runner keeps it. Outputs
    are paired in order per (asset, kind). `timing`, if given, receives the
    seconds spent in each side."""
    import time as _time
    tier = env.get("OPENDDIL_EDGE_ID", "")
    if not tier:
        raise ReplayInputError("env has no OPENDDIL_EDGE_ID: every output is stamped with its tier")
    pkg = _load_oracle(name)
    saved = {k: os.environ.get(k) for k in ("OPENDDIL_EDGE_ID", "OPENDDIL_REGION_ID")}
    spent = {"old": 0.0, "new": 0.0}
    try:
        for k in saved:
            if env.get(k):
                os.environ[k] = env[k]
            else:
                os.environ.pop(k, None)
        oracle = pkg.Oracle(params, tier, env.get("OPENDDIL_REGION_ID", ""))
        normalize = getattr(pkg, "normalize", lambda _kind, m: m)
        fill = None
        if synth_seed is not None:
            import importlib
            try:
                fill = importlib.import_module(f"{name}_v0.synth").make(synth_seed)
            except (ImportError, AttributeError):
                raise ReplayInputError(f"--synth-seed: {name}_v0 has no synth.make(seed)") from None
        kinds = {o.topic: o.kind for o in decl.outputs}
        unstamped = {o.kind for o in decl.outputs if o.stamp == "none"}
        stamp = (decl.unit, decl.version, declaration.config_hash(decl.unit, decl.version, params), tier)
        r = OracleReport(oracle=f"{name}_v0")
        states: dict[str, dict] = {}
        pending: dict[tuple, tuple[deque, deque]] = {}
        counters: Counter = Counter()
        emitted_old: Counter = Counter()
        emitted_new: Counter = Counter()
        diff_fields: Counter = Counter()
        seen_assets: set = set()
        n = 0

        def compare(key, record_no):
            old_q, new_q = pending[key]
            while old_q and new_q:
                (o_no, o_msg), (n_no, n_msg) = old_q.popleft(), new_q.popleft()
                r.compared += 1
                if key[1] in unstamped:
                    # Compared exactly: raw bytes, or the parsed JSON of a
                    # transition. No clearing of id or time, no normalizing.
                    a, b = _wire(o_msg), _wire(n_msg)
                else:
                    a, b = normalize(key[1], _norm_out(o_msg)), normalize(key[1], _norm_out(n_msg))
                if a == b:
                    r.equal += 1
                    continue
                r.different += 1
                paths = _diff_any(a, b)
                diff_fields.update(re.sub(r"\[\d+\]", "[]", p) for p in paths)
                if len(r.first_diffs) < _MAX_FIRST_DIFFS:
                    r.first_diffs.append({"kind": key[1], "old_record": o_no, "new_record": n_no, "paths": paths})

        for _offset, ts_ms, _key, value in raw_records:
            n += 1
            asset_id = ""
            if fill is not None:
                evt = pb.EntityTelemetryEvent()
                try:
                    evt.ParseFromString(value)
                except Exception:  # noqa: BLE001 - both sides see the bytes as they are
                    evt = None
                if evt is not None:
                    fill(evt)
                    value = evt.SerializeToString()
            rt_ns = ts_ms * 1_000_000
            try:
                probe = pb.EntityTelemetryEvent()
                probe.ParseFromString(value)
                asset_id = probe.asset.asset_id
            except Exception:  # noqa: BLE001 - counted by process_one
                asset_id = ""
            t0 = _time.perf_counter()
            old_out = oracle.feed(value, rt_ns)
            t1 = _time.perf_counter()
            new_state, sends, c = core.process_one(unit, decl, value, states.get(asset_id) if asset_id else None, rt_ns)
            t2 = _time.perf_counter()
            spent["old"] += t1 - t0
            spent["new"] += t2 - t1
            counters.update(c)
            if new_state is not None:
                states[asset_id] = new_state
            if asset_id:
                seen_assets.add(asset_id)
            touched = set()
            for kind, msg in old_out:
                emitted_old[kind] += 1
                pending.setdefault((asset_id, kind), (deque(), deque()))[0].append((n, msg))
                touched.add((asset_id, kind))
            for topic, _k, msg in sends:
                kind = kinds[topic]
                emitted_new[kind] += 1
                if kind not in unstamped and _stamp_of(msg) != stamp:
                    r.stamp_failures += 1
                pending.setdefault((asset_id, kind), (deque(), deque()))[1].append((n, msg))
                touched.add((asset_id, kind))
            for key in touched:
                compare(key, n)
        for old_q, new_q in pending.values():
            r.old_only += len(old_q)
            r.new_only += len(new_q)
        r.topics = {decl.input_topic: n}
        r.assets = len(seen_assets)
        r.unstamped_kinds = sorted(unstamped)
        r.emitted_old, r.emitted_new = dict(emitted_old), dict(emitted_new)
        r.diff_by_field = dict(diff_fields)
        r.core_counters = dict(counters)
        if timing is not None:
            timing.update(spent)
        return r
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def load_unit(unit_dir: Path, decl: Declaration, params: Mapping) -> Unit:
    """The unit imported from file under the guard, by the runner's loader."""
    return loader.load_unit_class(unit_dir, decl, ReplayInputError)(params)


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------
def _expand(pattern: str, what: str) -> list[str]:
    files = sorted(glob.glob(pattern))
    if not files:
        raise ReplayInputError(f"--{what} {pattern!r} matches no file")
    return files


def _oracle_summary(r: OracleReport, out: str, timing: dict) -> list[str]:
    fmt = lambda d: ", ".join(f"{k}={n}" for k, n in sorted(d.items())) or "none"  # noqa: E731
    lines = [
        f"mode: COMPARE-ORACLE against {r.oracle}: both sides over the same raw records, from a cold start",
        f"records: {fmt(r.topics)}   assets: {r.assets}",
        f"emitted_old: {fmt(r.emitted_old)}   emitted_new: {fmt(r.emitted_new)}",
        f"compared={r.compared} equal={r.equal} different={r.different} "
        f"old_only={r.old_only} new_only={r.new_only}",
        f"stamp_failures={r.stamp_failures}"
        + (f" (stay 0: {', '.join(r.unstamped_kinds)} not stamped)" if r.unstamped_kinds else ""),
        "counters: " + (" ".join(f"{k}={n}" for k, n in sorted(r.core_counters.items())) or "none"),
    ]
    if r.diff_by_field:
        lines.append("different by field: " + ", ".join(f"{k}={v}" for k, v in list(r.diff_by_field.items())[:6]))
    if timing:
        lines.append(f"wall seconds in each side: old={timing['old']:.2f} new={timing['new']:.2f}")
    lines.append(f"report: {out}")
    return lines


def _summary(r: Report, out: str) -> list[str]:
    if r.mode == "count-only":
        return [
            "mode: COUNT-ONLY, no recorded-side compare: every raw record was fed through the unit; "
            "nothing was compared with what production emitted",
            f"records: {', '.join(f'{t}={n}' for t, n in r.topics.items())}",
            "emitted: " + (", ".join(f"{k}={n}" for k, n in sorted(r.emitted.items())) or "none"),
            "counters: " + (" ".join(f"{k}={n}" for k, n in sorted(r.core_counters.items())) or "none"),
            f"assets seen: {(r.end_state or {}).get('assets', 0)}",
            f"report: {out}",
        ]
    es = r.end_state or {}
    lines = [
        f"records: {', '.join(f'{t}={n}' for t, n in r.topics.items())}",
        f"assets: aligned={r.aligned} ambiguous={r.aligned_ambiguous} fresh={r.fresh} unaligned={r.unaligned}",
        f"compared={r.compared} equal={r.equal} different={r.different}",
        f"replay_only={r.replay_only} recorded_only={r.recorded_only}",
        f"fallback_rows={r.fallback_rows} fallback_excluded={r.fallback_excluded}",
        f"stamp_failures={r.stamp_failures}",
    ]
    if r.diff_by_field:
        lines.append("different by field: " + ", ".join(f"{k}={v}" for k, v in list(r.diff_by_field.items())[:4]))
    if es:
        lines.append("end state: " + " ".join(f"{k}={v}" for k, v in es.items()))
    if r.oracle is not None:
        lines.append("oracle: " + " ".join(f"{k}={v}" for k, v in r.oracle.items()))
    lines.append(f"report: {out}")
    return lines


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="algorithm_runtime.replay", description=__doc__.split("\n")[0])
    ap.add_argument("--unit-dir", required=True)
    ap.add_argument("--params-json", default="{}", help="the unit's parameter overrides, a JSON object")
    ap.add_argument("--edge-id", required=True)
    ap.add_argument("--region-id", required=True)
    ap.add_argument("--raw", required=True, help="glob of recorded raw input files")
    ap.add_argument("--recorded", help="glob of recorded output files (not read in count-only mode)")
    ap.add_argument("--snapshot", help="glob of the changelog snapshot the replay starts from (not read in count-only mode)")
    ap.add_argument("--end-snapshot", help="glob of a later changelog snapshot to compare the final state with")
    ap.add_argument("--out", required=True)
    ap.add_argument("--overlap", type=int, default=_OVERLAP,
                    help="raw records in which a snapshot asset's sample must be found")
    ap.add_argument("--oracle", action="store_true", help="diagnostic: attribute each difference via the frozen engine")
    ap.add_argument("--compare-oracle", metavar="NAME",
                    help="run tests/oracle/NAME_v0 and the unit over the same raw records and compare exactly")
    ap.add_argument("--synth-seed", type=int,
                    help="with --compare-oracle: fill values the recording lacks, from the oracle package's synth")
    args = ap.parse_args(argv)
    try:
        unit_dir = Path(args.unit_dir)
        decl = declaration.load(unit_dir / "unit.yaml")
        try:
            overrides = json.loads(args.params_json)
        except ValueError as exc:
            raise ReplayInputError(f"--params-json is not JSON: {exc}") from None
        if not isinstance(overrides, dict):
            raise ReplayInputError("--params-json must be a JSON object")
        params = declaration.resolve_params(decl, overrides)
        unit = load_unit(unit_dir, decl, params)
        raw = _expand(args.raw, "raw")
        if args.synth_seed is not None and not args.compare_oracle:
            raise ReplayInputError("--synth-seed needs --compare-oracle")
        if args.compare_oracle:
            timing: dict = {}
            orep = compare_oracle(
                unit, decl, params, read_records(raw),
                {"OPENDDIL_EDGE_ID": args.edge_id, "OPENDDIL_REGION_ID": args.region_id},
                args.compare_oracle, args.synth_seed, timing)
            Path(args.out).write_text(orep.to_json(), encoding="utf-8")
            print("\n".join(_oracle_summary(orep, args.out, timing)))
            bad = orep.different or orep.old_only or orep.new_only or orep.stamp_failures
            return 0 if not bad else 1
        count_only = not any(o.kind == "sustainment" for o in decl.outputs)
        if count_only:
            snapshot, recorded, end = {}, [], None
        else:
            for name in ("recorded", "snapshot"):
                if not getattr(args, name):
                    raise ReplayInputError(f"--{name} is required for {decl.unit}")
            recorded = _expand(args.recorded, "recorded")
            snapshot = load_snapshot(_expand(args.snapshot, "snapshot"), decl)
            end = load_snapshot(_expand(args.end_snapshot, "end-snapshot"), decl) if args.end_snapshot else None
        report = replay(unit, decl, params, read_records(raw), snapshot,
                        iter(()) if count_only else read_records(recorded),
                        {"OPENDDIL_EDGE_ID": args.edge_id, "OPENDDIL_REGION_ID": args.region_id},
                        end_snapshot=end, overlap=args.overlap, oracle=args.oracle)
    except (ReplayInputError, declaration.DeclarationError, guard.GuardError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    Path(args.out).write_text(report.to_json(), encoding="utf-8")
    print("\n".join(_summary(report, args.out)))
    if report.mode == "count-only":
        return 0 if report.topics.get(decl.input_topic) else 1
    bad = report.different or report.stamp_failures or (report.end_state or {}).get("different")
    return 0 if report.compared > 0 and not bad else 1


if __name__ == "__main__":
    raise SystemExit(main())
