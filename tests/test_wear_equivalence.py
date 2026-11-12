"""Old-vs-new: the frozen engine against the wear unit, sample by sample.

The oracle (tests/oracle/prognostics_v0) is the original `edge/prognostics`
code, driven the way its agent drove it (extract, record <-> state, observe,
derive, build the envelope). The new path is `core.process_one` over the wear
unit. Seeded synthetic streams only. The comparison is exact: state JSON
equal, and the emitted event equal after clearing only what is allowed to
differ (ids, wall-clock stamps, the new producer_stamp). Any diff is a
failure; there is no tolerance to tune.
"""
from __future__ import annotations

import json
import random
import socket
import types

import pytest
from openddil.telemetry.v1 import telemetry_pb2 as pb

from algorithm_runtime import core, roles, runner
from prognostics_v0 import agent as old
from prognostics_v0.accumulators import observe_kinematic
from prognostics_v0.coefficients import WearCoefficients
from prognostics_v0.models import derive_sustainment
from unit_helpers import make_wear

FAKE_NOW_S = 1_900_000_000.25

# (name, wear params, max step m, max dt s): the demo values are the compose
# demo's 10 km / 1 h / 50 rounds / 100 deg.km lives.
SCENARIOS = [
    ("defaults", {}, 40_000.0, 40_000.0),
    ("demo", dict(track_life_total_km=10.0, engine_life_total_hours=1.0,
                  barrel_life_total_rounds=50, terrain_total_deg_km=100.0), 2_000.0, 120.0),
    ("zero-life", dict(track_life_total_km=0.0, engine_life_total_hours=0.0,
                       barrel_life_total_rounds=0, terrain_total_deg_km=0.0), 2_000.0, 120.0),
    ("mid", dict(track_life_total_km=300.0, engine_life_total_hours=2.0,
                 barrel_life_total_rounds=20, terrain_total_deg_km=2_000.0), 3_000.0, 300.0),
    ("tight-bands", dict(track_life_total_km=50.0, engine_life_total_hours=0.5,
                         barrel_life_total_rounds=10, terrain_total_deg_km=500.0), 1_500.0, 90.0),
]
ASSETS = [f"asset-{i}" for i in range(8)]
SAMPLES_PER_SCENARIO = 2_400


def make_event(rng, asset, pos, t_ns):
    """One synthetic raw event with the shape variety the stream has: attitude
    absent / rad / deg / empty-unit, no ECEF, valid_at unset, the source's own
    sample_time unset, labels and edge/region present or absent."""
    evt = pb.EntityTelemetryEvent()
    evt.asset.asset_id = asset
    evt.asset.callsign = f"cs-{asset}"
    evt.asset.platform_type = "tracked"
    evt.asset.platform_variant = "v1"
    evt.asset.force = rng.choice([0, 1, 2])
    has_ecef = rng.random() > 0.06
    if has_ecef:
        evt.kinematics.position.ecef.x.value = pos[0]
        evt.kinematics.position.ecef.y.value = pos[1]
        evt.kinematics.position.ecef.z.value = pos[2]
        evt.kinematics.position.ecef.x.unit = "m"
    if rng.random() > 0.08:  # valid_at = 0 otherwise
        evt.kinematics.position.valid_at.seconds = t_ns // 1_000_000_000
        evt.kinematics.position.valid_at.nanos = t_ns % 1_000_000_000
    mode = rng.choice(["none", "rad", "deg", "empty", "pitch-only", "roll-rad-pitch-deg"])
    if mode != "none":
        e = evt.kinematics.attitude.euler
        pitch, roll = rng.uniform(-0.5, 0.5), rng.uniform(-0.4, 0.4)
        if mode == "rad":
            e.pitch.value, e.pitch.unit, e.roll.value, e.roll.unit = pitch, "rad", roll, "rad"
        elif mode == "deg":
            e.pitch.value, e.pitch.unit = pitch * 57.0, "deg"
            e.roll.value, e.roll.unit = roll * 57.0, "deg"
        elif mode == "empty":
            e.pitch.value, e.roll.value = pitch, roll  # unit empty: absent
        elif mode == "pitch-only":
            e.pitch.value, e.pitch.unit = pitch * 57.0, "deg"
        else:
            e.pitch.value, e.pitch.unit = pitch * 57.0, "deg"
            e.roll.value, e.roll.unit = roll, "rad"
    if rng.random() > 0.3:  # sample_time unset on the source otherwise
        evt.provenance.sample_time.seconds = 1_800_000_000 + rng.randrange(10_000)
        evt.provenance.sample_time.nanos = rng.randrange(1_000_000_000)
    if rng.random() > 0.4:
        evt.provenance.edge_id = "edge-src"
        evt.provenance.region_id = "region-src"
    if rng.random() > 0.5:  # labelled source
        evt.provenance.originator_nation = rng.choice(["AAA", "BBB"])
        evt.provenance.releasable_to.extend(rng.sample(["AAA", "BBB", "CCC"], rng.randrange(1, 4)))
    return evt


def stream(seed, n, max_step_m, max_dt_s):
    rng = random.Random(seed)
    pos = {a: [6_371_000.0 + 100.0 * i, 1_000.0 * i, 500.0 * i] for i, a in enumerate(ASSETS)}
    t = {a: rng.randrange(0, 5_000_000_000) for a in ASSETS}
    out = []
    for _ in range(n):
        a = rng.choice(ASSETS)
        if rng.random() < 0.01:
            out.append(("garbage", b"\xff\xff\xff\xff"))
            continue
        for k in range(3):
            pos[a][k] += rng.uniform(-max_step_m, max_step_m)
        # mostly forward in time; now and then an out-of-order sample
        dt = int(rng.uniform(0, max_dt_s) * 1e9) * (1 if rng.random() > 0.05 else -1)
        t[a] = max(t[a] + dt, 1)
        evt = make_event(rng, "" if rng.random() < 0.005 else a, pos[a], t[a])
        out.append((a, evt.SerializeToString()))
    return out


class OldEngine:
    """The original agent's per-event body, with a dict for the table."""

    def __init__(self, coeffs, seeded):
        self.coeffs = coeffs
        self.table = {a: old._AccumulatorRecord(rounds_fired=r) for a, r in seeded.items()}

    def feed(self, raw):
        try:
            evt = pb.EntityTelemetryEvent()
            evt.ParseFromString(raw)
        except Exception:  # noqa: BLE001
            return None
        asset_id = evt.asset.asset_id
        if not asset_id:
            return None
        kin = old._extract_kinematic(evt)
        if kin is None:
            return None
        ecef_m, pitch_deg, roll_deg, sample_time_ns = kin
        rec = self.table.get(asset_id) or old._AccumulatorRecord()
        state = old._record_to_state(rec)
        observe_kinematic(state, ecef_m=ecef_m, pitch_deg=pitch_deg,
                          roll_deg=roll_deg, sample_time_ns=sample_time_ns)
        self.table[asset_id] = old._state_to_record(state)
        out = old._build_derived_event(evt, derive_sustainment(state, self.coeffs))
        return asset_id, self.table[asset_id], out, evt


def json_state(rec):
    d = json.loads(rec.dumps())
    d.pop("__faust")  # the namespace differs by design; the changelog test covers it
    return d


def normalise(out, src):
    out = pb.EntityTelemetryEvent.FromString(out.SerializeToString())
    out.ClearField("event_id")
    out.provenance.ClearField("ingest_time")
    out.provenance.ClearField("producer_stamp")
    if not (src.provenance.sample_time.seconds or src.provenance.sample_time.nanos):
        out.provenance.ClearField("sample_time")
    return out


@pytest.fixture
def pinned(monkeypatch):
    """Offline, deterministic: sockets raise, the wall clock the fallback
    reads is fixed for both paths, the env defaults are the same for both."""
    def no_socket(*a, **k):
        raise AssertionError("network access attempted")
    monkeypatch.setattr(socket, "socket", no_socket)
    fake = types.SimpleNamespace(time=lambda: FAKE_NOW_S)
    monkeypatch.setattr(roles, "time", fake)
    monkeypatch.setattr(old, "time", fake)
    monkeypatch.setenv("OPENDDIL_EDGE_ID", "edge-tst")
    monkeypatch.delenv("OPENDDIL_REGION_ID", raising=False)


def test_old_and_new_agree_exactly(pinned):
    total = diffs = 0
    outputs_seen = emitted = 0
    statuses = set()
    for n, (name, params, step_m, dt_s) in enumerate(SCENARIOS):
        unit, decl = make_wear(params)
        rec_cls = runner.make_record_class(decl)
        coeffs = WearCoefficients(
            track_life_total_km=float(unit.params["track_life_total_km"]),
            engine_life_total_hours=float(unit.params["engine_life_total_hours"]),
            barrel_life_total_rounds=int(unit.params["barrel_life_total_rounds"]),
            terrain_total_deg_km=float(unit.params["terrain_total_deg_km"]),
        )
        # Rounds fired has no live writer; seed some assets so the barrel
        # model and its status bands are compared too.
        seeded = {a: 3 * i for i, a in enumerate(ASSETS) if i % 2}
        oracle = OldEngine(coeffs, seeded)
        states = {a: dict(core.default_state(decl), rounds_fired=r) for a, r in seeded.items()}

        for asset, raw in stream(1000 + n, SAMPLES_PER_SCENARIO, step_m, dt_s):
            total += 1
            expected = oracle.feed(raw)
            src = pb.EntityTelemetryEvent.FromString(raw) if expected else None
            key = expected[0] if expected else None
            new_state, sends, counters = core.process_one(unit, decl, raw, states.get(key))
            if expected is None:
                if new_state is not None or sends:
                    diffs += 1
                continue
            if new_state is None or len(sends) != 1:
                diffs += 1
                continue
            states[key] = new_state
            if json_state(expected[1]) != json_state(rec_cls(**new_state)):
                diffs += 1
            topic, k, msg = sends[0]
            assert topic == "derived-sustainment" and k == key.encode()
            if normalise(msg, src) != normalise(expected[2], src):
                diffs += 1
            emitted += 1
            for comp in msg.sustainment.wear.components.values():
                statuses.add((name, comp.status))
            stamp = msg.provenance.producer_stamp
            assert (stamp.unit, stamp.version, stamp.tier) == ("wear", "1.0.0", "edge-tst")
            assert len(stamp.config_hash) == 64
            assert not expected[2].provenance.HasField("producer_stamp")
            outputs_seen += 1

    assert total >= 10_000 and emitted > 0.85 * total
    assert diffs == 0
    # The streams reached every wear band under some parameter set, so the
    # status thresholds were compared, not just the nominal case.
    reached = {s for _, s in statuses}
    assert {pb.WEAR_NOMINAL, pb.WEAR_DEGRADED, pb.WEAR_SERVICE_DUE, pb.WEAR_CRITICAL,
            pb.WEAR_UNSPECIFIED} <= reached


def test_the_oracle_is_the_original(pinned):
    """Guard on the oracle itself: its record carries the original namespace
    the changelog uses, so the equivalence above is against the real thing."""
    assert json.loads(old._AccumulatorRecord().dumps())["__faust"]["ns"].endswith("_AccumulatorRecord")
