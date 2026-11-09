"""The posture unit against the frozen old in-line path (tests/oracle/posture_v0).

Both sides see the same raw records in the same order. The unit runs through
Wiring and a generated Record exactly as the agent does (so the record bytes
are the ones the changelog gets); the oracle is the old agent body with a dict
for its Table. Everything it sends is compared as (topic, key, bytes)."""
from __future__ import annotations

import json
import random
from collections import Counter

import pytest
from openddil.telemetry.v1 import telemetry_pb2 as pb

from algorithm_runtime import declaration as d
from algorithm_runtime import replay, runner
from unit_helpers import make_posture

EDGE, REGION = "edge-tst", "region-tst"
POSTURE_FIELDS = ("posture", "posture_since", "motion", "motion_since")


def event(asset, t, *, launcher=None, emitting=None, speed=None, nation="", rel=()):
    e = pb.EntityTelemetryEvent()
    e.asset.asset_id = asset
    e.asset.platform_variant = "variant-x"
    if launcher is not None:
        e.operational_state.launcher_raised = launcher
    if emitting is not None:
        e.operational_state.emitting = emitting
    if speed is not None:
        e.kinematics.velocity.ecef.x = speed
    e.provenance.sample_time.FromNanoseconds(int(t * 1e9))
    if nation:
        e.provenance.originator_nation = nation
        e.provenance.releasable_to.extend(rel)
    return e.SerializeToString()


def stream(seed=7, per_asset=2000):
    """Round-robin over three assets.

    launch-1: a launcher variant; the bit starts explicitly False, then flaps
        and holds; speed phases of stopped and moving, a velocity-absent patch.
    sensor-1: an emitting variant that states no signal for its first 700
        records (the first signal arrives mid-stream), then flaps fast.
    quiet-1: never states a signal; speed phases only (stays unspecified or
        moving/unspecified, never emplaced)."""
    rng = random.Random(seed)
    t = {a: 1_000.0 for a in ("launch-1", "sensor-1", "quiet-1")}
    phase = {a: [0, 0.0] for a in t}      # [records left, speed]
    sig = {"launch-1": False, "sensor-1": True}
    out = []
    for i in range(per_asset):
        for a in t:
            t[a] += rng.choice((1.0, 2.0, 3.0, 5.0, 8.0))
            if phase[a][0] <= 0:
                phase[a] = [rng.randint(4, 40), rng.choice((0.0, 0.0, 0.5, 2.0, 6.0))]
            phase[a][0] -= 1
            speed = phase[a][1]
            if a == "launch-1" and 900 <= i < 960:
                speed = None
            kw = {"speed": speed}
            if a == "launch-1":
                if rng.random() < 0.08:
                    sig[a] = not sig[a]
                kw["launcher"] = sig[a]
            elif a == "sensor-1" and i >= 700:
                if rng.random() < 0.3:
                    sig[a] = not sig[a]
                kw["emitting"] = sig[a]
            out.append(event(a, t[a], nation="AAA" if a == "launch-1" else "",
                             rel=("AAA", "BBB") if a == "launch-1" else (), **kw))
    return out


class Rig:
    """The unit as the agent drives it, next to the oracle."""

    def __init__(self, overrides=None, initial=None, record_bytes=None):
        self.unit, self.decl = make_posture(overrides)
        self.params = d.resolve_params(self.decl, overrides)
        self.cls = runner.make_record_class(self.decl)
        self.names = [f.name for f in self.decl.state_fields if f.persistence != "memory"]
        self.table: dict = {}
        self.wiring = runner.Wiring(self.unit, self.decl, lambda _a: None, {})
        import os
        os.environ["OPENDDIL_EDGE_ID"], os.environ["OPENDDIL_REGION_ID"] = EDGE, REGION
        from posture_v0 import Oracle
        self.oracle = Oracle(self.params, EDGE, REGION, initial=initial)
        for aid, b in (record_bytes or {}).items():
            self.table[aid] = self.cls.loads(b, serializer="json")

    def feed(self, raw):
        """([old sends], [new sends]) as (topic, key, bytes)."""
        old = self.oracle.sends(raw)
        e = pb.EntityTelemetryEvent()
        e.ParseFromString(raw)
        aid = e.asset.asset_id
        rec = self.table.get(aid)
        state = {n: getattr(rec, n) for n in self.names} if rec is not None else None
        new_state, sends = self.wiring.process(raw, 0, state)
        if new_state is not None:
            self.table[aid] = self.cls(**new_state)
        return old, [(t, k, runner.outputs.encode(m)) for t, k, m in sends]

    def record_matches(self, aid):
        rec, want = self.table[aid], self.oracle.table[aid]
        return {k: getattr(rec, k) for k in POSTURE_FIELDS} == want


def run(rig, records):
    n_old = n_new = 0
    for i, raw in enumerate(records):
        old, new = rig.feed(raw)
        assert new == old, f"record {i}"
        n_old, n_new = n_old + len(old), n_new + len(new)
    return n_old, n_new


def test_two_thousand_records_per_asset_send_exactly_what_the_old_path_sent():
    rig = Rig()
    recs = stream()
    kinds = Counter()
    for i, raw in enumerate(recs):
        old, new = rig.feed(raw)
        assert new == old, f"record {i}"
        for topic, _k, _v in new:
            kinds[topic] += 1
    assert len(recs) == 6000
    assert kinds["telemetry-latest-state"] == 6000
    assert kinds["tactical-events"] > 20
    for aid in ("launch-1", "sensor-1", "quiet-1"):
        assert rig.record_matches(aid), aid


def test_every_transition_row_fires_in_the_data():
    """Row 1 (to moving), row 2 (moving to each of the three), row 3 (stationary
    and signalled to emplaced), row 4 (emplaced and signal off)."""
    rig = Rig()
    seen = set()
    for raw in stream():
        prev = {a: dict(r) for a, r in rig.oracle.table.items()}
        rig.feed(raw)
        e = pb.EntityTelemetryEvent()
        e.ParseFromString(raw)
        a = e.asset.asset_id
        before = prev.get(a, {"posture": "unspecified"})["posture"]
        after = rig.oracle.table[a]["posture"]
        if before != after:
            seen.add((before, after))
    to_moving = {p for p in seen if p[1] == "moving"}
    from_moving = {p[1] for p in seen if p[0] == "moving"}
    assert to_moving, seen
    assert {"unspecified", "emplaced", "emplacing"} <= from_moving, seen
    assert {("unspecified", "emplaced"), ("emplaced", "march_ordered")} <= seen, seen
    assert ("march_ordered", "emplaced") in seen or ("emplacing", "emplaced") in seen, seen


def test_a_signal_that_first_appears_mid_stream_switches_the_forward():
    """Untouched (the input bytes) while unspecified and signal-less; decided
    after the first signal, even a steady False."""
    rig = Rig()
    first = event("s", 100.0, speed=0.0)
    old, new = rig.feed(first)
    assert new == old and new[0][2] == first
    second = event("s", 101.0, speed=0.0, emitting=False)
    old, new = rig.feed(second)
    assert new == old                  # decided (re-serialized), though no posture is set yet
    third = event("s", 102.0, speed=0.0, emitting=True)
    old, new = rig.feed(third)
    assert new == old and new[0][2] != third
    got = pb.EntityTelemetryEvent()
    got.ParseFromString(new[0][2])
    assert got.operational_state.posture_status == pb.POSTURE_STATUS_EMPLACED


def test_params_come_from_the_declaration_and_equal_the_old_defaults():
    _, decl = make_posture()
    assert d.resolve_params(decl, None) == {"move_speed_mps": 1.0, "move_hold_s": 10.0, "stop_hold_s": 20.0}


@pytest.mark.parametrize("overrides", [{"move_speed_mps": 0.2, "move_hold_s": 3, "stop_hold_s": 7},
                                       {"move_hold_s": 0, "stop_hold_s": 0}])
def test_other_thresholds_stay_equal(overrides):
    rig = Rig(overrides)
    run(rig, stream(seed=11, per_asset=600))


def test_a_warm_start_from_a_changelog_record_stays_equal():
    warm = (b'{"last_temp_k":0.0,"temp_ewma_k":0.0,"temp_ewma_alpha":0.2,"posture":"moving",'
            b'"posture_since":900.0,"motion":"moving","motion_since":905.0,'
            b'"__faust":{"ns":"faust_edge.StateRecord"}}')
    initial = {"launch-1": {"posture": "moving", "posture_since": 900.0,
                            "motion": "moving", "motion_since": 905.0}}
    rig = Rig(initial=initial, record_bytes={"launch-1": warm})
    # signal_ever_seen is memory-only: cold on both sides after the "restart".
    run(rig, stream(seed=3, per_asset=300))
    assert rig.record_matches("launch-1")


def test_a_flip_and_flip_back_is_two_events_not_debounced():
    rig = Rig()
    a = event("x", 10.0, speed=0.0, launcher=True)       # unspecified -> emplaced
    b = event("x", 11.0, speed=0.0, launcher=False)      # emplaced -> march_ordered
    c = event("x", 12.0, speed=0.0, launcher=True)       # march_ordered -> emplaced
    events = 0
    for raw in (a, b, c):
        old, new = rig.feed(raw)
        assert new == old
        events += sum(1 for t, _, _ in new if t == "tactical-events")
    assert events == 3


def test_suppressed_payloads_are_counted():
    rig = Rig()
    recs = stream(seed=5, per_asset=500)
    sent = 0
    for raw in recs:
        _, new = rig.feed(raw)
        sent += sum(1 for t, _, _ in new if t == "tactical-events")
    assert sent + rig.wiring.counters["transition_suppressed"] == len(recs)


def test_the_transition_event_is_byte_for_byte_the_old_one_with_labels():
    rig = Rig()
    old, new = rig.feed(event("l", 50.0, speed=0.0, launcher=True, nation="AAA", rel=("AAA", "BBB")))
    assert [t for t, _, _ in new] == ["telemetry-latest-state", "tactical-events"]
    assert new == old
    ev = json.loads(new[1][2])
    assert ev["type"] == "openddil.posture.transition"
    assert "producer_stamp" not in json.dumps(ev)


def test_an_event_without_an_asset_id_is_skipped_where_the_old_path_wrote_under_empty_key():
    """The one divergence: the old agent keyed its table by "" and went on; the
    host counts no_asset_id and sends nothing."""
    rig = Rig()
    raw = event("", 10.0, speed=0.0, launcher=True)
    old, new = rig.feed(raw)
    assert [t for t, _, _ in old] == ["telemetry-latest-state", "tactical-events"] and old[0][1] == ""
    assert new == []
    assert rig.wiring.counters["no_asset_id"] == 1


def test_compare_oracle_mode_reports_equal_and_leaves_unstamped_kinds_out_of_stamp_failures(monkeypatch):
    unit, decl = make_posture()
    recs = [(i, int(1_000_000 + i * 1000), b"k", raw) for i, raw in enumerate(stream(seed=2, per_asset=300))]
    rep = replay.compare_oracle(unit, decl, d.resolve_params(decl, None), recs,
                                {"OPENDDIL_EDGE_ID": EDGE, "OPENDDIL_REGION_ID": REGION}, "posture")
    assert rep.different == 0 and rep.old_only == 0 and rep.new_only == 0
    assert rep.compared == sum(rep.emitted_old.values()) == sum(rep.emitted_new.values())
    assert rep.stamp_failures == 0 and sorted(rep.unstamped_kinds) == ["latest_state", "posture_transition"]
    assert rep.emitted_old == rep.emitted_new and rep.emitted_new["latest_state"] == 900


def test_the_posture_declaration_passes_the_validator_and_the_schema():
    import yaml
    from jsonschema import Draft202012Validator
    from unit_helpers import POSTURE_DIR, ROOT
    doc = yaml.safe_load((POSTURE_DIR / "unit.yaml").read_text(encoding="utf-8"))
    schema = json.loads((ROOT / "algorithms" / "declaration.schema.json").read_text(encoding="utf-8"))
    assert list(Draft202012Validator(schema).iter_errors(doc)) == []
    decl = d.validate(doc)
    assert [o.kind for o in decl.outputs] == ["latest_state", "posture_transition"]
    assert [f.name for f in decl.state_fields][-1] == "signal_ever_seen"
