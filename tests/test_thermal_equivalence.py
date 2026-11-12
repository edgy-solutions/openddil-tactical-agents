"""Old-vs-new: the frozen thermal-runaway rule against the thermal unit, sample by sample.

The oracle (tests/oracle/detection_v0) is the original `edge/detection/algorithms.py`
rule driven the way faust_edge drove it (the view, AssetState from the stored
floats, REGISTERED, the CloudEvent, the `or 0.0` write-back). The new path is
`core.process_one` over the thermal unit. Seeded synthetic streams plus hand
cases at the thresholds. The comparison is exact: the stored floats `==`, the
same events, every old key equal after leaving out only the id and the time,
and the new event's extra keys exactly the ones the contract adds. Any diff
is a failure; there is no tolerance to tune.
"""
from __future__ import annotations

import json
import random
import re
import socket
import uuid
from datetime import datetime

import pytest
from openddil.telemetry.v1 import telemetry_pb2 as pb

from algorithm_runtime import core, declaration, outputs
from detection_v0 import glue
from unit_helpers import make_thermal

EDGE, REGION = "edge-tst", "region-tst"
ASSETS = [f"asset-{i}" for i in range(14)] + ["no-thermal"]
N_SAMPLES = 6_000


def to_unit(kelvin: float, unit: str) -> float:
    if unit in ("degC", "Cel"):
        return kelvin - 273.15
    if unit == "[degF]":
        return (kelvin - 273.15) * 9.0 / 5.0 + 32.0
    return kelvin


def make_event(asset, comp=None, amb=None, nation=None, releasable=()):
    """`comp` / `amb` are (value, unit) as the proto holds them, or None for unset."""
    evt = pb.EntityTelemetryEvent()
    evt.asset.asset_id = asset
    if comp is not None:
        evt.sustainment.thermal.component_temperature.value = comp[0]
        evt.sustainment.thermal.component_temperature.unit = comp[1]
    if amb is not None:
        evt.sustainment.thermal.ambient_temperature.value = amb[0]
        evt.sustainment.thermal.ambient_temperature.unit = amb[1]
    if nation:
        evt.provenance.originator_nation = nation
        evt.provenance.releasable_to.extend(releasable)
    return evt


def stream(seed, n):
    """Interleaved assets, each walking ramps (some steeper than the rate
    threshold, some not), plateaus and drops, with the temperature stated in
    K, degC or [degF], and now and then a reading left unset."""
    rng = random.Random(seed)
    comp = {a: rng.uniform(300.0, 340.0) for a in ASSETS}
    amb = {a: rng.uniform(270.0, 310.0) for a in ASSETS}
    phase = {a: ("plateau", 0) for a in ASSETS}
    out = []
    for _ in range(n):
        a = rng.choice(ASSETS)
        if a == "no-thermal":
            out.append(make_event(a))
            continue
        kind, left = phase[a]
        if left <= 0:
            kind = rng.choice(["ramp-fast", "ramp-slow", "plateau", "drop", "ramp-fast"])
            left = rng.randrange(3, 40)
        phase[a] = (kind, left - 1)
        step = {"ramp-fast": rng.uniform(1.0, 7.0), "ramp-slow": rng.uniform(0.0, 2.0),
                "plateau": rng.uniform(-0.3, 0.3), "drop": -rng.uniform(1.0, 30.0)}[kind]
        comp[a] = max(comp[a] + step, 1.0)
        amb[a] = amb[a] + rng.uniform(-0.2, 0.2)
        unit = rng.choice(["K", "K", "degC", "[degF]"])
        c = (to_unit(comp[a], unit), unit)
        m = (to_unit(amb[a], unit), unit)
        roll = rng.random()
        if roll < 0.03:
            c = None
        elif roll < 0.06:
            m = None
        elif roll < 0.08:
            c = (0.0, "")           # unset, as the source leaves it
        nation = rng.choice(["", "", "AAA", "BBB"])
        out.append(make_event(a, c, m, nation, rng.sample(["AAA", "BBB", "CCC"], 2) if nation else ()))
    return out


@pytest.fixture
def pinned(monkeypatch):
    def no_socket(*a, **k):
        raise AssertionError("network access attempted")
    monkeypatch.setattr(socket, "socket", no_socket)
    monkeypatch.setenv("OPENDDIL_EDGE_ID", EDGE)
    monkeypatch.setenv("OPENDDIL_REGION_ID", REGION)


class Pair:
    """Both paths side by side. `feed` runs one event through each and checks
    everything the test promises; it returns the number of events emitted."""

    def __init__(self, params=None):
        self.unit, self.decl = make_thermal(params)
        self.old_table: dict[str, glue.StoredRecord] = {}
        self.new_states: dict[str, dict] = {}
        self.fired = 0
        self.compared = 0

    def feed(self, evt):
        asset = evt.asset.asset_id
        rec = self.old_table.setdefault(asset, glue.StoredRecord())
        old_sent = [json.loads(b) for b in glue.feed(evt, rec, EDGE, REGION)]
        new_state, sends, counters = core.process_one(
            self.unit, self.decl, evt.SerializeToString(), self.new_states.get(asset))
        if new_state is not None:
            self.new_states[asset] = new_state
        stored = self.new_states.get(asset, core.default_state(self.decl))
        # exactly equal, floats `==`, not approx
        assert (rec.last_temp_k, rec.temp_ewma_k) == (stored["last_temp_k"], stored["temp_ewma_k"])
        assert type(stored["last_temp_k"]) is float and type(stored["temp_ewma_k"]) is float
        assert not counters.get("role_unconvertible")
        assert len(sends) == len(old_sent)
        for (topic, key, msg), old in zip(sends, old_sent):
            assert topic == "tactical-events" and key == asset.encode()
            new = json.loads(outputs.encode(msg))
            self.check_event(new, old, evt)
        self.compared += 1
        self.fired += len(sends)
        return len(sends)

    def check_event(self, new, old, src):
        for k in old:
            if k in ("id", "time"):
                continue
            if k != "data":
                assert new[k] == old[k], k
        data = new["data"]
        labelled = bool(src.provenance.originator_nation)
        extra = {"provenance", "producer_stamp"} | ({"originator_nation", "releasable_to"} if labelled else set())
        assert set(data) - set(old["data"]) == extra
        for k in old["data"]:
            assert data[k] == old["data"][k], k
        assert set(new) == set(old)
        assert data["provenance"] == {"origin": "DERIVED", "confidence": 0.0}
        stamp = data["producer_stamp"]
        assert stamp == {
            "unit": "thermal", "version": "1.0.0", "tier": EDGE,
            "config_hash": declaration.config_hash("thermal", "1.0.0", self.unit.params)}
        assert re.fullmatch(r"[0-9a-f]{64}", stamp["config_hash"])
        if labelled:
            assert data["originator_nation"] == src.provenance.originator_nation
            assert data["releasable_to"] == list(src.provenance.releasable_to)
        assert str(uuid.UUID(new["id"])) == new["id"]
        assert datetime.fromisoformat(new["time"]).tzinfo is not None


def test_old_and_new_agree_exactly_on_a_synthetic_stream(pinned):
    pair = Pair()
    events = stream(2024, N_SAMPLES)
    assert len(events) >= 5_000 and len({e.asset.asset_id for e in events}) >= 12
    units = {e.sustainment.thermal.component_temperature.unit for e in events}
    assert {"K", "degC", "[degF]"} <= units
    for evt in events:
        pair.feed(evt)
    # The stream crossed both thresholds, and also sat below them: the
    # decision was compared on both sides, not just the quiet one.
    assert 50 < pair.fired < pair.compared * 0.5


def test_alpha_moves_only_the_unused_average(pinned):
    """The average is not in the decision: at another alpha the unit fires
    on exactly the samples the original does and keeps the same last
    temperature, and only its stored average differs."""
    unit, decl = make_thermal({"ewma_alpha": 0.7})
    old_table, states = {}, {}
    fired = differing_ewma = 0
    for evt in stream(5, 1_500):
        asset = evt.asset.asset_id
        rec = old_table.setdefault(asset, glue.StoredRecord())
        n_old = len(glue.feed(evt, rec, EDGE, REGION))
        new_state, sends, _ = core.process_one(unit, decl, evt.SerializeToString(), states.get(asset))
        if new_state is not None:
            states[asset] = new_state
        stored = states.get(asset, core.default_state(decl))
        assert len(sends) == n_old
        assert stored["last_temp_k"] == rec.last_temp_k
        differing_ewma += stored["temp_ewma_k"] != rec.temp_ewma_k
        fired += n_old
    assert fired > 0 and differing_ewma > 0


def at(prev_k, comp_k, amb_k=300.0):
    return [make_event("hand", (prev_k, "K"), (amb_k, "K")), make_event("hand", (comp_k, "K"), (amb_k, "K"))]


HAND = [
    # (name, events, fires on the last event)
    ("delta and rate both exactly at the thresholds", at(378.0, 380.0), 0),
    ("delta over, rate exactly 2.0", at(380.0, 382.0), 0),
    ("rate over, delta exactly 80.0", at(370.0, 380.0), 0),
    ("both just above", at(377.9, 380.1), 1),
    ("delta just above, rate just above", at(380.0, 382.0000001), 1),
    ("rate well above, delta just under", at(370.0, 379.9999), 0),
    ("first sample already over delta", [make_event("hand", (400.0, "K"), (300.0, "K"))], 0),
    ("first sample over, then a rise", [make_event("hand", (400.0, "K"), (300.0, "K")),
                                        make_event("hand", (403.0, "K"), (300.0, "K"))], 1),
    ("ambient absent", [make_event("hand", (400.0, "K"), None), make_event("hand", (410.0, "K"), None)], 0),
    ("component absent", [make_event("hand", None, (300.0, "K")), make_event("hand", None, (300.0, "K"))], 0),
    ("value 0.0 with unit '' is absent", [make_event("hand", (0.0, ""), (300.0, "K")),
                                          make_event("hand", (400.0, "K"), (0.0, ""))], 0),
    ("value 0.0 with unit degC is present", [make_event("hand", (0.0, "degC"), (0.0, "degC")),
                                            make_event("hand", (100.0, "degC"), (0.0, "degC"))], 1),
    # g. the frozen oracle's glue calls the live from_proto; on the old table it raised on "Cel"
    ("Cel ramp fires, oracle and unit agree", [make_event("hand", (110.0, "Cel"), (20.0, "Cel")),
                                              make_event("hand", (115.0, "Cel"), (20.0, "Cel"))], 1),
    ("Cel just under the delta does not fire", [make_event("hand", (96.0, "Cel"), (20.0, "Cel")),
                                               make_event("hand", (99.0, "Cel"), (20.0, "Cel"))], 0),
    ("an asset with no thermal at all", [make_event("hand"), make_event("hand")], 0),
    ("unlabelled source", [make_event("hand", (300.0, "K"), (200.0, "K")),
                           make_event("hand", (310.0, "K"), (200.0, "K"))], 1),
    ("labelled source", [make_event("hand", (300.0, "K"), (200.0, "K"), "AAA", ["AAA", "BBB"]),
                         make_event("hand", (310.0, "K"), (200.0, "K"), "AAA", ["AAA", "BBB"])], 1),
    ("units mixed on one asset", [make_event("hand", (26.85, "degC"), (300.0, "K")),
                                  make_event("hand", (700.0, "[degF]"), (300.0, "K"))], 1),
]


@pytest.mark.parametrize("name,events,fires", HAND, ids=[h[0] for h in HAND])
def test_hand_cases(pinned, name, events, fires):
    pair = Pair()
    n = [pair.feed(e) for e in events]
    assert n[-1] == fires
    assert sum(n[:-1]) == 0


# --- readings the old path could not survive ------------------------------------
# ("Cel", UCUM's Celsius, is no longer here: detection.units bridges it, so it is a compared unit; see HAND.)
@pytest.mark.parametrize("unit", ["1", "m", "no_such_unit"])
def test_an_unconvertible_unit_is_skipped_and_counted_with_no_state_write(pinned, unit):
    """Deliberate divergence: the old path raised inside the agent; the new one
    drops the role, counts it and writes nothing."""
    thermal, decl = make_thermal()
    state = {"last_temp_k": 311.0, "temp_ewma_k": 77.0}
    evt = make_event("hand", (5.0, unit), (300.0, "K"))
    new_state, sends, counters = core.process_one(thermal, decl, evt.SerializeToString(), state)
    assert new_state is None and sends == []
    assert counters["role_unconvertible"] == 1
    assert state == {"last_temp_k": 311.0, "temp_ewma_k": 77.0}


def test_both_unconvertible_counts_both(pinned):
    thermal, decl = make_thermal()
    evt = make_event("hand", (5.0, "m"), (5.0, "no_such_unit"))
    _, _, counters = core.process_one(thermal, decl, evt.SerializeToString(), None)
    assert counters["role_unconvertible"] == 2


def test_the_oracle_is_the_original():
    """Guard on the oracle itself: the frozen rule's constants and its
    registry are what faust_edge ran."""
    from detection_v0 import algorithms as frozen
    assert [f.__name__ for f in frozen.REGISTERED] == ["thermal_runaway"]
    assert frozen.AssetState().temp_ewma_alpha == 0.2
