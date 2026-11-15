"""Seeded, deterministic sustainment values for the trends equivalence.

The recorded raw stream carries no fuel, consumables, wear or fault codes, so
the recorded run of the old-versus-new compare would exercise nothing. This
adds them to each decoded event, before both sides see it, keeping the
recorded asset ids, order and timestamps. `make(seed)` returns a function that
fills one event in place; call it on the records in order. The same seed and
the same record order always give the same values.

What it covers, so each of the old path's branches is hit on real timing:
  * fuel in L, now and then in gal_us (the unit changes inside one window),
    absent on some records, and a zero value with no unit (which is "unset");
  * two ammunition slots, one with capacity 0 and one with capacity > 0, whose
    capacity changes now and then, and a third slot present on some records;
  * wear components with hours and RUL (RUL in h or %), one component whose RUL
    has a unit pint cannot parse, one with RUL and no hours, one with hours and
    no RUL;
  * active fault codes on some records.
Per-asset values drift smoothly with noise, so regressions are not degenerate.
"""
from __future__ import annotations

import random

_SLOTS = ("main_gun", "coax", "smoke")


class _AssetWalk:
    def __init__(self, rng: random.Random):
        self.fuel = rng.uniform(200.0, 900.0)
        self.rounds = {s: rng.randrange(20, 400) for s in _SLOTS}
        self.hours = {c: rng.uniform(100.0, 3000.0) for c in ("engine", "transmission", "track", "orphan")}
        self.rul = {c: rng.uniform(500.0, 5000.0) for c in ("engine", "transmission", "track", "orphan")}
        self.cap_main = rng.choice([0, 40, 120])


def make(seed: int):
    rng = random.Random(seed)
    walks: dict[str, _AssetWalk] = {}

    def fill(evt) -> None:
        aid = evt.asset.asset_id
        w = walks.get(aid)
        if w is None:
            w = walks[aid] = _AssetWalk(rng)
        s = evt.sustainment

        # fluids
        r = rng.random()
        if r < 0.80:
            w.fuel = max(0.0, w.fuel - rng.uniform(0.0, 0.6))
            if rng.random() < 0.12:
                s.fluids.fuel_remaining.value = w.fuel / 3.785411784
                s.fluids.fuel_remaining.unit = "gal_us"
            else:
                s.fluids.fuel_remaining.value = w.fuel
                s.fluids.fuel_remaining.unit = "L"
        elif r < 0.85:
            s.fluids.fuel_remaining.value = 0.0          # zero and no unit: not a quantity
        # else: absent

        # consumables
        if rng.random() < 0.85:
            if rng.random() < 0.02:
                w.cap_main = rng.choice([0, 40, 120])
            for slot in _SLOTS:
                if slot == "smoke" and rng.random() < 0.5:
                    continue
                if rng.random() < 0.3:
                    w.rounds[slot] = max(0, w.rounds[slot] - rng.randrange(0, 3))
                item = s.consumables.items[slot]
                item.quantity_remaining = w.rounds[slot]
                item.quantity_capacity = (w.cap_main if slot == "main_gun"
                                          else 0 if slot == "coax" else 60)
                if rng.random() < 0.5:
                    item.nsn = f"nsn-{slot}"

        # wear
        if rng.random() < 0.75:
            for comp in ("engine", "transmission", "track", "orphan"):
                w.hours[comp] += rng.uniform(0.0, 0.01)
                w.rul[comp] = max(0.0, w.rul[comp] - rng.uniform(0.0, 0.02))
                comp_state = None
                if comp != "orphan" or rng.random() < 0.5:
                    comp_state = s.wear.components[comp]
                if comp_state is None:
                    continue
                if comp != "orphan":           # "orphan" carries RUL and never hours
                    comp_state.hours_in_service.value = w.hours[comp]
                    comp_state.hours_in_service.unit = "h"
                if comp == "track":            # hours but a RUL pint cannot parse
                    comp_state.remaining_useful_life.value = w.rul[comp]
                    comp_state.remaining_useful_life.unit = "no such unit"
                elif comp == "transmission" and rng.random() < 0.3:
                    pass                       # hours with no RUL on this record
                elif comp == "engine" and rng.random() < 0.4:
                    comp_state.remaining_useful_life.value = w.rul[comp] / 50.0
                    comp_state.remaining_useful_life.unit = "%"
                else:
                    comp_state.remaining_useful_life.value = w.rul[comp]
                    comp_state.remaining_useful_life.unit = "h"

        # fault codes
        if rng.random() < 0.15:
            for _ in range(rng.randrange(1, 3)):
                s.health.active_fault_codes.append(f"P{rng.randrange(100, 999)}")

    return fill
