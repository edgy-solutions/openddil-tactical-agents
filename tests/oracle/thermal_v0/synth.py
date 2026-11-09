"""Seeded, deterministic temperatures for measuring the thermal unit.

The recorded raw stream carries neither `sustainment.thermal.component_temperature`
nor `ambient_temperature`, so a run over the recording skips every record in
the required-role check and measures nothing of the unit. This adds both, in K,
to each decoded event, keeping the recorded asset ids, order and timestamps.
It is used by `--measure` only. `make(seed)` returns a function that fills one
event in place; call it on the records in order. The same seed and the same
record order always give the same values.

Each asset has an ambient temperature that drifts a little, and a component
temperature held at `ambient + delta`. The delta walks toward a target, then
picks another, so it crosses the unit's `delta_threshold_k` (80 K by default)
upward and downward, and the climbs are steep enough (up to 5 K a sample)
to pass the rate threshold now and then. That runs the unit's whole path:
the decision, the state write and the anomaly output.
"""
from __future__ import annotations

import random

DELTA_THRESHOLD_K = 80.0
_TARGETS = (30.0, 125.0, 60.0, 110.0, 15.0, 95.0)


class _AssetWalk:
    def __init__(self, rng: random.Random):
        self.ambient = rng.uniform(270.0, 315.0)
        self.delta = rng.uniform(20.0, 140.0)
        self.target = rng.choice(_TARGETS)


def make(seed: int):
    rng = random.Random(seed)
    walks: dict[str, _AssetWalk] = {}

    def fill(evt) -> None:
        aid = evt.asset.asset_id
        w = walks.get(aid)
        if w is None:
            w = walks[aid] = _AssetWalk(rng)
        w.ambient += rng.uniform(-0.05, 0.05)
        if abs(w.target - w.delta) < 2.0:
            w.target = rng.choice(_TARGETS)
        move = max(-5.0, min(5.0, w.target - w.delta)) * rng.uniform(0.2, 1.0)
        w.delta += move + rng.gauss(0.0, 0.3)
        th = evt.sustainment.thermal
        th.ambient_temperature.value = w.ambient
        th.ambient_temperature.unit = "K"
        th.component_temperature.value = w.ambient + w.delta
        th.component_temperature.unit = "K"

    return fill
