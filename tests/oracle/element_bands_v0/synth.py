"""Seeded, deterministic element envelopes for the element-bands equivalence.

The recording holds no element telemetry (`asset-element-telemetry` is not in
it), so what is compared is the RECORDED TIMING with SYNTHETIC ELEMENT VALUES.
`make(seed)` returns `fill(ts_ms, asset_id) -> bytes`: the JSON envelope for
one recorded (timestamp, asset) pair, with `observed_at_ns` taken from the
record. Call it on the pairs in order; the same seed and the same pairs always
give the same envelopes.

The envelope has the shape the existing band tests use (`_env` in
edge/tests/test_element_band_event.py): asset_id, profile_name,
observed_at_ns, originator_nation, releasable_to, and elements carrying
element_id, layer_name and health, plus temp_c and load_pct as the producer
sends them. Each asset has 8 elements. An element's health is a random walk
that heads for a target (low, between the limits, or high) and then picks
another, so it crosses each limit in both directions; now and then it dithers
inside the hysteresis margin of a limit, which is not a crossing. Some assets
are unlabelled, as a source can be.
"""
from __future__ import annotations

import json
import random

LIMITS = (0.90, 0.97)
HYSTERESIS = 0.01
_LAYERS = ("RADAR UNIT", "BACKPLANE", "PROCESSOR BANK", "GAN MMIC CHIP")
_TARGETS = (0.55, 0.93, 0.99, 0.80, 0.95, 0.30)


class _Element:
    def __init__(self, rng: random.Random):
        self.health = rng.uniform(0.3, 0.99)
        self.target = rng.choice(_TARGETS)
        self.hold = rng.randint(3, 40)
        self.dither_around = None      # a limit, while dithering
        self.dither_left = 0

    def step(self, rng: random.Random) -> float:
        if self.dither_left > 0:
            self.dither_left -= 1
            # Stay inside the margin: above the limit by a little, then below
            # it by less than the hysteresis (never a fall).
            self.health = self.dither_around + rng.uniform(-HYSTERESIS * 0.9, HYSTERESIS * 0.9)
        else:
            self.hold -= 1
            if self.hold <= 0:
                self.target = rng.choice(_TARGETS)
                self.hold = rng.randint(3, 40)
                if rng.random() < 0.3:
                    self.dither_around = rng.choice(LIMITS)
                    self.dither_left = rng.randint(4, 12)
                    self.health = self.dither_around + 0.001
            self.health += (self.target - self.health) * 0.25 + rng.gauss(0.0, 0.01)
        self.health = min(1.0, max(0.0, self.health))
        return self.health


class _Asset:
    def __init__(self, rng: random.Random):
        self.elements = [_Element(rng) for _ in range(8)]
        self.labelled = rng.random() < 0.7
        self.rel = rng.choice((["BDR"], ["BDR", "CRL"], []))


def make(seed: int):
    rng = random.Random(seed)
    assets: dict[str, _Asset] = {}

    def fill(ts_ms: int, asset_id: str) -> bytes:
        a = assets.get(asset_id)
        if a is None:
            a = assets[asset_id] = _Asset(rng)
        elements = []
        for i, el in enumerate(a.elements):
            elements.append({
                "element_id": f"el-{i}", "layer_name": _LAYERS[i % len(_LAYERS)],
                # Emitted rounded to 9 places: rng.gauss goes through libm, whose last bit differs
                # between platforms, and the envelope's bytes must be the same on every platform.
                # The walk itself keeps full precision; only what is sent is rounded.
                "health": round(el.step(rng), 9),
                "temp_c": round(40.0 + rng.uniform(-5.0, 5.0), 9), "load_pct": round(rng.uniform(5.0, 95.0), 9),
            })
        env = {"asset_id": asset_id, "profile_name": "mrad",
               "observed_at_ns": int(ts_ms) * 1_000_000, "elements": elements}
        if a.labelled:
            env["originator_nation"] = "ATL"
            env["releasable_to"] = list(a.rel)
        return json.dumps(env).encode("utf-8")

    return fill


# --- the element rollup's extra fields --------------------------------------------------
_LEVELS = ("CONDITION_LEVEL_NOMINAL", "CONDITION_LEVEL_DEGRADED", "CONDITION_LEVEL_CRITICAL")
_SOURCES = ("CONDITION_SOURCE_APPEARANCE_POWER", "CONDITION_SOURCE_EMISSION", "CONDITION_SOURCE_DATA_HEALTH")
_VARIANTS = ("", "v1", "v2")


def make_extended(seed: int):
    """`make(seed)` plus what the element rollup reads: an `operational` block
    (core_temp_c and uptime_hours, each sometimes missing or not a number), a
    `condition` block (sometimes malformed), a platform_variant, and now and
    then an element with no `element_id`. The base envelope is `make(seed)`'s,
    unchanged, and the extras come from their own generator (seeded from `seed`
    but separate), so adding them moves nothing `make` produces."""
    base = make(seed)
    rng = random.Random(seed * 7919 + 104729)

    def fill(ts_ms: int, asset_id: str) -> bytes:
        env = json.loads(base(ts_ms, asset_id))
        op = {}
        if rng.random() < 0.8:
            op["core_temp_c"] = 55.0 + rng.uniform(-10.0, 25.0)
        if rng.random() < 0.8:
            op["uptime_hours"] = rng.uniform(1.0, 900.0)
        if rng.random() < 0.05:
            op["core_temp_c"] = "hot"                       # present, not a number
        if rng.random() < 0.3:
            roll = rng.random()
            if roll < 0.75:
                op["condition"] = {
                    "level": rng.choice(_LEVELS), "moved_by": [rng.choice(_SOURCES)],
                    "claims": [{"source": rng.choice(_SOURCES), "level": rng.choice(_LEVELS),
                                "detail": "synthetic"}]}
            elif roll < 0.9:
                op["condition"] = {"level": {"not": "a level"}}     # malformed: dropped
            else:
                op["condition"] = "red"                             # not a block at all
        if rng.random() < 0.9:
            env["operational"] = op
        env["platform_variant"] = rng.choice(_VARIANTS)
        if rng.random() < 0.1:
            env["elements"].append({"layer_name": "ORPHAN", "health": rng.uniform(0.3, 0.99),
                                    "temp_c": 41.0, "load_pct": 50.0})
        return json.dumps(env).encode("utf-8")

    return fill
