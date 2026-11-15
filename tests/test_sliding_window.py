"""SlidingWindow against the path it replaces: a deque(maxlen) of Samples
filtered by windows._filter_window."""
from __future__ import annotations

import json
import random
from collections import deque

from detection.windows import Sample, _filter_window, _ureg

from algorithm_runtime.contract import SlidingWindow


def reference(items, window_ns, now_ns):
    return [(s.epoch_ns, float(s.quantity.magnitude))
            for s in _filter_window(list(items), window_ns=window_ns, now_ns=now_ns)]


def test_matches_deque_and_filter_window_on_synthetic_sequences():
    rng = random.Random(7)
    for _ in range(60):
        window_ns = rng.choice([1, 10, 1_000, 5_000_000_000])
        cap = rng.choice([1, 3, 50, 500])
        w = SlidingWindow(window_ns, cap)
        dq = deque(maxlen=cap)
        t = rng.randrange(0, 1_000)
        for _ in range(rng.randrange(0, 400)):
            # forward, repeated and out-of-order times; equal times keep order
            t += rng.choice([0, 0, 1, window_ns // 3 + 1, window_ns * 2, -(window_ns // 2)])
            t = max(t, 0)
            v = rng.uniform(-100, 100)
            w.push(t, v)
            dq.append(Sample(epoch_ns=t, quantity=_ureg.Quantity(v, "m")))
            if rng.random() < 0.3:
                now = t + rng.choice([0, 1, window_ns, -1])
                assert w.in_window(now) == reference(dq, window_ns, now)
        for now in (t - window_ns - 1, t - window_ns, t, t + 1, t + window_ns, t + window_ns + 1):
            assert w.in_window(now) == reference(dq, window_ns, now)


def test_boundaries_are_inclusive_both_ends():
    w = SlidingWindow(10, 10)
    for t in (89, 90, 100, 101):
        w.push(t, float(t))
    assert [t for t, _ in w.in_window(100)] == [90, 100]


def test_cap_drops_the_oldest():
    w = SlidingWindow(1_000, 3)
    for t in range(5):
        w.push(t, float(t))
    assert [t for t, _ in w.in_window(4)] == [2, 3, 4]


def test_state_round_trip_is_json_safe_and_exact():
    rng = random.Random(3)
    w = SlidingWindow(500, 20)
    for _ in range(40):
        w.push(rng.randrange(0, 1_000), rng.uniform(-5, 5))
    state = json.loads(json.dumps(w.to_state()))
    back = SlidingWindow.from_state(500, 20, state)
    assert back.to_state() == w.to_state()
    assert back.in_window(900) == w.in_window(900)
