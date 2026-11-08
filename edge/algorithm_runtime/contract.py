"""The contract a declared analytic unit is written against.

A unit is a class with one method, `step(sample, state)`. It is built once
with its resolved parameters, is handed one sample at a time for one key,
mutates that key's declared state in place, and returns the outputs it wants
sent (output kind -> payload dict) or None. There are no other hooks: no
window callback, no startup or shutdown, no access to the broker or the table.

Network guarantee, and its limits. Units may import only the allowlist in
`guard.py` (`__future__`, `math`, `dataclasses`, `typing` and this module),
checked statically over the unit's source at load. The tests also run units
with sockets disabled. That is a guard against accident in a bundle someone
reviewed, not a sandbox: it does not stop code that is deliberately written
to get around a static check.

Stdlib only.
"""
from __future__ import annotations

from collections import deque
from typing import Mapping

# The fixed role vocabulary: what a declaration may name as an input. Code
# defines it so a typo in a declaration is a refusal, not a silently empty
# input. Value is the Python type the role carries when present.
ROLES: dict[str, str] = {
    "position_ecef_m": "tuple[float, float, float]",
    "pitch_deg": "float",
    "roll_deg": "float",
    "sample_time_ns": "int",
    # Kelvin, converted through detection.units (pint) from whatever UCUM unit
    # the source stated; present only when the source set the quantity.
    "component_temp_k": "float",
    "ambient_temp_k": "float",
}


class Unit:
    """Base class for a declared unit. `params` is resolved and typed: the
    declaration's defaults with the deployment's overrides applied."""

    def __init__(self, params: Mapping[str, int | float]):
        self.params = params

    def step(self, sample: Mapping[str, object], state: dict) -> Mapping[str, dict] | None:
        """Fold one sample into `state` (declared fields only, mutated in
        place) and return {output kind: payload dict}, or None for no output.

        `sample` holds the declared input roles that are present; a role
        declared `required` is always there."""
        raise NotImplementedError


class SlidingWindow:
    """Time window over (t_ns, value) pairs with a count cap.

    Same behaviour as the deque + `windows._filter_window` pair the edge
    trends use: `push` into a deque with maxlen `cap` (the oldest drops),
    `in_window` keeps `now_ns - window_ns <= t <= now_ns` and returns the
    pairs sorted by time (stable, so equal times keep arrival order)."""

    def __init__(self, window_ns: int, cap: int):
        self.window_ns = window_ns
        self.cap = cap
        self._items: deque[tuple[int, float]] = deque(maxlen=cap)

    def push(self, t_ns: int, value: float) -> None:
        self._items.append((t_ns, value))

    def in_window(self, now_ns: int) -> list[tuple[int, float]]:
        cutoff = now_ns - self.window_ns
        kept = [p for p in self._items if cutoff <= p[0] <= now_ns]
        return sorted(kept, key=lambda p: p[0])

    def to_state(self) -> list[list]:
        """JSON-safe, so a window can live in a unit's declared state."""
        return [[t, v] for t, v in self._items]

    @classmethod
    def from_state(cls, window_ns: int, cap: int, state: list[list]) -> "SlidingWindow":
        w = cls(window_ns, cap)
        for t, v in state:
            w.push(int(t), v)
        return w
