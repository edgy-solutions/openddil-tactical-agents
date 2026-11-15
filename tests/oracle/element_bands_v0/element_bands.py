"""
Schmitt-trigger band tracking for element fields. A pure sibling module (same
boundary discipline as posture.py): no Faust, no Kafka, no I/O.

Why a Schmitt trigger: a health value that dithers around a limit would
otherwise flip bands on every sample and flood the event topic. The band rises
to k when the value passes limits[k-1], but falls below band k only when the
value drops under limits[k-1] - hysteresis, so noise inside that margin is not
a crossing.

The first observation of a (asset, element, field) key sets its band and is
not a crossing: the edge restarting must not announce every element as having
"entered" its band. Non-numeric values and unconfigured fields are ignored;
nothing is defaulted.
"""
from __future__ import annotations

import json
from typing import Optional


def _is_num(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def parse_config(json_text: str) -> dict:
    """Parse and validate the band config; ValueError on any bad shape."""
    try:
        cfg = json.loads(json_text)
    except (TypeError, ValueError) as e:
        raise ValueError(f"band config is not valid JSON: {e}") from e
    if not isinstance(cfg, dict) or not cfg:
        raise ValueError("band config must be a non-empty object of field -> spec")
    for field, spec in cfg.items():
        if not isinstance(spec, dict):
            raise ValueError(f"{field}: spec must be an object")
        limits = spec.get("limits")
        names = spec.get("names")
        hyst = spec.get("hysteresis")
        sev = spec.get("severity")
        if not isinstance(limits, list) or not limits or not all(_is_num(x) for x in limits):
            raise ValueError(f"{field}: limits must be a non-empty list of numbers")
        if any(b <= a for a, b in zip(limits, limits[1:])):
            raise ValueError(f"{field}: limits must be strictly ascending")
        if (not isinstance(names, list) or len(names) != len(limits) + 1
                or not all(isinstance(n, str) and n for n in names)
                or len(set(names)) != len(names)):
            raise ValueError(
                f"{field}: names must be {len(limits) + 1} distinct non-empty strings")
        if not _is_num(hyst) or hyst < 0:
            raise ValueError(f"{field}: hysteresis must be a number >= 0")
        if not isinstance(sev, dict):
            raise ValueError(f"{field}: severity must be an object of band -> string")
        for n in names:
            if not isinstance(sev.get(n), str) or not sev[n]:
                raise ValueError(f"{field}: band {n!r} has no severity")
    return cfg


class BandTracker:
    def __init__(self, config: dict):
        self._cfg = config
        self._band: dict[tuple[str, str, str], int] = {}

    def update(self, asset_id: str, element_path: str, field: str,
               value) -> Optional[tuple[str, str]]:
        spec = self._cfg.get(field)
        if spec is None or not _is_num(value):
            return None
        limits, names, h = spec["limits"], spec["names"], spec["hysteresis"]
        key = (asset_id, element_path, field)
        old = self._band.get(key)

        # Highest band whose limit the value is strictly above.
        up = sum(1 for lim in limits if value > lim)
        if old is None:
            self._band[key] = up
            return None
        new = old
        if up > old:
            new = up
        else:
            # Fall only while below the band's limit minus the margin.
            while new > 0 and value < limits[new - 1] - h:
                new -= 1
        if new == old:
            return None
        self._band[key] = new
        return names[old], names[new]
