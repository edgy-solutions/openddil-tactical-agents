"""Rolling-window trends: the edge's windowed sustainment telemetry as a declared unit.

Every raw sample is folded into per-signal sliding windows (fuel, ammunition
counts, component wear hours and remaining useful life). Every
`emit_every_n_samples`-th sample of an asset, if the asset has any window yet,
the unit emits one `window` output: a least-squares trend (latest value, slope
per hour, r squared) for each signal that has two or more samples in its
window. The clock is the input record's broker time (`record_time_ns`), which
stamps every sample, the window cutoff and the window end.

This reproduces the old in-line path exactly, quirks included:
  * the window start and the sample count come from the fluid trends only;
  * capacity and nsn come from the sample that triggers the emit, even when
    that item's capacity is 0, and the fault codes come from that sample;
  * an emitted window with no trends still goes out;
  * a window exists for an asset only once its first value has been pushed.
One difference: wear trends are ordered by component key (the old order
followed set iteration, so it changed with the hash seed).

Stdlib only. The payload and its shape are defined in
`algorithm_runtime.outputs`' docstring (kind `window`).
"""
from __future__ import annotations

from algorithm_runtime.contract import SlidingWindow, Unit

_NS_PER_H = 3.6e12


class _Series:
    """One signal's samples: values and, beside them, the unit each sample
    came in (the old path kept a pint Quantity per sample, so the slope's unit
    is the first sample's and the latest value's unit is the last one's). Two
    windows of the same cap and window length hold the same entries, so their
    filtered, stably sorted lists line up."""

    def __init__(self, window_ns: int, cap: int):
        self.values = SlidingWindow(window_ns, cap)
        self.unit_ids = SlidingWindow(window_ns, cap)
        self.units: list[str] = []

    def push(self, t_ns: int, value: float, unit: str) -> None:
        if unit not in self.units:
            self.units.append(unit)
        self.values.push(t_ns, value)
        self.unit_ids.push(t_ns, float(self.units.index(unit)))

    def trend(self, now_ns: int) -> dict | None:
        """The least-squares trend over the window, or None below 2 samples.
        The arithmetic is windows._slope_per_hour's, operation for operation."""
        kept = self.values.in_window(now_ns)
        if len(kept) < 2:
            return None
        ids = self.unit_ids.in_window(now_ns)
        t0_ns = kept[0][0]
        xs = [(t - t0_ns) / _NS_PER_H for t, _v in kept]
        ys = [float(v) for _t, v in kept]

        n = len(xs)
        sx, sy = sum(xs), sum(ys)
        mean_x = sx / n
        mean_y = sy / n

        cov = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
        var_x = sum((x - mean_x) ** 2 for x in xs)
        var_y = sum((y - mean_y) ** 2 for y in ys)

        if var_x == 0:
            slope, r2 = 0.0, 0.0
        else:
            slope = cov / var_x
            r2 = (cov ** 2) / (var_x * var_y) if var_y > 0 else 0.0
            r2 = max(0.0, min(1.0, r2))
        return {
            "latest": ys[-1],
            "latest_unit": self.units[int(ids[-1][1])],
            "slope": slope,
            "slope_unit": self.units[int(ids[0][1])],
            "r_squared": r2,
            "latest_sample_ns": kept[-1][0],
            "window_start_ns": kept[0][0],
            "sample_count": n,
        }


class Trends(Unit):
    def __init__(self, params):
        self.params = params          # the base class does this; the guard refuses a dunder call
        self.fluid_ns = int(float(params["fluid_window_min"]) * 60 * 1e9)
        self.wear_ns = int(float(params["wear_window_min"]) * 60 * 1e9)
        self.every_n = params["emit_every_n_samples"]
        self.cap = params["window_cap"]

    def _push(self, buffers: dict, key: str, window_ns: int, t_ns: int, value: float, unit: str) -> None:
        series = buffers.get(key)
        if series is None:
            series = _Series(window_ns, self.cap)
            buffers[key] = series
        series.push(t_ns, value, unit)

    def step(self, sample, state):
        now_ns = sample["record_time_ns"]
        buffers = state["buffers"]

        fuel = sample.get("fuel_remaining")
        if fuel is not None:
            self._push(buffers, "fluid:fuel_remaining", self.fluid_ns, now_ns, fuel[0], fuel[1])
        consumables = sample.get("consumables") or {}
        for slot, item in consumables.items():
            if item["capacity"] > 0:
                self._push(buffers, "ammo:" + slot, self.fluid_ns, now_ns, float(item["remaining"]), "count")
        for comp, qty in (sample.get("wear_hours") or {}).items():
            self._push(buffers, "wear_hours:" + comp, self.wear_ns, now_ns, qty[0], qty[1])
        for comp, qty in (sample.get("wear_rul") or {}).items():
            self._push(buffers, "wear_rul:" + comp, self.wear_ns, now_ns, qty[0], qty[1])

        state["samples_since_emit"] += 1
        if state["samples_since_emit"] < self.every_n:
            return None
        state["samples_since_emit"] = 0
        if not buffers:
            return None
        return {"window": self._window(buffers, consumables, sample, now_ns)}

    def _window(self, buffers: dict, consumables: dict, sample, now_ns: int) -> dict:
        total_samples = 0
        window_min_start = None
        fluid_trends = {}
        for key, series in buffers.items():
            if not key.startswith("fluid:"):
                continue
            trend = series.trend(now_ns)
            if trend is None:
                continue
            fluid_trends[key.split(":", 1)[1]] = trend
            total_samples += trend["sample_count"]
            if window_min_start is None or trend["window_start_ns"] < window_min_start:
                window_min_start = trend["window_start_ns"]

        ammo = []
        for key, series in buffers.items():
            if not key.startswith("ammo:"):
                continue
            trend = series.trend(now_ns)
            if trend is None:
                continue
            slot = key.split(":", 1)[1]
            entry = {"slot_key": slot, "remaining": trend}
            item = consumables.get(slot)
            if item is not None:
                entry["capacity"] = item["capacity"]
                entry["nsn"] = item["nsn"]
            ammo.append(entry)

        components = sorted({key.split(":", 1)[1] for key in buffers if key.startswith("wear_hours:")})
        wear = []
        for comp in components:
            hours = buffers.get("wear_hours:" + comp)
            rul = buffers.get("wear_rul:" + comp)
            if hours is None or rul is None:
                continue
            h_trend = hours.trend(now_ns)
            r_trend = rul.trend(now_ns)
            if h_trend is None and r_trend is None:
                continue
            entry = {"component_key": comp}
            if h_trend is not None:
                entry["hours_in_service"] = h_trend
            if r_trend is not None:
                entry["remaining_useful_life"] = r_trend
            wear.append(entry)

        return {
            "window_start_ns": window_min_start or now_ns,
            "window_end_ns": now_ns,
            "sample_count": total_samples,
            "fluid_trends": fluid_trends,
            "consumable_trends": ammo,
            "wear_trends": wear,
            "active_fault_codes": list(sample.get("active_fault_codes") or ()),
        }
