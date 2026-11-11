"""The element rollup, as a declared unit: the counts and averages over one
element snapshot that the tiers above the edge read instead of the per-element
payload.

The logic is the old `rollup_from_envelope`, operation for operation: an
element counts as critical when its numeric health is strictly above
`health_critical`, else degraded when strictly above `health_degraded`;
temperature and load are averaged over the elements that carry a number (a bool
is not one); the core temperature and uptime ride only when the envelope's
`operational` block carries a number; the producer's `condition` block is
passed through as it came (the host parses it, and drops a malformed one).

The unit states what the snapshot says. The window around it (the asset, the
span, the trends, the clock, the origin node) is the host's.

Stdlib only.
"""
from __future__ import annotations

from algorithm_runtime.contract import Unit


def _is_num(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _mean(values: list) -> float:
    return sum(values) / len(values) if values else 0.0


class ElementRollup(Unit):
    def step(self, sample, state):
        critical_at = self.params["health_critical"]
        degraded_at = self.params["health_degraded"]
        elements = sample["element_values"]
        critical = degraded = 0
        temps: list = []
        loads: list = []
        for el in elements:
            h = el.get("health")
            if _is_num(h):
                if h > critical_at:
                    critical += 1
                elif h > degraded_at:
                    degraded += 1
            t = el.get("temp_c")
            if _is_num(t):
                temps.append(float(t))
            ld = el.get("load_pct")
            if _is_num(ld):
                loads.append(float(ld))

        nation, releasable = sample.get("envelope_labels", ("", ()))
        out = {
            "profile_name": sample.get("profile_name", ""),
            "element_count": len(elements),
            "critical_count": critical,
            "degraded_count": degraded,
            "avg_temp_c": _mean(temps),
            "avg_load_pct": _mean(loads),
            "observed_at_ns": sample.get("observed_at_ns"),
            "originator_nation": nation,
            "releasable_to": list(releasable),
            "platform_variant": sample.get("platform_variant", ""),
        }
        # Presence matters: an absent readout stays absent rather than becoming 0.
        op = sample.get("operational") or {}
        if _is_num(op.get("core_temp_c")):
            out["core_temp_c"] = float(op["core_temp_c"])
        if _is_num(op.get("uptime_hours")):
            out["uptime_hours"] = float(op["uptime_hours"])
        cond = op.get("condition")
        if isinstance(cond, dict):
            out["condition"] = cond
        state["envelopes_seen"] += 1
        return {"element_window": out}
