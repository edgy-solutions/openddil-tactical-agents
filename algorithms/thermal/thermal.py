"""Thermal runaway: the edge's one anomaly rule (`thermal.runaway.v1`) as a
declared unit.

A component more than `delta_threshold_k` above ambient and rising by more
than `rate_threshold_k_per_sample` since the previous sample is a runaway.
The rate is per sample, not per second: the rule never looked at the clock.
The arithmetic is the original's, operation for operation; the equivalence
test holds this unit to the frozen rule with no tolerance, so reorder nothing
here without reading that test.

The moving average is carried in state exactly as the original carried it
(starting from the stored 0.0, so the first sample's value is `alpha * temp`,
not `temp`) and is not used in the decision.

Temperatures arrive in kelvin: the runner converts the source's unit, and a
reading the source did not set is absent. The severity, the labels and the
envelope are the contract's, not this unit's.
"""
from __future__ import annotations

from algorithm_runtime.contract import Unit


class ThermalRunaway(Unit):
    def step(self, sample, state):
        comp = sample["component_temp_k"]
        amb = sample["ambient_temp_k"]

        a = self.params["ewma_alpha"]
        ewma = a * comp + (1 - a) * state["temp_ewma_k"]

        delta = comp - amb
        rate = comp - (state["last_temp_k"] or comp)

        # Stored as `x or 0.0`, as the original wrote it back. Both values are
        # floats, so only a 0.0 K reading (or a -0.0) could differ from storing
        # them as they are.
        state["last_temp_k"] = comp or 0.0
        state["temp_ewma_k"] = ewma or 0.0

        if delta > self.params["delta_threshold_k"] and rate > self.params["rate_threshold_k_per_sample"]:
            return {"anomaly": {
                "summary": f"Component {delta:.1f}K above ambient, rising {rate:.2f}K/sample",
                "evidence": {
                    "component_temp_k": comp,
                    "ambient_temp_k": amb,
                    "delta_k": delta,
                    "rate_k_per_sample": rate,
                },
            }}
        return None
