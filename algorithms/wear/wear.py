"""Wear: the four Phase 5 wear models (ADR-0020) as a declared unit.

Cumulative per-asset state (distance, observed time, terrain integral, rounds
fired) folded from successive ECEF samples, and a SustainmentMetrics payload
derived from it. The arithmetic is the engine's original, operation for
operation: the equivalence test holds this unit to the frozen original with
no tolerance, so reorder nothing here without reading that test.

Honest scope (ADR-0020): this demonstrates the mechanism. The life totals are
authored placeholders, not calibrated against measured ground truth. "Operating
hours" is observed time (DIS Entity State carries no engine-on signal), "track
wear" is distance over a kilometres-of-life constant, "barrel life" is rounds
over a rounds-of-life constant, "suspension stress" is a trapezoidal terrain
integral. Every value emitted is DERIVED; the runner stamps that, not this unit.

Emit-side unit contract (ADR-0013), unchanged:
    track      hours_in_service  "km"      distance-driven
    engine     hours_in_service  "h"       observed-time stand-in
    barrel     cycles            "1"       rounds, UCUM dimensionless count
    suspension hours_in_service  "deg.km"  terrain integral
`remaining_useful_life` is always percent remaining, unit "%".
"""
from __future__ import annotations

from algorithm_runtime.contract import Unit

_NS_PER_HOUR = 3_600_000_000_000.0


def _percent_remaining(consumed: float, total: float) -> float:
    """Clamped percent of life remaining, in [0.0, 100.0]."""
    if total <= 0.0:
        return 0.0
    frac = max(0.0, min(1.0, consumed / total))
    return 100.0 * (1.0 - frac)


class Wear(Unit):
    def step(self, sample, state):
        self.observe(sample, state)
        return {"sustainment": self.derive(state)}

    # -- accumulation -----------------------------------------------------
    def observe(self, sample, state):
        """Fold one kinematic observation into the accumulators, in place.

        No out-of-order replay protection: that is an explicit non-goal of
        the mechanism demonstration (scripted scenarios, monotonic time)."""
        ecef = sample["position_ecef_m"]
        pitch_deg = sample.get("pitch_deg")
        roll_deg = sample.get("roll_deg")
        sample_time_ns = sample["sample_time_ns"]

        # First observation? The stored position is the sentinel, NOT
        # `observed_time_first_ns == 0` — a legitimate sample time of 0 would
        # otherwise read as "never observed" and the first timestamp be lost.
        last_x = state["last_ecef_x"]
        last_y = state["last_ecef_y"]
        last_z = state["last_ecef_z"]
        is_first = last_x is None or last_y is None or last_z is None

        # --- time window ---
        if is_first:
            state["observed_time_first_ns"] = sample_time_ns
            state["observed_time_last_ns"] = sample_time_ns
        elif sample_time_ns > state["observed_time_last_ns"]:
            state["observed_time_last_ns"] = sample_time_ns

        # --- distance + terrain integral (need a previous position) ---
        if not is_first:
            dx = ecef[0] - last_x
            dy = ecef[1] - last_y
            dz = ecef[2] - last_z
            seg_m = (dx * dx + dy * dy + dz * dz) ** 0.5
            state["cumulative_distance_m"] += seg_m

            # Average |pitch|+|roll| across the segment x segment length in
            # km. Trapezoidal: uses both endpoints.
            last_pitch = state["last_pitch_deg"]
            last_roll = state["last_roll_deg"]
            if (
                last_pitch is not None
                and pitch_deg is not None
                and last_roll is not None
                and roll_deg is not None
            ):
                avg_pitch = (abs(last_pitch) + abs(pitch_deg)) / 2.0
                avg_roll = (abs(last_roll) + abs(roll_deg)) / 2.0
                state["terrain_integral_deg_km"] += (avg_pitch + avg_roll) * (seg_m / 1000.0)

        # --- update "last" state for next segment ---
        state["last_ecef_x"] = ecef[0]
        state["last_ecef_y"] = ecef[1]
        state["last_ecef_z"] = ecef[2]
        if pitch_deg is not None:
            state["last_pitch_deg"] = pitch_deg
        if roll_deg is not None:
            state["last_roll_deg"] = roll_deg

    def record_round_fired(self, state):
        """Increment rounds fired.

        Nothing calls this: the Fire input is not wired (ADR-0020 names
        Fire/Detonation ingestion as a follow-on step). The barrel model is
        shaped against it, so while `rounds_fired` stays 0 the barrel
        component is absent, which is honest absence rather than a zero."""
        state["rounds_fired"] += 1

    # -- derivation -------------------------------------------------------
    def _status(self, consumed, total):
        """Map fraction-consumed to a WearStatus enum name."""
        if total <= 0:
            return "WEAR_UNSPECIFIED"
        frac = consumed / total
        if frac >= self.params["critical_fraction"]:
            return "WEAR_CRITICAL"
        if frac >= self.params["service_due_fraction"]:
            return "WEAR_SERVICE_DUE"
        if frac >= self.params["degraded_fraction"]:
            return "WEAR_DEGRADED"
        return "WEAR_NOMINAL"

    def _component(self, measure_key, value, unit, consumed, total):
        return {
            measure_key: {"value": value, "unit": unit},
            "remainingUsefulLife": {
                "value": _percent_remaining(consumed, total),
                "unit": "%",
            },
            "status": self._status(consumed, total),
        }

    def derive_components(self, state):
        p = self.params
        components = {}

        # Track: distance-driven.
        if state["cumulative_distance_m"] > 0.0:
            cumulative_km = state["cumulative_distance_m"] / 1000.0
            components["track"] = self._component(
                "hoursInService", cumulative_km, "km",
                cumulative_km, p["track_life_total_km"])

        # Engine: observed time as the stand-in for engine-on time.
        hours = (state["observed_time_last_ns"] - state["observed_time_first_ns"]) / _NS_PER_HOUR
        if hours > 0.0:
            components["engine"] = self._component(
                "hoursInService", hours, "h",
                hours, p["engine_life_total_hours"])

        # Barrel: rounds through the tube.
        if state["rounds_fired"] > 0:
            rounds = float(state["rounds_fired"])
            components["barrel"] = self._component(
                "cycles", rounds, "1",
                rounds, float(p["barrel_life_total_rounds"]))

        # Suspension: terrain integral, deg.km consumed against its total.
        # The absolute scale is a placeholder; the kernel is the mechanism.
        if state["terrain_integral_deg_km"] > 0.0:
            components["suspension"] = self._component(
                "hoursInService", state["terrain_integral_deg_km"], "deg.km",
                state["terrain_integral_deg_km"], p["terrain_total_deg_km"])
        return components

    def derive(self, state):
        """SustainmentMetrics as proto-JSON, without value_provenance (the
        runner states the source class). Empty when no model has data yet."""
        components = self.derive_components(state)
        return {"wear": {"components": components}} if components else {}
