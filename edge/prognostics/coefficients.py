"""Authored placeholder coefficients for the Phase 5 prognostics engine.

THESE ARE NOT CALIBRATED — they are authored placeholders. Per ADR-0020,
Phase 5 demonstrates the *mechanism*; the *coefficients* are validated
against measured ground truth in the deferred AFSIM / VR-Forces phase.
Env-overridable so a future calibration run can swap values without a
code change. Order-of-magnitude reasonable for heavy ground platforms;
that is the most that should be claimed for any of these numbers.

This module is framework-free: imports os + stdlib only. No faust, no
protobuf.
"""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class WearCoefficients:
    """The four wear-model coefficients (ADR-0020 initial set). The long
    tail (brake wear, filter clogging, fluid degradation, fatigue cycling,
    ...) is explicitly deferred."""

    # Distance-driven track wear: total track life in km. After
    # `track_life_total_km` of cumulative distance the track is "spent."
    # Placeholder; real calibration is the AFSIM/VR-Forces phase.
    track_life_total_km: float = 5000.0

    # Operating-hours engine wear: total engine life in operating hours.
    # Phase 5 uses *observed* time as a stand-in for "engine-on" time
    # because DIS Entity State PDUs carry no engine-on signal (ADR-0020).
    engine_life_total_hours: float = 5000.0

    # Rounds-through-tube barrel life: total rounds before barrel
    # replacement. Highly platform-specific in reality; the value here is
    # placeholder. NB: Phase 5 ingestion of Fire/Detonation PDUs is the
    # *next* wiring step (ADR-0020 scopes it in) — the model has the
    # shape; the input feed is on the follow-on list.
    barrel_life_total_rounds: int = 1000

    # Terrain-integral suspension stress: cumulative (|pitch|+|roll|) (deg)
    # × distance (km) at which the suspension is fully consumed. Pure
    # placeholder; the entire metric is a demonstration of the *shape* of
    # a terrain-stress model — the integration kernel, not the constants.
    terrain_total_deg_km: float = 100000.0

    @classmethod
    def from_env(cls) -> "WearCoefficients":
        """Build from PROGNOSTICS_* env vars; defaults applied for any
        unset. Same env-driven idiom as fusion/thresholds.py."""
        return cls(
            track_life_total_km=float(
                os.getenv("PROGNOSTICS_TRACK_LIFE_TOTAL_KM", "5000"),
            ),
            engine_life_total_hours=float(
                os.getenv("PROGNOSTICS_ENGINE_LIFE_TOTAL_HOURS", "5000"),
            ),
            barrel_life_total_rounds=int(
                os.getenv("PROGNOSTICS_BARREL_LIFE_TOTAL_ROUNDS", "1000"),
            ),
            terrain_total_deg_km=float(
                os.getenv("PROGNOSTICS_TERRAIN_TOTAL_DEG_KM", "100000"),
            ),
        )
