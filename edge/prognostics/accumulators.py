"""Cumulative per-asset state for the prognostics engine.

Pure-Python. Plain dataclass + pure functions. NO faust types — agent.py
wraps `AccumulatorState` for the Faust Table at the boundary, mirroring
the algorithms.py / asset_state.py split that already exists in this edge
package. When the engine extracts into its own service later, this module
goes with it unchanged.

The accumulators are *lifetime* state, not rolling windows: cumulative
distance and observed-time-window are summed-forever quantities (ADR-0020,
durable Faust Table). They are owned by the Table (RocksDB-backed,
changelog-replicated) so a faust-edge restart does NOT reset them — that
is the difference from the existing windowing buffers in faust_edge.py.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass
class AccumulatorState:
    """Per-asset cumulative state. Mutable — `observe_kinematic` updates
    in place. Defaults represent "never observed yet."

    Field-name units are explicit (`_m`, `_ns`, `_deg_km`, etc.) — the
    accumulator math is on plain floats with conventional units, deferring
    pint to the proto-emit boundary in agent.py. Phase 5 mechanism keeps
    this layer simple."""

    # Total distance traveled (m), summed from successive ECEF positions
    # by Euclidean delta. For short segments this is ~indistinguishable
    # from great-circle; Phase 5 mechanism — fine.
    cumulative_distance_m: float = 0.0

    # Wall-clock window observed for this asset (ns). `observed_hours()`
    # returns (last - first) / 3.6e12. Phase 5 stand-in for "operating
    # hours": DIS Entity State PDUs carry no engine-on signal (ADR-0020),
    # so observed time is the closest available proxy.
    observed_time_first_ns: int = 0
    observed_time_last_ns: int = 0

    # Cumulative (|pitch| + |roll|) deg × distance km, integrated per
    # segment via trapezoidal-style averaging. Proxy for suspension
    # stress (ADR-0020 wear model 4). Pure mechanism — the integration
    # kernel is what's demonstrated; the constant that turns this into
    # "consumed suspension life" is in WearCoefficients and is a
    # placeholder.
    terrain_integral_deg_km: float = 0.0

    # Rounds fired. Shape only in Phase 5 — Fire/Detonation PDU ingestion
    # is named in scope by ADR-0020 but is a follow-on wiring step. The
    # accumulator field is here so the model has a typed input slot when
    # that wiring lands; today the slot stays 0 and the barrel-life
    # model emits nothing.
    rounds_fired: int = 0

    # Last observation, for delta computations. None until first sample.
    # Stored as a tuple so the dataclass is trivially serialisable.
    last_ecef_m: tuple[float, float, float] | None = None
    last_pitch_deg: float | None = None
    last_roll_deg: float | None = None


def observe_kinematic(
    state: AccumulatorState,
    *,
    ecef_m: tuple[float, float, float],
    pitch_deg: float | None,
    roll_deg: float | None,
    sample_time_ns: int,
) -> None:
    """Fold one kinematic observation into the per-asset accumulators.

    Mutates `state` in place. Pure with respect to outside-the-state: no
    I/O, no globals, no clock reads. Caller is responsible for ordering
    (no out-of-order replay protection here — that's an explicit Phase 5
    non-goal; ADR-0020's mechanism demonstration runs against scripted
    scenarios with monotonic timestamps)."""

    # First observation? Use `last_ecef_m is None` as the sentinel, NOT
    # `observed_time_first_ns == 0` — a legitimate sample_time_ns of 0
    # (epoch start, or a test that begins at t=0) would otherwise be
    # treated as "never observed" and the first timestamp would be lost.
    is_first = state.last_ecef_m is None

    # --- time window ---
    if is_first:
        state.observed_time_first_ns = sample_time_ns
        state.observed_time_last_ns = sample_time_ns
    elif sample_time_ns > state.observed_time_last_ns:
        state.observed_time_last_ns = sample_time_ns

    # --- distance + terrain integral (need a previous position) ---
    if not is_first:
        dx = ecef_m[0] - state.last_ecef_m[0]
        dy = ecef_m[1] - state.last_ecef_m[1]
        dz = ecef_m[2] - state.last_ecef_m[2]
        seg_m = (dx * dx + dy * dy + dz * dz) ** 0.5
        state.cumulative_distance_m += seg_m

        # Terrain integral: average |pitch|+|roll| across the segment ×
        # segment length in km. Trapezoidal — uses both endpoints.
        if (
            state.last_pitch_deg is not None
            and pitch_deg is not None
            and state.last_roll_deg is not None
            and roll_deg is not None
        ):
            avg_pitch = (abs(state.last_pitch_deg) + abs(pitch_deg)) / 2.0
            avg_roll = (abs(state.last_roll_deg) + abs(roll_deg)) / 2.0
            state.terrain_integral_deg_km += (avg_pitch + avg_roll) * (seg_m / 1000.0)

    # --- update "last" state for next segment ---
    state.last_ecef_m = ecef_m
    if pitch_deg is not None:
        state.last_pitch_deg = pitch_deg
    if roll_deg is not None:
        state.last_roll_deg = roll_deg


def observed_hours(state: AccumulatorState) -> float:
    """Observed time window in hours. Phase 5 stand-in for operating
    hours — see ADR-0020 and the comment on `observed_time_*_ns`.

    Returns 0.0 for a never-observed state (defaults give last - first =
    0). A single observation also yields 0.0 (last == first set together
    by `observe_kinematic`'s first-call branch)."""
    delta_ns = state.observed_time_last_ns - state.observed_time_first_ns
    return delta_ns / 3_600_000_000_000.0  # 1 hour = 3.6e12 ns


def record_round_fired(state: AccumulatorState) -> None:
    """Increment rounds fired. Not currently called by `agent.py` —
    Fire/Detonation PDU ingestion is a follow-on wiring step per
    ADR-0020. The function is the seam the barrel-life model is shaped
    against; the wiring just needs to call it from the DIS sidecar when
    Fire/Detonation PDUs land on `raw-sensor-stream`."""
    state.rounds_fired += 1
