"""The four Phase 5 wear models (ADR-0020 initial set).

Pure-Python in the fusion/rules.py mold:
  - inputs: a frozen `AccumulatorState` snapshot + `WearCoefficients`
  - output: a `(component_name, WearState)` tuple, or None if there is
    not enough data yet
  - no faust, no Kafka, no I/O — only protobuf bindings

`derive_sustainment(state, coeffs)` is the single public entry point.
It runs every model, builds a `SustainmentMetrics`, and populates the
per-value provenance map with the Phase 5 message-level wildcard
(`"*"` → DERIVED) per ADR-0020.

Honest scope reminder (ADR-0020):
  - Phase 5 demonstrates the *mechanism*. The coefficients are authored
    placeholders, not validated against measured ground truth.
  - "Operating hours" is observed-time. "Track wear" is distance × an
    authored kilometres-of-life constant. "Barrel life" is rounds ÷ an
    authored rounds-of-life constant — and waits on Fire/Detonation PDU
    ingestion. "Suspension stress" is a trapezoidal terrain integral.
  - Every value emitted carries `Origin = ORIGIN_DERIVED`. The COP
    treatment of measured-vs-derived is a deferred post-Phase-5 pass.
"""
from __future__ import annotations

from typing import Optional

from openddil.telemetry.v1 import telemetry_pb2 as pb

from prognostics.accumulators import AccumulatorState, observed_hours
from prognostics.coefficients import WearCoefficients


# ---------------------------------------------------------------------------
# Phase 5 emit-side unit contract — committed at first deploy, per ADR-0013.
# ---------------------------------------------------------------------------
# `hours_in_service` / `cycles` carries the natural-unit raw measure:
#     track       -> "km"      (distance-driven)
#     engine      -> "h"       (observed-time stand-in)
#     barrel      -> "1"       (rounds — UCUM dimensionless count)
#     suspension  -> "deg.km"  (terrain integral)
#
# `remaining_useful_life` always carries percent-remaining (unit "%"). The
# `WearState.remaining_useful_life` proto comment already permits `"h" or "%"`;
# we pick `"%"` so it is the universal "how worn is this?" answer regardless
# of the underlying physics. Fusion's _eval_wear currently assumes time
# units and will skip distance-based wear; the small `if rul.unit == "%": ...`
# enhancement is tracked as a follow-on (COP-surface pass).
# ---------------------------------------------------------------------------


def _percent_remaining(consumed: float, total: float) -> float:
    """Clamped percent of life remaining, in [0.0, 100.0]."""
    if total <= 0.0:
        return 0.0
    frac = max(0.0, min(1.0, consumed / total))
    return 100.0 * (1.0 - frac)


# ---------------------------------------------------------------------------
# Individual wear models
# ---------------------------------------------------------------------------
def _derive_track_wear(
    state: AccumulatorState, coeffs: WearCoefficients,
) -> Optional[tuple[str, pb.WearState]]:
    """Distance-driven track wear.

    hours_in_service       ← cumulative distance (km, natural-unit raw measure)
    remaining_useful_life  ← percent remaining (%, universal answer)
    """
    if state.cumulative_distance_m <= 0.0:
        return None

    cumulative_km = state.cumulative_distance_m / 1000.0

    ws = pb.WearState()
    ws.hours_in_service.value = cumulative_km
    ws.hours_in_service.unit = "km"
    ws.remaining_useful_life.value = _percent_remaining(
        cumulative_km, coeffs.track_life_total_km,
    )
    ws.remaining_useful_life.unit = "%"
    ws.status = _wear_status(cumulative_km, coeffs.track_life_total_km)
    return ("track", ws)


def _derive_engine_hours(
    state: AccumulatorState, coeffs: WearCoefficients,
) -> Optional[tuple[str, pb.WearState]]:
    """Observed-time engine hours (Phase 5 stand-in for engine-on time).

    hours_in_service       ← observed hours (h, natural-unit raw measure)
    remaining_useful_life  ← percent remaining (%, universal answer)
    """
    hours = observed_hours(state)
    if hours <= 0.0:
        return None

    ws = pb.WearState()
    ws.hours_in_service.value = hours
    ws.hours_in_service.unit = "h"
    ws.remaining_useful_life.value = _percent_remaining(
        hours, coeffs.engine_life_total_hours,
    )
    ws.remaining_useful_life.unit = "%"
    ws.status = _wear_status(hours, coeffs.engine_life_total_hours)
    return ("engine", ws)


def _derive_barrel_life(
    state: AccumulatorState, coeffs: WearCoefficients,
) -> Optional[tuple[str, pb.WearState]]:
    """Rounds-through-tube barrel life.

    Phase 5: the model has the shape; the input feed (Fire/Detonation PDU
    ingestion → `record_round_fired`) is a follow-on wiring step per
    ADR-0020. While `state.rounds_fired == 0` (the current reality), the
    model returns None and emits nothing — honest absence.

    cycles                  ← rounds fired (UCUM "1", dimensionless count)
    remaining_useful_life   ← percent remaining (%, universal answer)
    """
    if state.rounds_fired <= 0:
        return None

    ws = pb.WearState()
    ws.cycles.value = float(state.rounds_fired)
    ws.cycles.unit = "1"
    ws.remaining_useful_life.value = _percent_remaining(
        float(state.rounds_fired), float(coeffs.barrel_life_total_rounds),
    )
    ws.remaining_useful_life.unit = "%"
    ws.status = _wear_status(
        float(state.rounds_fired), float(coeffs.barrel_life_total_rounds),
    )
    return ("barrel", ws)


def _derive_suspension(
    state: AccumulatorState, coeffs: WearCoefficients,
) -> Optional[tuple[str, pb.WearState]]:
    """Terrain-integral suspension stress.

    Integration kernel: ∫ (|pitch| + |roll|) ds, units deg·km. The model
    treats this cumulative integral as "consumed suspension life" against
    `terrain_total_deg_km`. The absolute scale is a placeholder; the
    *kernel* is the demonstrated mechanism.

    hours_in_service        ← terrain integral (deg.km, natural-unit raw measure)
    remaining_useful_life   ← percent remaining (%, universal answer)
    """
    if state.terrain_integral_deg_km <= 0.0:
        return None

    ws = pb.WearState()
    ws.hours_in_service.value = state.terrain_integral_deg_km
    ws.hours_in_service.unit = "deg.km"
    ws.remaining_useful_life.value = _percent_remaining(
        state.terrain_integral_deg_km, coeffs.terrain_total_deg_km,
    )
    ws.remaining_useful_life.unit = "%"
    ws.status = _wear_status(
        state.terrain_integral_deg_km, coeffs.terrain_total_deg_km,
    )
    return ("suspension", ws)


def _wear_status(consumed: float, total: float) -> int:
    """Map fraction-consumed to a WearStatus enum value."""
    if total <= 0:
        return pb.WEAR_UNSPECIFIED
    frac = consumed / total
    if frac >= 1.0:
        return pb.WEAR_CRITICAL
    if frac >= 0.9:
        return pb.WEAR_SERVICE_DUE
    if frac >= 0.5:
        return pb.WEAR_DEGRADED
    return pb.WEAR_NOMINAL


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------
_WEAR_MODELS = (
    _derive_track_wear,
    _derive_engine_hours,
    _derive_barrel_life,
    _derive_suspension,
)


def derive_sustainment(
    state: AccumulatorState, coeffs: WearCoefficients,
) -> pb.SustainmentMetrics:
    """Build a `SustainmentMetrics` from the per-asset accumulator state.

    Pure function. Empty `SustainmentMetrics` if no model produced output
    (insufficient data). Always stamps the message-level Phase 5
    provenance wildcard so a downstream consumer that reads
    `value_provenance['*']` sees the DERIVED marker honestly.
    """
    sustainment = pb.SustainmentMetrics()

    for fn in _WEAR_MODELS:
        result = fn(state, coeffs)
        if result is None:
            continue
        component_name, wear_state = result
        sustainment.wear.components[component_name].CopyFrom(wear_state)

    # Phase 5 message-level provenance: every value in this message is
    # DERIVED. The wildcard key is the message-level form documented in
    # telemetry.proto / ADR-0020. Per-field entries are forward-looking
    # shape for the validation phase (asset mixes MEASURED + DERIVED).
    sustainment.value_provenance["*"].origin = pb.ORIGIN_DERIVED
    # 0.0 is the ACCURATE value — there is no oracle, so there is nothing
    # to be 0.95 confident in. Real confidence arrives with the validation
    # phase (ADR-0020). Do not "default this to something reasonable" — a
    # placeholder masquerading as a real number is the failure mode this
    # whole framing exists to prevent.
    sustainment.value_provenance["*"].confidence = 0.0

    return sustainment
