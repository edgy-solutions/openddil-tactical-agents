"""
Framework-agnostic anomaly detection.

Rules:
1. Functions take pint Quantity objects, not proto messages.
2. Functions are pure: (event_view, asset_state) -> Optional[Anomaly].
3. No I/O, no Faust types, no Kafka, no logging frameworks.
4. Convert to preferred unit INSIDE the function. Pint makes this near-free
   when the source already matches.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Callable
from pint import Quantity as PQ

@dataclass
class EventView:
    """Algorithm-friendly projection of EntityTelemetryEvent. Built by the
    framework wrapper; algorithms never see proto types directly."""
    asset_id: str
    sample_time_ns: int
    component_temp: PQ | None
    ambient_temp: PQ | None
    ground_speed: PQ | None
    fuel_remaining: PQ | None
    bus_voltage: PQ | None
    # ... extend as needed; keep it narrow so algorithms stay focused

@dataclass
class AssetState:
    """Rolling state per asset_id. Owned by the framework, mutated here."""
    last_temp_k: float | None = None
    temp_ewma_k: float | None = None
    temp_ewma_alpha: float = 0.2
    # ... per-algorithm rolling state lives here

@dataclass
class Anomaly:
    """Framework-agnostic anomaly. The wrapper layer turns this into a
    CloudEvent. Algorithms do not know what CloudEvents are."""
    rule_id: str            # stable identifier, e.g. "thermal.runaway.v1"
    severity: str           # "info" | "warning" | "critical"
    asset_id: str
    summary: str
    detected_at_ns: int
    evidence: dict          # JSON-safe scalars, units as strings

# -------------------------------------------------------------------------
# Algorithm: Thermal Runaway
# Native unit: Kelvin (avoids divide-by-zero and matches Arrhenius math)
# -------------------------------------------------------------------------
def thermal_runaway(ev: EventView, st: AssetState) -> Optional[Anomaly]:
    if ev.component_temp is None or ev.ambient_temp is None:
        return None
    comp_k = ev.component_temp.to("kelvin").magnitude
    amb_k = ev.ambient_temp.to("kelvin").magnitude

    # EWMA tracking
    if st.temp_ewma_k is None:
        st.temp_ewma_k = comp_k
    else:
        a = st.temp_ewma_alpha
        st.temp_ewma_k = a * comp_k + (1 - a) * st.temp_ewma_k

    delta = comp_k - amb_k
    rate = (comp_k - (st.last_temp_k or comp_k))
    st.last_temp_k = comp_k

    if delta > 80.0 and rate > 2.0:
        return Anomaly(
            rule_id="thermal.runaway.v1",
            severity="critical",
            asset_id=ev.asset_id,
            summary=f"Component {delta:.1f}K above ambient, rising {rate:.2f}K/sample",
            detected_at_ns=ev.sample_time_ns,
            evidence={
                "component_temp_k": comp_k,
                "ambient_temp_k": amb_k,
                "delta_k": delta,
                "rate_k_per_sample": rate,
            },
        )
    return None

# Add other algorithms following the same shape.

REGISTERED: list[Callable[[EventView, AssetState], Optional[Anomaly]]] = [
    thermal_runaway,
    # kinematic_anomaly,
    # fuel_starvation,
    # power_brownout,
]
