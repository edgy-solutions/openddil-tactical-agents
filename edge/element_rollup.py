"""
Element-snapshot rollup derivation. A pure sibling module (same boundary
discipline as posture.py): it turns one decoded element-telemetry envelope
into the compact ElementRollup carried on a WindowedTelemetry, so the tiers
above the edge can show element health without ever receiving the
per-element payload.

No Faust, no Kafka, no I/O.
"""
from __future__ import annotations

from google.protobuf import json_format

from openddil.logistics.v1 import windowed_telemetry_pb2 as winpb

# These are the telemetry card's bands (openddil-demo frontend
# TelemetryCharts), kept identical on purpose: the edge card reading the
# elements and a region card reading this rollup must show the same numbers.
HEALTH_CRITICAL = 0.97   # health above this counts as critical
HEALTH_DEGRADED = 0.90   # health above this (and not critical) counts as degraded


def _is_num(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def rollup_from_envelope(env: dict) -> "winpb.ElementRollup | None":
    """Derive the rollup from a decoded envelope; None when it carries no
    elements (nothing to roll up)."""
    elements = env.get("elements")
    if not elements:
        return None

    critical = 0
    degraded = 0
    temps: list[float] = []
    loads: list[float] = []
    for el in elements:
        h = el.get("health")
        if _is_num(h):
            if h > HEALTH_CRITICAL:
                critical += 1
            elif h > HEALTH_DEGRADED:
                degraded += 1
        t = el.get("temp_c")
        if _is_num(t):
            temps.append(float(t))
        ld = el.get("load_pct")
        if _is_num(ld):
            loads.append(float(ld))

    out = winpb.ElementRollup()
    out.profile_name = env.get("profile_name") or ""
    out.element_count = len(elements)
    out.critical_count = critical
    out.degraded_count = degraded
    out.avg_temp_c = _mean(temps)
    out.avg_load_pct = _mean(loads)
    obs_ns = env.get("observed_at_ns")
    if _is_num(obs_ns):
        out.observed_at.FromNanoseconds(int(obs_ns))

    # Presence matters: an absent readout stays unset rather than becoming 0.
    op = env.get("operational") or {}
    if _is_num(op.get("core_temp_c")):
        out.core_temp_c = float(op["core_temp_c"])
    if _is_num(op.get("uptime_hours")):
        out.uptime_hours = float(op["uptime_hours"])

    # The condition that moved the snapshot (level, the sources at it, every
    # claim), copied as the producer wrote it so the uplink can say WHICH
    # source moved the asset, not only that its counts changed. Absent stays
    # absent; a malformed block is dropped rather than failing the rollup,
    # since the counts above are still true without it.
    cond = op.get("condition")
    if isinstance(cond, dict):
        try:
            json_format.ParseDict(cond, out.condition, ignore_unknown_fields=True)
        except json_format.ParseError:
            out.ClearField("condition")
    return out
