"""
Pure-Python rolling-window math for the edge Faust agent.

This module knows NOTHING about Faust, Kafka, Restate, or RabbitMQ. The
Faust agent buffers events and calls these functions; output protos are
constructed here and the agent ships them.

Per ADR-0006/0013: algorithm boundary stays clean. Inputs and outputs are
either pint Quantity objects or Protobuf messages (no faust.Record).

Window data shape:
  per (asset_id, signal_key) -> deque[(epoch_ns, pint Quantity)]

The agent owns the deques (Faust Table); this module operates over the
deque contents that the agent passes in.
"""
from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Iterable, Sequence

from pint import UnitRegistry, Quantity as PQ
from google.protobuf import timestamp_pb2, duration_pb2

from openddil.common.v1 import quantity_pb2 as qpb
from openddil.logistics.v1 import windowed_telemetry_pb2 as win

# Shared UCUM-to-pint dialect — kept in sync with detection.units.
_UCUM_TO_PINT = {
    "[degF]": "degF",
    "[kn_i]": "knot",
    "gal_us": "gallon",
    "1":      "dimensionless",
}

_ureg = UnitRegistry()


@dataclass(frozen=True)
class Sample:
    """One observation: time in nanoseconds + quantity carrying its unit."""
    epoch_ns: int
    quantity: PQ


@dataclass(frozen=True)
class TrendResult:
    """Linear-regression slope + latest value over a window."""
    latest: PQ
    slope: PQ              # value-unit / time-unit (we always emit per-hour)
    r_squared: float
    sample_count: int
    window_start_ns: int
    window_end_ns: int
    latest_sample_ns: int


# ---------------------------------------------------------------------------
# Linear regression on (time, value)
# ---------------------------------------------------------------------------
def _filter_window(samples: Sequence[Sample],
                    *, window_ns: int,
                    now_ns: int) -> list[Sample]:
    """Keep only samples inside the [now-window, now] interval, time-sorted."""
    cutoff = now_ns - window_ns
    filtered = [s for s in samples if cutoff <= s.epoch_ns <= now_ns]
    return sorted(filtered, key=lambda s: s.epoch_ns)


def _slope_per_hour(samples: list[Sample]) -> tuple[float, float, PQ]:
    """Run ordinary least-squares regression on (time_h, value).

    Returns (slope_value_per_hour, r_squared, slope_quantity_with_units).
    All samples must share a single unit (they always do because the agent
    keys deques by signal name).
    """
    assert len(samples) >= 2, "regression requires >= 2 samples"
    # Convert times to hours relative to the earliest sample so the X axis
    # has a sane scale (no overflow / no precision loss).
    t0_ns = samples[0].epoch_ns
    xs = [(s.epoch_ns - t0_ns) / 3.6e12 for s in samples]  # hours
    ys = [float(s.quantity.magnitude) for s in samples]

    n = len(xs)
    sx, sy = sum(xs), sum(ys)
    mean_x = sx / n
    mean_y = sy / n

    cov = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    var_x = sum((x - mean_x) ** 2 for x in xs)
    var_y = sum((y - mean_y) ** 2 for y in ys)

    if var_x == 0:
        return 0.0, 0.0, _ureg.Quantity(0.0, str(samples[0].quantity.units) + "/h")

    slope = cov / var_x
    r2 = (cov ** 2) / (var_x * var_y) if var_y > 0 else 0.0
    # Slope unit: value_unit / hour.
    slope_unit = str(samples[0].quantity.units) + "/h"
    return slope, max(0.0, min(1.0, r2)), _ureg.Quantity(slope, slope_unit)


def compute_trend(samples: Sequence[Sample],
                   *, window_ns: int,
                   now_ns: int) -> TrendResult | None:
    """Compute latest + linear slope (per hour) over the rolling window.

    Returns None if there aren't enough samples for a meaningful fit
    (fewer than 2). Callers should treat None as "skip emitting this trend
    until more data arrives" — no fabricated slopes.
    """
    in_window = _filter_window(samples, window_ns=window_ns, now_ns=now_ns)
    if len(in_window) < 2:
        return None

    slope_val, r2, slope_q = _slope_per_hour(in_window)
    latest = in_window[-1]
    return TrendResult(
        latest=latest.quantity,
        slope=slope_q,
        r_squared=r2,
        sample_count=len(in_window),
        window_start_ns=in_window[0].epoch_ns,
        window_end_ns=now_ns,
        latest_sample_ns=latest.epoch_ns,
    )


# ---------------------------------------------------------------------------
# Pint <-> Protobuf bridges (windows_pb2.ScalarTrend specifically)
# ---------------------------------------------------------------------------
def _quantity_to_proto(pq: PQ, ucum_unit: str | None = None) -> qpb.Quantity:
    """Pint Quantity -> proto Quantity. If `ucum_unit` is provided, convert
    to that unit first; otherwise emit in whatever unit pint is carrying.

    The trend output should keep the source's native unit on the wire — we
    do NOT canonicalize here (downstream is welcome to convert lazily, per
    ADR-0013).
    """
    if ucum_unit is None:
        # Pint unit -> UCUM-ish string. We don't have a full pint->UCUM
        # mapper, so emit the pint short form. Downstream (rules.py) goes
        # back to pint anyway via the same UCUM_TO_PINT dialect, so the
        # round-trip preserves semantic identity even if the string is
        # pint-flavored rather than strict UCUM.
        return qpb.Quantity(value=float(pq.magnitude), unit=str(pq.units))
    converted = pq.to(ucum_unit)
    return qpb.Quantity(value=float(converted.magnitude), unit=ucum_unit)


def trend_to_proto(trend: TrendResult,
                    *,
                    latest_unit_hint: str | None = None) -> win.ScalarTrend:
    """Project a TrendResult into the wire shape."""
    out = win.ScalarTrend()
    out.latest.CopyFrom(_quantity_to_proto(trend.latest, latest_unit_hint))
    out.slope.CopyFrom(_quantity_to_proto(trend.slope))
    out.r_squared = trend.r_squared
    out.latest_sample_at.FromNanoseconds(trend.latest_sample_ns)
    return out


def build_window_spec(*, window_start_ns: int,
                       window_end_ns: int,
                       sample_count: int) -> win.WindowSpec:
    spec = win.WindowSpec()
    spec.sample_count = sample_count
    spec.window_start.FromNanoseconds(window_start_ns)
    spec.window_end.FromNanoseconds(window_end_ns)
    dur = duration_pb2.Duration()
    dur.FromNanoseconds(max(0, window_end_ns - window_start_ns))
    spec.duration.CopyFrom(dur)
    return spec


# ---------------------------------------------------------------------------
# Convenience: extract a pint Quantity from a proto Quantity, applying the
# UCUM dialect. Mirrors detection.units.from_proto so the agent doesn't
# have to import two modules.
# ---------------------------------------------------------------------------
def proto_to_pint(q: qpb.Quantity) -> PQ | None:
    if q is None or (q.value == 0.0 and not q.unit):
        return None
    unit = _UCUM_TO_PINT.get(q.unit, q.unit)
    try:
        return _ureg.Quantity(q.value, unit)
    except Exception:  # noqa: BLE001
        return None
