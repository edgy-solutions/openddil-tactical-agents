"""Remaining-useful-life scoring harness: a skeleton (ADR-0059 §4.3, proposed).

Offline only. It reads remaining-life outputs and confirmed failures from files, joins them, and
writes one report file. It never reads or writes a live store, and imports nothing that could.

What is built:
- the join: each remaining-life output, at its meter reading, to the component's next confirmed
  failure on the same meter; the actual remaining use is the difference of the two readings;
- synthetic failures and synthetic predictions, for tests;
- the report: join counts, and one entry per metric.

What is not:
- the metrics. Each is a named stub that cites the paper the ADR names and raises. The harness
  asserts no definition of its own; each is implemented from the paper's text when the spec lands.
- the field names. ADR-0059 §4.2 binds the remaining-life kind's fields when the kind is specified,
  and §4.4 binds no outcome field until iagent's architect answers. The names here are this
  harness's own, provisional, and say nothing about either kind.

usage: python -m scoring.rul --predictions P.jsonl --failures F.jsonl --out REPORT.json
"""
from __future__ import annotations

import argparse
import json
import random
import sys
from dataclasses import dataclass, field
from pathlib import Path

SAXENA_2010 = (
    "Saxena, Celaya, Saha, Saha and Goebel, 'Metrics for Offline Evaluation of Prognostic Performance', "
    "International Journal of Prognostics and Health Management, 2010"
)

# ADR-0059 §4.4: only a closure carried by the work order's outcome is confirmed. Every other
# closure reason is kept and counted under its own name, never joined.
CONFIRMED = "confirmed"


@dataclass(frozen=True)
class Prediction:
    """One remaining-life output (provisional field names; see the module docstring)."""
    component_id: str
    meter: str            # a declared usage meter (ADR-0051 §3)
    unit: str             # its UCUM unit
    meter_reading: float  # the reading the prediction was computed at
    computed_at_ns: int
    quantiles: dict       # probability -> remaining use, in `unit`; never a point alone (ADR-0059 §4.2)
    horizon: float        # in `unit`
    threshold_ref: str    # the failure threshold it was computed against, by reference
    producer: str = ""


@dataclass(frozen=True)
class Failure:
    """One closed failure episode (provisional field names)."""
    component_id: str
    meter: str
    unit: str
    meter_reading: float  # the reading at failure
    closure: str          # CONFIRMED, or a declared-local reason


@dataclass(frozen=True)
class Joined:
    prediction: Prediction
    failure: Failure

    @property
    def actual_remaining(self) -> float:
        return self.failure.meter_reading - self.prediction.meter_reading


@dataclass
class JoinResult:
    joined: list = field(default_factory=list)
    # Predictions with no confirmed failure at or after their reading: right-censored, not scored.
    censored: list = field(default_factory=list)
    # A confirmed failure exists for the component, but on another meter or unit.
    meter_mismatch: list = field(default_factory=list)
    # Failures not joined because their closure is not confirmed, counted by reason.
    unconfirmed: dict = field(default_factory=dict)


class RefusedRecord(ValueError):
    pass


def check_prediction(p: Prediction) -> None:
    if not p.quantiles:
        raise RefusedRecord(f"{p.component_id}: a remaining-life output carries a distribution, never a point alone")
    for q in p.quantiles:
        if not 0.0 < float(q) < 1.0:
            raise RefusedRecord(f"{p.component_id}: quantile probability {q} is outside (0, 1)")


def join(predictions, failures) -> JoinResult:
    """Join each prediction to the component's next confirmed failure on the same meter and unit.

    "Next" is the first confirmed failure whose reading is at or above the prediction's. A component
    repaired and failed again is scored against the failure its prediction came before.
    """
    out = JoinResult()
    confirmed: dict[str, list[Failure]] = {}
    for f in failures:
        if f.closure != CONFIRMED:
            out.unconfirmed[f.closure] = out.unconfirmed.get(f.closure, 0) + 1
            continue
        confirmed.setdefault(f.component_id, []).append(f)
    for fs in confirmed.values():
        fs.sort(key=lambda f: f.meter_reading)
    for p in predictions:
        check_prediction(p)
        fs = confirmed.get(p.component_id, [])
        same = [f for f in fs if f.meter == p.meter and f.unit == p.unit]
        if fs and not same:
            out.meter_mismatch.append(p)
            continue
        nxt = next((f for f in same if f.meter_reading >= p.meter_reading), None)
        if nxt is None:
            out.censored.append(p)
        else:
            out.joined.append(Joined(p, nxt))
    return out


# --- The metrics (Saxena et al. 2010). Stubs until the spec lands. ---------------------------------

def _stub(name: str):
    def metric(joined, **params):
        raise NotImplementedError(f"{name}: not implemented; to be built from the text of {SAXENA_2010}")
    metric.__name__ = name
    metric.__doc__ = f"{name.replace('_', ' ')}, as defined in {SAXENA_2010}. Stub: raises until the spec lands."
    metric.stub = True
    return metric


prognostic_horizon = _stub("prognostic_horizon")
alpha_lambda_performance = _stub("alpha_lambda_performance")
relative_accuracy = _stub("relative_accuracy")
cumulative_relative_accuracy = _stub("cumulative_relative_accuracy")
convergence = _stub("convergence")

METRICS = (
    prognostic_horizon,
    alpha_lambda_performance,
    relative_accuracy,
    cumulative_relative_accuracy,
    convergence,
)


def score(predictions, failures) -> dict:
    j = join(predictions, failures)
    metrics = {}
    for m in METRICS:
        try:
            metrics[m.__name__] = {"status": "ok", "value": m(j.joined)}
        except NotImplementedError:
            metrics[m.__name__] = {"status": "stub", "cites": SAXENA_2010}
    return {
        "joined": len(j.joined),
        "censored": len(j.censored),
        "meter_mismatch": len(j.meter_mismatch),
        "unconfirmed": dict(sorted(j.unconfirmed.items())),
        "metrics": metrics,
    }


# --- Synthetic inputs ----------------------------------------------------------------------------

def synth_failures(seed: int, n: int, *, meter="operating_time", unit="h", unconfirmed_every=0) -> list:
    """n synthetic components, each with one failure. Every `unconfirmed_every`-th one is closed
    declared-local instead of confirmed (0 means none)."""
    rng = random.Random(seed)
    out = []
    for i in range(n):
        closure = "declared-local" if unconfirmed_every and (i + 1) % unconfirmed_every == 0 else CONFIRMED
        out.append(Failure(f"synthetic-component-{i:04d}", meter, unit, round(rng.uniform(500.0, 5000.0), 3), closure))
    return out


def synth_predictions(seed: int, failures, *, per_component=4, bias=0.0, spread=0.1) -> list:
    """Predictions before each failure, at evenly spaced readings. The median is the true remaining
    use times (1 + bias); the 0.05 and 0.95 quantiles sit `spread` either side of it, proportionally."""
    rng = random.Random(seed)
    out = []
    for f in failures:
        for k in range(per_component):
            reading = round(f.meter_reading * (k + 1) / (per_component + 1), 3)
            true = f.meter_reading - reading
            med = true * (1.0 + bias)
            out.append(Prediction(
                component_id=f.component_id, meter=f.meter, unit=f.unit, meter_reading=reading,
                computed_at_ns=1_700_000_000_000_000_000 + int(reading * 3.6e12) + rng.randrange(1000),
                quantiles={0.05: med * (1.0 - spread), 0.5: med, 0.95: med * (1.0 + spread)},
                horizon=med, threshold_ref="synthetic-threshold-1", producer="synthetic",
            ))
    return out


# --- Files and the command line ------------------------------------------------------------------

def _read_jsonl(path: Path, cls):
    rows = []
    with path.open(encoding="utf-8") as fh:
        for n, line in enumerate(fh, 1):
            if not line.strip():
                continue
            d = json.loads(line)
            if cls is Prediction:
                d["quantiles"] = {float(k): float(v) for k, v in (d.get("quantiles") or {}).items()}
            try:
                rows.append(cls(**d))
            except TypeError as e:
                raise RefusedRecord(f"{path.name}:{n}: {e}") from None
    return rows


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Score remaining-life outputs against confirmed failures (offline).")
    ap.add_argument("--predictions", type=Path, required=True)
    ap.add_argument("--failures", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    a = ap.parse_args(argv)
    if a.out.exists():
        print(f"refused: {a.out} exists", file=sys.stderr)
        return 2
    try:
        report = score(_read_jsonl(a.predictions, Prediction), _read_jsonl(a.failures, Failure))
    except RefusedRecord as e:
        print(f"refused: {e}", file=sys.stderr)
        return 2
    a.out.write_text(json.dumps(report, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    print(f"joined {report['joined']} censored {report['censored']} meter_mismatch {report['meter_mismatch']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
