"""The remaining-life scoring skeleton (ADR-0059 §4.3): the join, the refusals, the synthetic
inputs, the metric stubs and the offline rule. Synthetic data only."""
import ast
import json
from pathlib import Path

import pytest

from scoring import rul
from scoring.rul import CONFIRMED, Failure, Prediction

ROOT = Path(__file__).resolve().parents[1]


def _p(cid="c1", reading=100.0, meter="operating_time", unit="h", q=None):
    return Prediction(cid, meter, unit, reading, 0, q if q is not None else {0.5: 50.0}, 50.0, "t1")


def _f(cid="c1", reading=300.0, closure=CONFIRMED, meter="operating_time", unit="h"):
    return Failure(cid, meter, unit, reading, closure)


def test_join_by_component_and_meter_reading_gives_the_actual_remaining_use():
    j = rul.join([_p("c1", 100.0), _p("c2", 40.0)], [_f("c1", 300.0), _f("c2", 90.5)])
    got = {x.prediction.component_id: x.actual_remaining for x in j.joined}
    assert got == {"c1": 200.0, "c2": 50.5}
    assert not j.censored and not j.meter_mismatch


def test_unconfirmed_closures_are_counted_by_reason_and_never_joined():
    j = rul.join([_p("c1", 100.0)], [_f("c1", 300.0, closure="declared-local"), _f("c2", 10.0, closure="declared-local")])
    assert j.joined == []
    assert j.unconfirmed == {"declared-local": 2}
    assert len(j.censored) == 1


def test_a_failure_on_another_meter_or_unit_is_refused_from_the_join():
    j = rul.join([_p("c1", 100.0), _p("c2", 100.0)],
                 [_f("c1", 300.0, meter="cycles", unit="{cycles}"), _f("c2", 300.0, unit="min")])
    assert j.joined == []
    assert [p.component_id for p in j.meter_mismatch] == ["c1", "c2"]


def test_a_prediction_after_the_last_failure_is_censored_not_joined():
    j = rul.join([_p("c1", 400.0)], [_f("c1", 300.0)])
    assert j.joined == [] and len(j.censored) == 1


def test_a_prediction_at_the_failure_reading_joins_with_zero_remaining():
    j = rul.join([_p("c1", 300.0)], [_f("c1", 300.0)])
    assert [x.actual_remaining for x in j.joined] == [0.0]


def test_a_repaired_component_scores_each_prediction_against_the_next_failure():
    j = rul.join([_p("c1", 100.0), _p("c1", 350.0)], [_f("c1", 900.0), _f("c1", 300.0)])
    got = sorted((x.prediction.meter_reading, x.failure.meter_reading) for x in j.joined)
    assert got == [(100.0, 300.0), (350.0, 900.0)]


def test_a_point_alone_is_refused():
    with pytest.raises(rul.RefusedRecord, match="never a point alone"):
        rul.join([_p(q={})], [_f()])
    with pytest.raises(rul.RefusedRecord, match="outside"):
        rul.join([_p(q={1.0: 5.0})], [_f()])


def test_synthetic_inputs_are_deterministic_per_seed():
    assert rul.synth_failures(7, 5) == rul.synth_failures(7, 5)
    assert rul.synth_failures(7, 5) != rul.synth_failures(8, 5)
    f = rul.synth_failures(7, 5)
    assert rul.synth_predictions(3, f) == rul.synth_predictions(3, f)
    assert all(x.component_id.startswith("synthetic-") for x in f)


def test_an_unbiased_synthetic_predictor_has_its_median_at_the_actual_remaining_use():
    f = rul.synth_failures(1, 20, unconfirmed_every=5)
    p = rul.synth_predictions(2, f, per_component=3)
    j = rul.join(p, f)
    assert len(j.joined) == 16 * 3
    assert j.unconfirmed == {"declared-local": 4}
    assert len(j.censored) == 4 * 3
    for x in j.joined:
        assert x.prediction.quantiles[0.5] == pytest.approx(x.actual_remaining)


def test_a_biased_synthetic_predictor_is_off_by_its_bias():
    f = rul.synth_failures(1, 3)
    j = rul.join(rul.synth_predictions(2, f, bias=0.2), f)
    for x in j.joined:
        assert x.prediction.quantiles[0.5] == pytest.approx(1.2 * x.actual_remaining)


@pytest.mark.parametrize("metric", rul.METRICS, ids=lambda m: m.__name__)
def test_each_metric_is_a_named_stub_that_cites_the_paper(metric):
    assert metric.stub is True
    assert "Metrics for Offline Evaluation of Prognostic Performance" in metric.__doc__
    with pytest.raises(NotImplementedError, match="Saxena"):
        metric([])


def test_the_five_metrics_are_the_ones_the_adr_names():
    assert [m.__name__ for m in rul.METRICS] == [
        "prognostic_horizon", "alpha_lambda_performance", "relative_accuracy",
        "cumulative_relative_accuracy", "convergence",
    ]


def test_the_report_counts_the_join_and_lists_every_metric_as_a_stub():
    f = rul.synth_failures(1, 4, unconfirmed_every=4)
    rep = rul.score(rul.synth_predictions(2, f, per_component=2), f)
    assert (rep["joined"], rep["censored"], rep["meter_mismatch"]) == (6, 2, 0)
    assert rep["unconfirmed"] == {"declared-local": 1}
    assert set(rep["metrics"]) == {m.__name__ for m in rul.METRICS}
    assert all(v == {"status": "stub", "cites": rul.SAXENA_2010} for v in rep["metrics"].values())


def _jsonl(path, rows):
    path.write_text("".join(json.dumps(r.__dict__) + "\n" for r in rows), encoding="utf-8")


def test_the_command_line_writes_only_its_report_and_refuses_to_overwrite(tmp_path):
    f = rul.synth_failures(1, 2)
    _jsonl(tmp_path / "f.jsonl", f)
    _jsonl(tmp_path / "p.jsonl", rul.synth_predictions(2, f, per_component=1))
    out = tmp_path / "report.json"
    args = ["--predictions", str(tmp_path / "p.jsonl"), "--failures", str(tmp_path / "f.jsonl"), "--out", str(out)]
    assert rul.main(args) == 0
    assert json.loads(out.read_text())["joined"] == 2
    assert sorted(p.name for p in tmp_path.iterdir()) == ["f.jsonl", "p.jsonl", "report.json"]
    assert rul.main(args) == 2


def test_a_malformed_record_is_refused_with_its_line(tmp_path, capsys):
    (tmp_path / "f.jsonl").write_text('{"component_id": "c1"}\n', encoding="utf-8")
    (tmp_path / "p.jsonl").write_text("", encoding="utf-8")
    rc = rul.main(["--predictions", str(tmp_path / "p.jsonl"), "--failures", str(tmp_path / "f.jsonl"),
                   "--out", str(tmp_path / "r.json")])
    assert rc == 2
    assert "f.jsonl:1" in capsys.readouterr().err
    assert not (tmp_path / "r.json").exists()


def test_the_harness_imports_nothing_that_reaches_a_live_store():
    allowed = {"__future__", "argparse", "json", "random", "sys", "dataclasses", "pathlib"}
    tree = ast.parse((ROOT / "scoring" / "rul.py").read_text(encoding="utf-8"))
    mods = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            mods |= {a.name.split(".")[0] for a in node.names}
        elif isinstance(node, ast.ImportFrom):
            mods.add((node.module or "").split(".")[0])
    assert mods <= allowed, mods - allowed
