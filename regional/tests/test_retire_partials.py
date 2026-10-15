"""Retirement of emptied rollup partials (compacted topics, upsert key
includes the class). Offline: fake topics record sends; a dict stands in for
the emitted Table.

Run with: pytest regional/tests/test_retire_partials.py -v
"""
from __future__ import annotations

import asyncio

from openddil.regional.v1 import region_fleet_summary_pb2 as fs_pb
from openddil.regional.v1 import region_top_factors_pb2 as tf_pb
from openddil.regional.v1 import region_wear_trends_pb2 as wt_pb

import aggregator_app as agg

REGION = "region-east"


class _Topic:
    def __init__(self):
        self.sent = []

    async def send(self, key, value):
        self.sent.append((key, value))


class _Table(dict):
    def __missing__(self, key):
        return agg.AssetState()


def _run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


def _state(releasable=(), factors=None, wear=None):
    return agg.AssetState(
        releasable_to=list(releasable),
        logistics_factors=factors or [],
        sustainment_wear=wear or {},
    )


class _Rig:
    def __init__(self):
        self.assets = _Table()
        self.emitted = {}
        self.fleet, self.factors, self.wear = _Topic(), _Topic(), _Topic()

    def sweep(self):
        for t in (self.fleet, self.factors, self.wear):
            t.sent.clear()
        _run(agg._emit_rollups(
            region_id=REGION, assets_latest=self.assets, emitted=self.emitted,
            out_fleet_summary=self.fleet, out_top_factors=self.factors,
            out_wear_trends=self.wear))

    def fleet_msgs(self):
        out = {}
        for k, v in self.fleet.sent:
            m = fs_pb.RegionFleetSummary()
            m.ParseFromString(v)
            out[k] = m
        return out


def _assert_zero_fleet(m, releasable):
    assert m.region_id == REGION
    for f in ("nominal", "degraded", "critical", "non_operational",
              "destroyed", "deactivated", "removed", "asset_count"):
        assert getattr(m, f) == 0, f
    assert list(m.provenance.releasable_to) == releasable
    assert m.provenance.originator_nation == ""


def test_sweep1_emits_both_classes_and_records_them():
    r = _Rig()
    r.assets["a1"] = _state()
    r.assets["a2"] = _state(["ATL"])
    r.sweep()
    assert set(r.fleet_msgs()) == {REGION + "|", REGION + "|ATL"}
    assert {"fleet|", "fleet|ATL"} <= set(r.emitted)
    assert not r.factors.sent and not r.wear.sent
    assert set(r.emitted) == {"fleet|", "fleet|ATL"}


def test_relabel_retires_old_class_once():
    r = _Rig()
    r.assets["a1"] = _state()
    r.assets["a2"] = _state(["ATL"])
    r.sweep()
    r.assets["a1"] = _state(["ATL"])
    r.sweep()
    msgs = r.fleet_msgs()
    _assert_zero_fleet(msgs[REGION + "|"], [])
    assert msgs[REGION + "|ATL"].asset_count == 2
    assert "fleet|" not in r.emitted and "fleet|ATL" in r.emitted
    r.sweep()
    assert set(r.fleet_msgs()) == {REGION + "|ATL"}


def test_factors_going_empty_retires_factors_only():
    r = _Rig()
    r.assets["a1"] = _state(["ATL"], factors=[("F1", "HIGH")])
    r.sweep()
    assert "factors|ATL" in r.emitted and len(r.factors.sent) == 1
    r.assets["a1"] = _state(["ATL"])
    r.sweep()
    assert len(r.factors.sent) == 1
    key, raw = r.factors.sent[0]
    m = tf_pb.RegionTopFactors()
    m.ParseFromString(raw)
    assert key == REGION + "|ATL" and len(m.factors) == 0
    assert list(m.provenance.releasable_to) == ["ATL"]
    assert "factors|ATL" not in r.emitted


def test_wear_going_empty_retires_wear():
    r = _Rig()
    r.assets["a1"] = _state(["ATL"], wear={"c": {"unit": "h", "rul": 5.0}})
    r.sweep()
    assert "wear|ATL" in r.emitted
    r.assets["a1"] = _state(["ATL"])
    r.sweep()
    m = wt_pb.RegionWearTrends()
    m.ParseFromString(r.wear.sent[0][1])
    assert len(m.components) == 0


def test_empty_snapshot_retires_everything_then_cold_start_silent():
    r = _Rig()
    r.assets["a1"] = _state(["ATL"], factors=[("F1", "HIGH")])
    r.sweep()
    assert len(r.emitted) == 2
    r.assets.clear()
    r.sweep()
    assert len(r.fleet.sent) == 1 and len(r.factors.sent) == 1
    assert not r.emitted
    r.sweep()
    assert not r.fleet.sent and not r.factors.sent and not r.wear.sent


def test_multi_nation_class_retirement_label():
    r = _Rig()
    r.assets["a1"] = _state(["BDR", "ATL"])
    r.sweep()
    assert "fleet|ATL,BDR" in r.emitted
    r.assets["a1"] = _state(["ATL"])
    r.sweep()
    _assert_zero_fleet(r.fleet_msgs()[REGION + "|ATL,BDR"], ["ATL", "BDR"])
