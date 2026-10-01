"""Unit tests for ADR-0044 §3 — terminal operational-status partitions.

Covers both halves of the pipeline named in SPEC-rollup.md step 4:

  (a) a destroyed asset -> destroyed=1, not in any severity bucket,
      asset_count unchanged.
  (b) a later logistics update for that asset does not move it back
      into a bucket (sticky).
  (c) OPERATIONAL/UNSPECIFIED never reach the aggregator -- the
      source-App filter test (operates on source_app._wrap_and_forward_
      operational_claim directly, no Kafka/Faust runtime).
  (d) destroyed -> removed moves the count.

No Kafka, no Faust runtime, no Postgres. `_apply_*` and `_emit_class` in
aggregator_app are plain async functions over a dict-like Table; a
Faust Table's only behavior those functions rely on is "missing key
returns a fresh default", which _FakeTable below reproduces without an
App.

Run with: pytest regional/tests/test_operational_claim.py -v
"""
from __future__ import annotations

import asyncio

import pytest

from openddil.logistics.v1 import logistics_status_pb2 as lpb
from openddil.regional.v1 import regional_aggregator_input_pb2 as inp_pb
from openddil.telemetry.v1 import telemetry_pb2 as tpb

import aggregator_app as agg
import source_app as src


# ---------------------------------------------------------------------------
# Fake Faust Table -- dict with Faust's "missing key -> fresh default" shape
# ---------------------------------------------------------------------------
class _FakeTable(dict):
    def __missing__(self, key):
        return agg.AssetState()


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


def _terminal_envelope(asset_id: str, status: int, *, edge_id: str = "edge-01",
                        region_id: str = "region-east") -> inp_pb.RegionalAggregatorInput:
    ete = tpb.EntityTelemetryEvent()
    ete.asset.asset_id = asset_id
    ete.operational_state.operational_status = status
    env = inp_pb.RegionalAggregatorInput(
        source_edge_id=edge_id, region_id=region_id, asset_id=asset_id,
    )
    env.operational_claim.CopyFrom(ete)
    return env


def _logistics_envelope(asset_id: str, severity: int, *, edge_id: str = "edge-01",
                         region_id: str = "region-east") -> inp_pb.RegionalAggregatorInput:
    upd = lpb.AssetLogisticsStatusUpdate()
    upd.status.asset_id = asset_id
    upd.status.overall_severity = severity
    env = inp_pb.RegionalAggregatorInput(
        source_edge_id=edge_id, region_id=region_id, asset_id=asset_id,
    )
    env.logistics_status.CopyFrom(upd)
    return env


class _FakeOutTopic:
    """Stand-in for a Faust app.topic(...) handle -- records sends."""

    def __init__(self):
        self.sent: list[tuple[str, bytes]] = []

    async def send(self, *, key, value):
        self.sent.append((key, value))


# ---------------------------------------------------------------------------
# (a) destroyed -> destroyed=1, not in any bucket, asset_count unchanged
# ---------------------------------------------------------------------------
def test_destroyed_asset_counts_as_destroyed_not_a_bucket():
    table = _FakeTable()
    env = _terminal_envelope("asset-001", tpb.OPERATIONAL_STATUS_DESTROYED)
    _run(agg._apply_operational_claim(env, table))

    assert table["asset-001"].operational_status == "destroyed"

    out_fs, out_tf, out_wt = _FakeOutTopic(), _FakeOutTopic(), _FakeOutTopic()
    from google.protobuf.timestamp_pb2 import Timestamp
    now_ts = Timestamp()
    _run(agg._emit_class(
        region_id="region-east", cls="", snapshot=list(table.items()), now_ts=now_ts,
        out_fleet_summary=out_fs, out_top_factors=out_tf, out_wear_trends=out_wt,
    ))
    assert len(out_fs.sent) == 1
    fs_msg = inp_pb.RegionalAggregatorInput.__module__  # noqa: F841 (import sanity only)
    from openddil.regional.v1 import region_fleet_summary_pb2 as fs_pb
    msg = fs_pb.RegionFleetSummary()
    msg.ParseFromString(out_fs.sent[0][1])
    assert msg.destroyed == 1
    assert msg.deactivated == 0
    assert msg.removed == 0
    assert msg.nominal == 0
    assert msg.degraded == 0
    assert msg.critical == 0
    assert msg.non_operational == 0
    assert msg.asset_count == 1  # buckets (0) + terminal (1) -- unchanged partition total


# ---------------------------------------------------------------------------
# (b) a later logistics update does not move the asset back into a bucket
# ---------------------------------------------------------------------------
def test_sticky_destroyed_survives_later_logistics_update():
    table = _FakeTable()
    destroyed_env = _terminal_envelope("asset-001", tpb.OPERATIONAL_STATUS_DESTROYED)
    _run(agg._apply_operational_claim(destroyed_env, table))

    # A later, ordinary (non-terminal) logistics update for the same asset.
    logistics_env = _logistics_envelope("asset-001", severity=3)  # CRITICAL
    _run(agg._apply_logistics_status(logistics_env, table))

    # operational_status must still be "destroyed" -- untouched by the
    # logistics handler.
    assert table["asset-001"].operational_status == "destroyed"

    from google.protobuf.timestamp_pb2 import Timestamp
    out_fs, out_tf, out_wt = _FakeOutTopic(), _FakeOutTopic(), _FakeOutTopic()
    now_ts = Timestamp()
    _run(agg._emit_class(
        region_id="region-east", cls="", snapshot=list(table.items()), now_ts=now_ts,
        out_fleet_summary=out_fs, out_top_factors=out_tf, out_wear_trends=out_wt,
    ))
    from openddil.regional.v1 import region_fleet_summary_pb2 as fs_pb
    msg = fs_pb.RegionFleetSummary()
    msg.ParseFromString(out_fs.sent[0][1])
    assert msg.destroyed == 1
    assert msg.critical == 0  # must NOT have moved into the critical bucket
    assert msg.asset_count == 1


# ---------------------------------------------------------------------------
# (c) OPERATIONAL/UNSPECIFIED (and absent) never reach the aggregator --
#     the source-App filter.
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("status", [
    tpb.OPERATIONAL_STATUS_OPERATIONAL,
    tpb.OPERATIONAL_STATUS_UNSPECIFIED,
])
def test_source_app_drops_non_terminal_claims(status):
    ete = tpb.EntityTelemetryEvent()
    ete.asset.asset_id = "asset-002"
    ete.operational_state.operational_status = status
    raw = ete.SerializeToString()

    sent = []

    class _FakeProducer:
        async def send(self, topic, *, key, value):
            sent.append((topic, key, value))

    _run(src._wrap_and_forward_operational_claim(
        raw=raw, edge_id="edge-01", region_id="region-east",
        fan_in_topic="region-east-fan-in", producer=_FakeProducer(),
    ))
    assert sent == []


def test_source_app_drops_field_absent():
    # No operational_state set at all -- proto3 default is UNSPECIFIED (0).
    ete = tpb.EntityTelemetryEvent()
    ete.asset.asset_id = "asset-003"
    raw = ete.SerializeToString()

    sent = []

    class _FakeProducer:
        async def send(self, topic, *, key, value):
            sent.append((topic, key, value))

    _run(src._wrap_and_forward_operational_claim(
        raw=raw, edge_id="edge-01", region_id="region-east",
        fan_in_topic="region-east-fan-in", producer=_FakeProducer(),
    ))
    assert sent == []


def test_source_app_forwards_terminal_claim():
    ete = tpb.EntityTelemetryEvent()
    ete.asset.asset_id = "asset-004"
    ete.operational_state.operational_status = tpb.OPERATIONAL_STATUS_DESTROYED
    raw = ete.SerializeToString()

    sent = []

    class _FakeProducer:
        async def send(self, topic, *, key, value):
            sent.append((topic, key, value))

    _run(src._wrap_and_forward_operational_claim(
        raw=raw, edge_id="edge-01", region_id="region-east",
        fan_in_topic="region-east-fan-in", producer=_FakeProducer(),
    ))
    assert len(sent) == 1
    topic, key, value = sent[0]
    assert topic == "region-east-fan-in"
    assert key == "asset-004"
    env = inp_pb.RegionalAggregatorInput()
    env.ParseFromString(value)
    assert env.WhichOneof("payload") == "operational_claim"
    assert env.operational_claim.asset.asset_id == "asset-004"
    assert env.source_edge_id == "edge-01"
    assert env.region_id == "region-east"


# ---------------------------------------------------------------------------
# (d) destroyed -> removed moves the count
# ---------------------------------------------------------------------------
def test_later_terminal_claim_overwrites_destroyed_to_removed():
    table = _FakeTable()
    destroyed_env = _terminal_envelope("asset-005", tpb.OPERATIONAL_STATUS_DESTROYED)
    _run(agg._apply_operational_claim(destroyed_env, table))
    assert table["asset-005"].operational_status == "destroyed"

    removed_env = _terminal_envelope("asset-005", tpb.OPERATIONAL_STATUS_REMOVED)
    _run(agg._apply_operational_claim(removed_env, table))
    assert table["asset-005"].operational_status == "removed"

    from google.protobuf.timestamp_pb2 import Timestamp
    out_fs, out_tf, out_wt = _FakeOutTopic(), _FakeOutTopic(), _FakeOutTopic()
    now_ts = Timestamp()
    _run(agg._emit_class(
        region_id="region-east", cls="", snapshot=list(table.items()), now_ts=now_ts,
        out_fleet_summary=out_fs, out_top_factors=out_tf, out_wear_trends=out_wt,
    ))
    from openddil.regional.v1 import region_fleet_summary_pb2 as fs_pb
    msg = fs_pb.RegionFleetSummary()
    msg.ParseFromString(out_fs.sent[0][1])
    assert msg.destroyed == 0
    assert msg.removed == 1
    assert msg.asset_count == 1
