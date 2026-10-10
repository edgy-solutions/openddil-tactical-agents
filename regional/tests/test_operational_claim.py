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


# ---------------------------------------------------------------------------
# (e) `deactivated` is reversible: an appearance (a non-terminal record with
#     the asset's own kinematics) is forwarded by the source and clears the
#     aggregator's deactivated status. destroyed/removed are never cleared.
# ---------------------------------------------------------------------------
class _Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


class _CapturingProducer:
    def __init__(self):
        self.sent = []

    async def send(self, topic, *, key, value):
        self.sent.append((topic, key, value))


def _raw(asset_id, status=tpb.OPERATIONAL_STATUS_UNSPECIFIED, *, kinematics=True):
    ete = tpb.EntityTelemetryEvent()
    ete.asset.asset_id = asset_id
    ete.operational_state.operational_status = status
    if kinematics:
        ete.kinematics.SetInParent()
    return ete.SerializeToString()


def _forward(raw, producer, tracker):
    _run(src._wrap_and_forward_operational_claim(
        raw=raw, edge_id="edge-01", region_id="region-east",
        fan_in_topic="region-east-fan-in", producer=producer,
        appearances=tracker,
    ))


def test_source_forwards_appearance_after_deactivated_then_not_inside_interval():
    clock = _Clock()
    tracker = src._AppearanceTracker(60.0, clock=clock)
    producer = _CapturingProducer()
    # First sight of the asset: spend the interval-based forward on it so
    # the assertions below are about the deactivated trigger alone.
    _forward(_raw("asset-010"), producer, tracker)
    assert len(producer.sent) == 1

    _forward(_raw("asset-010", tpb.OPERATIONAL_STATUS_DEACTIVATED), producer, tracker)
    assert len(producer.sent) == 2  # the terminal claim, exactly as before

    clock.t += 1.0  # well inside the interval
    _forward(_raw("asset-010"), producer, tracker)
    assert len(producer.sent) == 3  # the appearance, forced by the claim
    env = inp_pb.RegionalAggregatorInput()
    env.ParseFromString(producer.sent[2][2])
    assert env.WhichOneof("payload") == "operational_claim"
    assert env.operational_claim.asset.asset_id == "asset-010"
    assert env.operational_claim.operational_state.operational_status == \
        tpb.OPERATIONAL_STATUS_UNSPECIFIED

    clock.t += 1.0
    _forward(_raw("asset-010"), producer, tracker)
    assert len(producer.sent) == 3  # not again inside the interval


def test_source_forwards_appearance_again_after_the_interval():
    clock = _Clock()
    tracker = src._AppearanceTracker(60.0, clock=clock)
    producer = _CapturingProducer()
    _forward(_raw("asset-011"), producer, tracker)
    assert len(producer.sent) == 1  # no appearance yet for this asset

    clock.t += 59.0
    _forward(_raw("asset-011"), producer, tracker)
    assert len(producer.sent) == 1

    clock.t += 2.0
    _forward(_raw("asset-011"), producer, tracker)
    assert len(producer.sent) == 2


def test_source_never_forwards_a_non_terminal_record_without_kinematics():
    clock = _Clock()
    tracker = src._AppearanceTracker(60.0, clock=clock)
    producer = _CapturingProducer()
    _forward(_raw("asset-012", tpb.OPERATIONAL_STATUS_DEACTIVATED), producer, tracker)
    assert len(producer.sent) == 1
    clock.t += 1000.0
    _forward(_raw("asset-012", kinematics=False), producer, tracker)
    _forward(_raw("asset-012", tpb.OPERATIONAL_STATUS_OPERATIONAL, kinematics=False),
             producer, tracker)
    assert len(producer.sent) == 1


def _appearance_envelope(asset_id, *, kinematics=True):
    ete = tpb.EntityTelemetryEvent()
    ete.asset.asset_id = asset_id
    if kinematics:
        ete.kinematics.SetInParent()
    env = inp_pb.RegionalAggregatorInput(
        source_edge_id="edge-02", region_id="region-east", asset_id=asset_id,
    )
    env.operational_claim.CopyFrom(ete)
    return env


def test_aggregator_appearance_revives_deactivated():
    table = _FakeTable()
    _run(agg._apply_operational_claim(
        _terminal_envelope("asset-020", tpb.OPERATIONAL_STATUS_DEACTIVATED), table))
    assert table["asset-020"].operational_status == "deactivated"

    _run(agg._apply_operational_claim(_appearance_envelope("asset-020"), table))
    assert table["asset-020"].operational_status == ""
    assert table["asset-020"].last_source_edge_id == "edge-02"

    from google.protobuf.timestamp_pb2 import Timestamp
    from openddil.regional.v1 import region_fleet_summary_pb2 as fs_pb
    out_fs, out_tf, out_wt = _FakeOutTopic(), _FakeOutTopic(), _FakeOutTopic()
    _run(agg._emit_class(
        region_id="region-east", cls="", snapshot=list(table.items()),
        now_ts=Timestamp(),
        out_fleet_summary=out_fs, out_top_factors=out_tf, out_wear_trends=out_wt,
    ))
    msg = fs_pb.RegionFleetSummary()
    msg.ParseFromString(out_fs.sent[0][1])
    assert msg.deactivated == 0


@pytest.mark.parametrize("status,name", [
    (tpb.OPERATIONAL_STATUS_DESTROYED, "destroyed"),
    (tpb.OPERATIONAL_STATUS_REMOVED, "removed"),
])
def test_aggregator_appearance_never_clears_destroyed_or_removed(status, name):
    table = _FakeTable()
    _run(agg._apply_operational_claim(_terminal_envelope("asset-021", status), table))
    _run(agg._apply_operational_claim(_appearance_envelope("asset-021"), table))
    assert table["asset-021"].operational_status == name


def test_aggregator_appearance_does_not_create_an_entry_for_an_unknown_asset():
    table = _FakeTable()
    _run(agg._apply_operational_claim(_appearance_envelope("asset-022"), table))
    assert "asset-022" not in table
    assert len(table) == 0


def test_aggregator_appearance_without_kinematics_does_not_revive():
    table = _FakeTable()
    _run(agg._apply_operational_claim(
        _terminal_envelope("asset-023", tpb.OPERATIONAL_STATUS_DEACTIVATED), table))
    _run(agg._apply_operational_claim(
        _appearance_envelope("asset-023", kinematics=False), table))
    assert table["asset-023"].operational_status == "deactivated"
