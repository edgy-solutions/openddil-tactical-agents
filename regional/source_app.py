"""Source App factory for faust-regional.

ADR-0023 Phase 6b §B. One source App per (region, source_edge) pair. Each
source App is bound to its assigned edge's Kafka cluster (App.broker =
edge URL), consumes the four per-asset topics from that edge, wraps each
event in a RegionalAggregatorInput envelope, and produces the envelope to
the per-region fan-in topic on redpanda-hq via a sidecar aiokafka
producer.

Why a sidecar producer rather than pure Faust:
  Faust App is single-broker (verified — app.topic() does not accept a
  per-topic broker override). The source App's CONSUME side must point at
  the edge cluster (consumer-group coordination, rebalance, offset
  management — all the Faust strengths). The PRODUCE side must reach hq,
  a different cluster. aiokafka.AIOKafkaProducer is the small bounded
  hybrid that bridges the cluster boundary on the produce side only.
  Inverting the hybrid (Faust on hq + aiokafka consumers from each edge)
  collapses into the rejected Option (c) and gives up the Faust
  rebalance/offset machinery on the harder side.

Source Apps STAY STATELESS. No RocksDB Tables, no per-asset memory. Just
wrap-and-forward. All state lives on the aggregator App.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Optional

import aiokafka
import faust

from google.protobuf.timestamp_pb2 import Timestamp
from openddil.logistics.v1 import (
    logistics_status_pb2 as lpb,
    windowed_telemetry_pb2 as wpb,
)
from openddil.regional.v1 import regional_aggregator_input_pb2 as inp_pb
from openddil.telemetry.v1 import telemetry_pb2 as tpb

log = logging.getLogger("faust_regional.source")


# Source topics consumed from each edge cluster. The cm-state topic is
# different — it's on hq, not on the edge — so it's handled by the
# aggregator App directly, NOT by source Apps. See aggregator_app.py.
_EDGE_SOURCE_TOPICS = (
    "asset-logistics-status",     # AssetLogisticsStatusUpdate (proto)
    "derived-sustainment",        # EntityTelemetryEvent (proto)
    "asset-telemetry-windows",    # WindowedTelemetry (proto) — wired but
                                  # does NOT drive emissions in §B; see
                                  # ASYMMETRIC COVERAGE in faust_regional.py
)


def make_source_app(
    *,
    region_id: str,
    edge_id: str,
    edge_broker_url: str,
    fan_in_topic: str,
    hq_brokers: str,
    web_port: int,
) -> tuple[faust.App, "_HqProducerService"]:
    """Build a source App for one (region, edge) pair.

    Returns (app, hq_producer_service). The caller is responsible for
    composing the app + service into the same Worker as the aggregator App.
    """
    app_id = f"region-{region_id}-source-{edge_id}"
    app = faust.App(
        app_id,
        broker=edge_broker_url,
        store="memory://",
        value_serializer="raw",
        web_port=web_port,
    )

    # Sidecar producer — same lifecycle as the App (started/stopped via the
    # Service-class machinery). We wrap it in a faust.Service so it can be
    # passed alongside the App into faust.Worker(...).
    hq_producer = _HqProducerService(hq_brokers=hq_brokers, label=app_id)

    # One Faust agent per consumed edge topic; each agent unmarshals just
    # enough to extract asset_id (so the envelope can carry it for
    # partitioning), then wraps the raw bytes into the envelope's oneof
    # payload and hands the serialized envelope to the sidecar producer.

    @app.agent(app.topic("asset-logistics-status", value_type=bytes))
    async def on_logistics_status(stream):
        async for raw in stream:
            await _wrap_and_forward_logistics_status(
                raw=raw, edge_id=edge_id, region_id=region_id,
                fan_in_topic=fan_in_topic, producer=hq_producer,
            )

    @app.agent(app.topic("derived-sustainment", value_type=bytes))
    async def on_derived_sustainment(stream):
        async for raw in stream:
            await _wrap_and_forward_derived_sustainment(
                raw=raw, edge_id=edge_id, region_id=region_id,
                fan_in_topic=fan_in_topic, producer=hq_producer,
            )

    @app.agent(app.topic("asset-telemetry-windows", value_type=bytes))
    async def on_asset_telemetry_windows(stream):
        async for raw in stream:
            await _wrap_and_forward_windowed_telemetry(
                raw=raw, edge_id=edge_id, region_id=region_id,
                fan_in_topic=fan_in_topic, producer=hq_producer,
            )

    return app, hq_producer


# ---------------------------------------------------------------------------
# Per-topic wrap-and-forward helpers
# ---------------------------------------------------------------------------

def _now_timestamp() -> Timestamp:
    ts = Timestamp()
    ts.FromNanoseconds(int(time.time() * 1_000_000_000))
    return ts


async def _wrap_and_forward_logistics_status(
    *, raw: bytes, edge_id: str, region_id: str,
    fan_in_topic: str, producer: "_HqProducerService",
) -> None:
    if not raw:
        return
    upd = lpb.AssetLogisticsStatusUpdate()
    try:
        upd.ParseFromString(raw)
    except Exception as exc:
        log.warning("%s: bad AssetLogisticsStatusUpdate bytes (len=%d): %s",
                    edge_id, len(raw), exc)
        return
    asset_id = upd.status.asset_id or ""
    env = inp_pb.RegionalAggregatorInput(
        source_edge_id=edge_id, region_id=region_id,
        wrapped_at=_now_timestamp(), asset_id=asset_id,
    )
    env.logistics_status.CopyFrom(upd)
    await producer.send(fan_in_topic, key=asset_id, value=env.SerializeToString())


async def _wrap_and_forward_derived_sustainment(
    *, raw: bytes, edge_id: str, region_id: str,
    fan_in_topic: str, producer: "_HqProducerService",
) -> None:
    if not raw:
        return
    ete = tpb.EntityTelemetryEvent()
    try:
        ete.ParseFromString(raw)
    except Exception as exc:
        log.warning("%s: bad derived-sustainment EntityTelemetryEvent (len=%d): %s",
                    edge_id, len(raw), exc)
        return
    asset_id = ete.asset.asset_id or ""
    env = inp_pb.RegionalAggregatorInput(
        source_edge_id=edge_id, region_id=region_id,
        wrapped_at=_now_timestamp(), asset_id=asset_id,
    )
    env.derived_sustainment.CopyFrom(ete)
    await producer.send(fan_in_topic, key=asset_id, value=env.SerializeToString())


async def _wrap_and_forward_windowed_telemetry(
    *, raw: bytes, edge_id: str, region_id: str,
    fan_in_topic: str, producer: "_HqProducerService",
) -> None:
    if not raw:
        return
    wt = wpb.WindowedTelemetry()
    try:
        wt.ParseFromString(raw)
    except Exception as exc:
        log.warning("%s: bad WindowedTelemetry bytes (len=%d): %s",
                    edge_id, len(raw), exc)
        return
    asset_id = wt.asset_id or ""
    env = inp_pb.RegionalAggregatorInput(
        source_edge_id=edge_id, region_id=region_id,
        wrapped_at=_now_timestamp(), asset_id=asset_id,
    )
    env.asset_telemetry_windows.CopyFrom(wt)
    await producer.send(fan_in_topic, key=asset_id, value=env.SerializeToString())


# ---------------------------------------------------------------------------
# Sidecar HQ producer
# ---------------------------------------------------------------------------

# We use faust.Service rather than mode.Service to keep the import surface
# inside Faust's own namespace; faust.Service IS mode.Service re-exported.
class _HqProducerService(faust.Service):
    """Lifecycle-managed wrapper around an aiokafka.AIOKafkaProducer that
    targets the hq cluster. One instance per source App; faust.Worker
    starts/stops it alongside the App so a region-process restart cycles
    the producer cleanly."""

    def __init__(self, *, hq_brokers: str, label: str) -> None:
        super().__init__()
        self._hq_brokers = hq_brokers
        self._label = label
        self._producer: Optional[aiokafka.AIOKafkaProducer] = None

    async def on_start(self) -> None:
        self._producer = aiokafka.AIOKafkaProducer(
            bootstrap_servers=self._hq_brokers,
            client_id=f"{self._label}-hq-producer",
            acks="all",
        )
        await self._producer.start()
        log.info("%s: hq producer started (brokers=%s)",
                 self._label, self._hq_brokers)

    async def on_stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            log.info("%s: hq producer stopped", self._label)
        self._producer = None

    async def send(self, topic: str, *, key: str, value: bytes) -> None:
        if self._producer is None:
            raise RuntimeError(
                f"{self._label}: hq producer not started — "
                "the source App is producing before its sidecar is ready"
            )
        await self._producer.send_and_wait(
            topic, value=value,
            key=key.encode("utf-8") if isinstance(key, str) else key,
        )
