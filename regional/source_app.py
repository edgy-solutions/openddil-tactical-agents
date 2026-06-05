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

from asset_registry_cache import (
    AssetRegistryCache,
    resolve_region_from_cache,
)

log = logging.getLogger("faust_regional.source")


# Per-edge source App subscriptions. ONLY topics actually produced on
# the edge brokers go here. Per §A migrations, asset-cm-state and
# asset-logistics-status are produced to redpanda-hq (not per-edge), so
# they live on the hq source App (see make_hq_source_app below). The
# per-edge source App consumes:
#   - derived-sustainment (faust-edge prognostics produces here)
#   - asset-telemetry-windows (faust-edge windowing produces here; wired
#     but DEBUG no-op in aggregator per §B asymmetric coverage)
# A per-edge subscription on asset-logistics-status would consume nothing
# (fusion produces only to hq); dropping it avoids the spurious lag-zero
# consumer group on every edge broker.


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
# HQ source App — bound to hq broker, consumes the topics that are
# produced ON hq (not on per-edge brokers): asset-cm-state and
# asset-logistics-status per §A. Wraps each event into the envelope and
# produces to the fan-in topic on the SAME broker (no aiokafka sidecar
# needed — Faust's native producer handles same-broker produces).
#
# Why route through fan-in rather than letting the aggregator subscribe
# directly: asset-cm-state and asset-logistics-status both have
# partitions=8 (existing config; per-asset rows distributed). The
# aggregator Tables are partitions=1 to match the fan-in topic. Faust's
# PartitionsMismatch fires if an agent updates a Table from a
# differently-partitioned source. Wrap-and-republish gives the aggregator
# a single-partition view.
# ---------------------------------------------------------------------------

def make_hq_source_app(
    *,
    region_id: str,
    hq_brokers: str,
    fan_in_topic: str,
    web_port: int,
    asset_registry_topic: str = "asset-registry-events",
) -> faust.App:
    """Build the hq source App. Consumes asset-cm-state (JSON) and
    asset-logistics-status (proto) from hq; wraps each into the envelope
    and forwards to the per-region fan-in topic.

    Also subscribes to `asset_registry_topic` (ADR-0028) on the same
    hq broker to maintain an in-memory asset_id -> (edge_id, region_id)
    cache. cm-state and logistics-status events with empty region_id
    are resolved against this cache before the positive-match filter
    runs -- closes the rollup gap that prompted ADR-0028.
    """
    import json
    app_id = f"region-{region_id}-hq-source"
    app = faust.App(
        app_id,
        broker=f"kafka://{hq_brokers}",
        store="memory://",
        value_serializer="raw",
        web_port=web_port,
    )

    cm_in_topic = app.topic("asset-cm-state", value_type=bytes)
    log_in_topic = app.topic("asset-logistics-status", value_type=bytes)
    registry_in_topic = app.topic(asset_registry_topic, value_type=bytes)
    out_topic = app.topic(fan_in_topic, value_type=bytes)

    # ADR-0028 Phase 2: cache lives for the App's lifetime, shared
    # across the three agents below. Faust runs agents on the same
    # event loop within a Worker, so a plain dict is safe -- no lock
    # needed.
    registry_cache = AssetRegistryCache()

    @app.agent(registry_in_topic)
    async def on_asset_registry_event(stream):
        async for raw in stream:
            if not raw:
                continue
            try:
                event = json.loads(raw)
            except Exception as exc:
                log.warning("%s: bad asset-registry-events JSON (len=%d): %s",
                            app_id, len(raw), exc)
                continue
            asset_id = event.get("asset_id") or ""
            if not asset_id:
                continue
            edge_id_val = event.get("edge_id") or ""
            region_id_val = event.get("region_id") or ""
            if not region_id_val:
                # Registry deliberately emitted no region -- treat as
                # "no usable mapping yet". Don't overwrite an existing
                # cached entry with empty data.
                continue
            registry_cache.set(asset_id, edge_id_val, region_id_val)

    @app.agent(cm_in_topic)
    async def on_cm_state(stream):
        async for raw in stream:
            if not raw:
                continue
            # Filter to this region's events. cm-service produces all
            # regions' cm-state to one topic; the source App requires a
            # POSITIVE region match -- events with empty region_id are
            # resolved via the asset-registry cache (ADR-0028 Phase 2)
            # before being filtered.
            #
            # Bug history (caught by test_47): the earlier
            # `if event_region and event_region != region_id` filter
            # evaluated falsy on empty-string region_id and wrapped the
            # event to BOTH regions' fan-in topics, poisoning both
            # aggregator Tables. Positive-match closes this; the
            # registry-cache lookup keeps the filter strict while
            # backfilling the region_id cm-service can't compute itself.
            try:
                envelope = json.loads(raw)
            except Exception as exc:
                log.warning("%s: bad cm-state JSON (len=%d): %s",
                            app_id, len(raw), exc)
                continue
            asset_id = envelope.get("asset_id") or ""
            event_region = envelope.get("region_id") or ""
            if not event_region:
                if not asset_id:
                    continue  # no key to resolve against
                event_region = await resolve_region_from_cache(
                    registry_cache, asset_id, f"{app_id}/cm-state",
                ) or ""
            if event_region != region_id:
                continue
            env = inp_pb.RegionalAggregatorInput(
                source_edge_id=envelope.get("edge_id") or "",
                region_id=region_id,
                wrapped_at=_now_timestamp(),
                asset_id=asset_id,
                asset_cm_state_json=raw if isinstance(raw, (bytes, bytearray)) else bytes(raw),
            )
            await out_topic.send(key=asset_id, value=env.SerializeToString())

    @app.agent(log_in_topic)
    async def on_logistics_status(stream):
        async for raw in stream:
            if not raw:
                continue
            upd = lpb.AssetLogisticsStatusUpdate()
            try:
                upd.ParseFromString(raw)
            except Exception as exc:
                log.warning("%s: bad AssetLogisticsStatusUpdate (len=%d): %s",
                            app_id, len(raw), exc)
                continue
            # Filter to this region -- POSITIVE match (see on_cm_state
            # above for the bug-history note about empty-region_id
            # ambiguous routing). Empty region_id resolves via the
            # asset-registry cache (ADR-0028 Phase 2). logistics-fusion
            # currently emits empty provenance.region_id for every
            # event -- the cache lookup is the path that actually
            # delivers any logistics-status events to the aggregator.
            event_region = upd.provenance.region_id or ""
            asset_id = upd.status.asset_id or ""
            if not event_region:
                if not asset_id:
                    continue
                event_region = await resolve_region_from_cache(
                    registry_cache, asset_id, f"{app_id}/logistics-status",
                ) or ""
            if event_region != region_id:
                continue
            env = inp_pb.RegionalAggregatorInput(
                source_edge_id=upd.provenance.edge_id or "",
                region_id=region_id,
                wrapped_at=_now_timestamp(),
                asset_id=asset_id,
            )
            env.logistics_status.CopyFrom(upd)
            await out_topic.send(key=asset_id, value=env.SerializeToString())

    return app


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
