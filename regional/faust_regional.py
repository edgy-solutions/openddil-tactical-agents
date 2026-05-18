"""faust-regional — per-region streaming aggregator (ADR-0023 Phase 6b §B).

ONE container per region. Composes multiple faust.App instances in a single
process via faust.Worker(primary, *secondaries):
  - Source App(s): one per source edge, bound to that edge's Kafka cluster.
                   Consume per-asset events, wrap in RegionalAggregatorInput
                   envelope, produce to the per-region fan-in topic on hq
                   via a sidecar aiokafka producer.
  - Aggregator App: bound to redpanda-hq. Consumes the fan-in topic + the
                   asset-cm-state topic. Owns all RocksDB Tables. Emits
                   three rolled-up topics on heartbeat.

Why this shape (captured here so future-debuggers see the rationale):
  - Multi-cluster consumption in one Python process is achievable in Faust
    only via faust.Worker(primary, *services) composing multiple Apps
    (verified by the §B greenlight spike — broker=[url1, url2] silently
    picks one cluster).
  - Source/aggregator split keeps Tables on ONE App (the aggregator),
    avoiding cross-App Table writes which Faust does not support. Source
    Apps stay stateless wrap-and-forward.
  - Cross-cluster produce from source Apps uses a small aiokafka sidecar;
    inverting the hybrid (Faust on hq + aiokafka consumers) gives up the
    Faust rebalance/offset machinery on the harder side.

Worker composition coupling (also documented in README.md): all Apps run
in one process. A crash in any one App takes the whole region's
aggregator down. This is INTENTIONAL — Kafka durability handles it,
restart recovers from changelogs / consumer offsets cleanly, and the
single process gives one observability target per region.

Env config:
  OPENDDIL_REGION_ID            — required, e.g. "region-east"
  REGIONAL_EDGES                — required, comma-separated
                                  "edge-01=redpanda-edge-01:9092,
                                   edge-02=redpanda-edge-02:9092"
  REGIONAL_HQ_BROKERS           — default "redpanda-hq:19092"
  REGIONAL_FAN_IN_TOPIC         — default "region-<region_id>-fan-in"
  REGIONAL_HEARTBEAT_INTERVAL_S — default "30"
  REGIONAL_TOP_FACTORS_N        — default "10"
"""
from __future__ import annotations

import logging
import os
import sys

import faust

from aggregator_app import make_aggregator_app
from source_app import make_cm_state_source_app, make_source_app


log = logging.getLogger("faust_regional.main")


def _parse_edges(spec: str) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for entry in spec.split(","):
        entry = entry.strip()
        if not entry:
            continue
        if "=" not in entry:
            raise RuntimeError(
                f"REGIONAL_EDGES entry {entry!r} must be 'edge_id=host:port'"
            )
        edge_id, brokers = entry.split("=", 1)
        out.append((edge_id.strip(), brokers.strip()))
    return out


def main() -> int:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        stream=sys.stdout,
    )

    region_id = os.environ["OPENDDIL_REGION_ID"]
    edges_spec = os.environ["REGIONAL_EDGES"]
    edges = _parse_edges(edges_spec)
    if not edges:
        raise RuntimeError("REGIONAL_EDGES parsed to zero entries — refusing to start")

    hq_brokers = os.getenv("REGIONAL_HQ_BROKERS", "redpanda-hq:19092")
    fan_in_topic = os.getenv(
        "REGIONAL_FAN_IN_TOPIC",
        f"region-{region_id.replace('region-', '')}-fan-in",
    )

    log.info("faust-regional region=%s edges=%s hq=%s fan_in=%s",
             region_id, [e[0] for e in edges], hq_brokers, fan_in_topic)

    # Aggregator on hq — PRIMARY app for the Worker (owns the web UI port).
    aggregator = make_aggregator_app(
        region_id=region_id,
        hq_brokers=hq_brokers,
        fan_in_topic=fan_in_topic,
        web_port=6066,
    )

    # Source App + sidecar per edge. faust.Worker takes ServiceT positional
    # args; both the source App and its sidecar HQ producer are services.
    services: list = []
    for i, (edge_id, edge_broker) in enumerate(edges):
        source_app, hq_producer = make_source_app(
            region_id=region_id,
            edge_id=edge_id,
            edge_broker_url=f"kafka://{edge_broker}",
            fan_in_topic=fan_in_topic,
            hq_brokers=hq_brokers,
            web_port=6067 + i,
        )
        services.append(source_app)
        services.append(hq_producer)

    # HQ cm-state source — routes asset-cm-state through the fan-in topic.
    # See aggregator_app.py docstring for the partition-invariant rationale.
    cm_state_source = make_cm_state_source_app(
        region_id=region_id,
        hq_brokers=hq_brokers,
        fan_in_topic=fan_in_topic,
        web_port=6067 + len(edges),
    )
    services.append(cm_state_source)

    worker = faust.Worker(aggregator, *services, loglevel="info")
    worker.execute_from_commandline()
    return 0


if __name__ == "__main__":
    sys.exit(main())
