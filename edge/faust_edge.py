# =============================================================================
# ADR-0007: Dual-path streaming architecture.
# Migration target is Quix Streams for sub-10ms p99 latency and native
# Protobuf+Schema Registry support. Faust is current; Quix is planned.
# DO NOT leak Faust types (faust.Record, faust.Stream, App) into
# detection/units.py or the declared units.
# =============================================================================

"""
OpenDDIL Faust Edge Agent
-------------------------
Modernized anomaly detection for tactical telemetry.

DEPLOYMENT NOTE:
- Production: Uses `docker-compose.yml` which pulls pre-built images.
- Development: Uses `docker-compose.override.yml` to build from source and
               mount local code for hot-reloading.
"""
import logging
import os

import faust
from openddil.telemetry.v1 import telemetry_pb2 as pb
logger = logging.getLogger(__name__)
# Declared analytic units (ADR-0034, first slice; wear is the ADR-0020
# prognostics engine as one, thermal the runaway rule as another). `register(app)` below adds each enabled unit's
# own agent + Table; this is the *only* coupling. To extract the runner into
# its own service later, delete the import and the call site at the bottom of
# this file.
from algorithm_runtime.runner import register as register_algorithms

# ADR-0023 §Faust-edge: per-edge instance gets a distinct FAUST_APP_ID so
# each instance's consumer groups + changelog topics are namespaced per
# edge ("openddil-edge-01-prognostics_accumulators-changelog" etc.). The
# broker URL is built from KAFKA_BROKERS env so each instance points at its
# own broker. Defaults preserve single-tier behavior pre-6a.
FAUST_APP_ID  = os.getenv("FAUST_APP_ID",  "openddil-edge")
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "redpanda-edge:9092")

# Origin-node provenance env (ADR-0022 / ADR-0023). Phase 6a stamps these
# into emitted-event Provenance so 6b's coordinated handler upgrades
# inherit a populated field rather than having to backfill it then.
OPENDDIL_EDGE_ID   = os.getenv("OPENDDIL_EDGE_ID",   "edge-01")
OPENDDIL_REGION_ID = os.getenv("OPENDDIL_REGION_ID", "region-01")

app = faust.App(
    FAUST_APP_ID,
    broker=f"kafka://{KAFKA_BROKERS}",
    value_serializer="raw",
    # Element snapshots run to several MB per record; the per-partition fetch
    # default (1 MiB) is below that. Matches the element topic's
    # max.message.bytes so a record the broker accepts is one we can fetch.
    consumer_max_fetch_size=16 * 1024 * 1024,
)

events_topic = app.topic("tactical-events", value_type=bytes)
# Phase 3.5: rolling-window aggregations consumed by the logistics fusion
# service. Output records are openddil.telemetry.v1.WindowedTelemetry.
windows_topic = app.topic("asset-telemetry-windows", value_type=bytes)
# Rolling-window trends (algorithms/trends) and the element window
# (algorithms/element_rollup) are declared units; the runner keeps each
# asset's last trends payload and last element rollup, so nothing windowed
# lives here.

algorithm_runner = register_algorithms(app)

if __name__ == "__main__":
    app.main()
