# faust-regional

Per-region streaming aggregator. ADR-0023 Phase 6b §B.

One container per region. Composes multiple `faust.App` instances in a
single process via `faust.Worker(aggregator, *source_apps)`:

- **Source App(s)** — one per source edge. Bound to that edge's Kafka
  cluster (App.broker = edge URL). Consumes per-asset events
  (`asset-logistics-status`, `derived-sustainment`,
  `asset-telemetry-windows`), wraps each in a
  `openddil.regional.v1.RegionalAggregatorInput` envelope, produces the
  envelope to the per-region fan-in topic on `redpanda-hq` via a small
  `aiokafka.AIOKafkaProducer` sidecar service.
- **Aggregator App** — bound to `redpanda-hq`. Consumes the fan-in topic
  plus `asset-cm-state` (which `cm-service` produces directly to hq, per
  §A). Owns all RocksDB Tables (`region_<id>_assets_latest` — one row
  per asset, holding latest per-stream contribution). Emits three
  rolled-up topics on heartbeat (`region-fleet-summary`,
  `region-top-factors`, `region-wear-trends`).

## Worker composition coupling (read this before debugging restart loops)

**All Apps run in one Worker per region.** A crash in any one App takes
the whole region's aggregator down. This is **intentional** — Kafka
durability handles it, restart recovers from changelogs and consumer
offsets cleanly, and the single process gives one observability target
per region.

If you see `faust-regional-east restarted, all 3 Apps came back` in the
logs, that's by design — not "one App crashed and somehow took the
others with it." Process-level isolation between source Apps and the
aggregator App was deliberately not pursued; it would have meant 3
containers per region (2 sources + 1 aggregator), three restart
surfaces to monitor, three sets of consumer-group rebalance latencies
to debug. One-process-per-region is the cleaner failure-mode target,
and Kafka's durability guarantees mean a coupled crash is bounded —
restart, consume the gap, you're back.

## Why a sidecar `aiokafka` producer rather than pure Faust

Faust's `App.broker` is single-broker (verified at §B greenlight:
`app.topic()` does NOT accept a per-topic broker override). Source Apps
must consume from their assigned edge's cluster (where the per-asset
events live) but produce to hq (where the fan-in topic lives). The
sidecar producer is the small bounded hybrid that bridges the cluster
boundary on the produce side only. Inverting the hybrid (Faust on hq +
`aiokafka` consumers from each edge) collapses into the rejected Option
(c) and gives up Faust's rebalance/offset machinery on the harder side.

## Asymmetric coverage (§B greenlight call, not a defect)

`region-wear-trends` ships sourcing from `derived-sustainment` only
(live in 6a). The `asset-telemetry-windows` input path is **wired** in
the fan-in envelope and the aggregator's dispatcher, but does **NOT
drive emissions** in §B — the dispatcher logs at DEBUG and no-ops on
that envelope tag. Full-join verification (both inputs hot) waits for
follow-up #11's sustainment-data test fixtures, per the recipe-greenlit
sequencing decision. See the `asset_telemetry_windows` case in
`aggregator_app.py:_dispatch_envelope` and the §B-greenlight tracked
follow-up #11 in `openddil-demo/tests/hero_scenario_v3/README.md`.

## Configuration

| Env var | Required | Default | Notes |
|---|---|---|---|
| `OPENDDIL_REGION_ID` | yes | — | `region-east`, `region-west`, ... |
| `REGIONAL_EDGES` | yes | — | `"edge-01=redpanda-edge-01:9092,edge-02=redpanda-edge-02:9092"` |
| `REGIONAL_HQ_BROKERS` | no | `redpanda-hq:19092` | hq broker for fan-in + outputs |
| `REGIONAL_FAN_IN_TOPIC` | no | derived from `OPENDDIL_REGION_ID` | per-region; do NOT share across regions |
| `REGIONAL_HEARTBEAT_INTERVAL_S` | no | `30` | aggregator emit cadence |
| `REGIONAL_TOP_FACTORS_N` | no | `10` | top-N selection for `region-top-factors` |

## Topic configuration (set in the redpanda-init bootstrap, NOT here)

Fan-in topics are **pipeline internals**, not persistent state. They
must be created with these settings or restart-and-catch-up will
accumulate forever:

| Setting | Value | Why |
|---|---|---|
| `cleanup.policy` | `delete` | not compacted — multiple updates per asset per heartbeat interval |
| `retention.ms` | `3600000` (1h) | survives restart-and-catch-up, doesn't accumulate |
| `partitions` | `1` | matches the aggregator Tables partition invariant |
| `replication.factor` | `1` | single-broker demo; production would set per-cluster |

The three **output** topics (`region-fleet-summary`, `region-top-factors`,
`region-wear-trends`) are compacted-by-region-id and produced one row
per region per heartbeat — see `redpanda-init` for exact settings.
