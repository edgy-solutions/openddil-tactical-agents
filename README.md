# OpenDDIL Tactical Agents (The Edge Brains)

This repository contains the autonomous agents that detect patterns in the `raw-sensor-stream` while disconnected from Central Command. 

**DO NOT add external database writes or HTTP Webhooks to these agents.** They must output exclusively to the `tactical-events` Redpanda topic to maintain DDIL resilience.

## Agent Architecture

We use two distinct processing frameworks, mapped to specific tactical needs:

### 1. Faust (`edge/edge_faust_detector.py` & `edge/faust_edge.py`)
* **Best for:** High-throughput, stateless stream processing and time-windowing.
* **Usage:** Tumbling averages and simple threshold detection. `faust_edge.py` specifically handles fast-twitch analytics for the "Hero" Demo scenario.

### 2. Restate (`edge/edge_restate_detector.py`, `hub/hub_restate_projector.py` & `hub/restate_hub.py`)
* **Best for:** Complex state machines, durable timers, and exactly-once execution that must survive container crashes and network drops.
* **Usage:** 
  * **Edge:** If a connection drops, `await ctx.sleep(45s)`. If the container reboots during those 45 seconds, Restate remembers the timer and wakes up exactly on time.
  * **Hub:** Projecting events into the local UI Read Model (CQRS) idempotently using `ctx.run` to guarantee exactly-once UI alerts.
  * **Demo:** `restate_hub.py` manages the durable Saga workflow for the "Hero" Demo scenario, updating local state and queueing ALCS API requests.

> **Note:** The demo orchestration, Toxiproxy network simulation, and React UIs are located in the `openddil-demo` repository.

## Dynamic Configuration

The tactical agents use a data-driven architecture powered by Pydantic. 
All topics, thresholds, and sensor configurations are defined in `config.yaml` and validated via `config.py`.
* Hardcoded thresholds and topics have been removed.
* Agents dynamically pull configuration based on the CloudEvent type they process.

## Faust Tables — the partition-count invariant (READ BEFORE ADDING A TABLE)

Every `app.Table(...)` declaration in the edge faust app **must use the same
`partitions=N` as the source topic the agents consume.** Today
`raw-sensor-stream` has 1 partition, so every Table declares `partitions=1`.

### Why the trap is invisible until it triggers

Faust requires `source_topic.partitions == changelog_topic.partitions` so
per-partition state stays co-located on the same consumer. **It enforces
this strictly only when an app has more than one Table.** With a single
Table, a mismatch can sit latent for arbitrarily long without crashing —
faust-edge ran 13 hours stable with `asset_state` declared `partitions=8`
against a 1-partition source. Adding the Phase 5 prognostics Table flipped
Faust into strict mode and both agents (the existing `process` and the new
`prognostics_process`) crash-looped with `PartitionsMismatch`. The bug had
been there the whole time; adding the second Table just made it visible.

### What to do when you add `app.Table(...)` #3 (or change the source's partitions)

- Set `partitions=` to match `raw-sensor-stream`'s current partition count.
  If you don't know it, run
  `docker compose exec redpanda-edge rpk topic describe raw-sensor-stream --print-partitions`.
- If you need higher parallelism, re-partition the **source topic** first
  and update **every** Table in the app in the same change. There is no
  per-Table partition count — they all align with the source or nothing
  works.
- If you change the partition count of an existing Table, the existing
  changelog topic on the broker will keep its old partition count and
  Faust will refuse to start. Delete the stale changelog
  (`rpk topic delete openddil-edge-<table-name>-changelog`) so Faust
  recreates it with the new count.

Both current Tables (`asset_state` in `edge/faust_edge.py` and
`prognostics_accumulators` in `edge/prognostics/agent.py`) point back to
this section in their inline comments.
