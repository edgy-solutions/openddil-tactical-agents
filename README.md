# OpenDDIL Tactical Agents (The Edge Brains)

This repository contains the autonomous agents that detect patterns in the `raw-sensor-stream` while disconnected from Central Command. 

**DO NOT add external database writes or HTTP Webhooks to these agents.** They must output exclusively to the `tactical-events` Redpanda topic to maintain DDIL resilience.

## Agent Architecture

We use two distinct processing frameworks, mapped to specific tactical needs:

### 1. Faust (`edge/edge_faust_detector.py`)
* **Best for:** High-throughput, stateless stream processing and time-windowing.
* **Usage:** Tumbling averages and simple threshold detection.

### 2. Restate (`edge/edge_restate_detector.py` & `hub/hub_restate_projector.py`)
* **Best for:** Complex state machines, durable timers, and exactly-once execution that must survive container crashes and network drops.
* **Usage:** 
  * **Edge:** If a connection drops, `await ctx.sleep(45s)`. If the container reboots during those 45 seconds, Restate remembers the timer and wakes up exactly on time.
  * **Hub:** Projecting events into the local UI Read Model (CQRS) idempotently using `ctx.run` to guarantee exactly-once UI alerts.
