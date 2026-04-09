# OpenDDIL Tactical Agents - Agents Documentation

## Role and Context
You are an AI agent working on the `openddil-tactical-agents` repository. This repository is part of Phases 7.2 and 7.3 of the OpenDDIL architecture. It handles autonomous pattern detection, long-running stateful workflows, and local UI read projection at the Regional Hub.

## Core Directives

1. **Faust Streaming Framework**:
   - Used for high-speed stream processing in `edge/edge_faust_detector.py`.
   - Maintain the use of `@app.agent` decorators for stream processing.
   - State management must use Faust's built-in `Table` capabilities (e.g., tumbling windows).

2. **Restate Durable Execution**:
   - Used for long-running, stateful workflows in `edge/edge_restate_detector.py` and exactly-once projections in `hub/hub_restate_projector.py`.
   - Workflows must use Restate Virtual Objects keyed by `device_id` or Services for stateless projections.
   - Use `ctx.sleep()` for durable timers and `ctx.run()` for side-effects (like publishing to Kafka or writing to Postgres).

3. **CloudEvents Standard**:
   - Any new events published to `tactical-events` MUST be wrapped in a valid CloudEvents JSON envelope.
   - Required fields: `id`, `source` (device_id), `type`, `time`, `datacontenttype`, `data`.

4. **CQRS & Database Rules**:
   - The UI Read Projector writes to a **Regional** PostgreSQL database, NOT the HQ database.
   - Use `asyncpg` for all database interactions.
   - Database operations MUST be idempotent (e.g., `ON CONFLICT DO NOTHING`).
   - **DO NOT** add any external HTTP webhook calls to the projector. The projection is strictly database-driven for ElectricSQL UI syncing.

5. **Resilience**:
   - Handle exceptions gracefully inside the agents to prevent the Faust worker from crashing on malformed events.
   - Restate agents are naturally resilient to container crashes and network drops.