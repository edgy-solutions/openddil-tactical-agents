# OpenDDIL Tactical Agents

This repository contains the Faust-based autonomous agents, local read projectors, and Restate stateful workflows for the OpenDDIL architecture (Phases 7.2 & 7.3). These agents run at the Regional Hub to process tactical sensor data, detect anomalies, handle long-running stateful workflows, and project data for local UI offline capabilities.

## Architecture Overview

The application is built using two primary frameworks:
1. **[Faust](https://faust.readthedocs.io/)**: A stream processing library for Python, used for high-speed tumbling window calculations and CQRS local data projection.
2. **[Restate](https://restate.dev/)**: A durable execution framework used for long-running stateful workflows that must survive container crashes and network drops in a DDIL environment.

### Key Components

1. **The Tumbling Window Agent (`faust_agents.py`)**:
   - Subscribes to the `raw-sensor-stream` Kafka topic.
   - Filters for relevant CloudEvents (e.g., `openddil.sensor.temperature`).
   - Groups data by `device_id`.
   - Maintains a 10-second tumbling window to calculate average sensor readings.
   - If the average exceeds a critical threshold (e.g., 85.0), it constructs a new `openddil.tactical.ThresholdExceeded` CloudEvent and publishes it to the `tactical-events` topic.

2. **The UI Read Projector (CQRS) (`faust_agents.py`)**:
   - Subscribes to the `tactical-events` Kafka topic.
   - Connects to the **Regional PostgreSQL database** via `asyncpg`.
   - Inserts alerts into the `local_tactical_alerts` table using an idempotent `ON CONFLICT DO NOTHING` approach.
   - This database serves local UI syncing via ElectricSQL (completely isolated from HQ).

3. **Stateful Detection Agent (`restate_agents.py`)**:
   - Uses the official `restate-sdk` to implement a `SensorStateMonitor` Virtual Object.
   - The object key is the `device_id`.
   - Listens for `openddil.sensor.Connection_Lost` CloudEvents.
   - Uses Restate's durable execution sleep (`await ctx.sleep(...)`) to wait exactly 45 seconds (survives container reboots).
   - If no `Connection_Restored` event is received during the sleep window, it publishes a `openddil.tactical.DeviceOfflineAlert` CloudEvent to the `tactical-events` topic using a Kafka producer inside a side-effect context.

## Running the Application

### Prerequisites
- Python 3.9+
- Local Redpanda/Kafka broker running on `localhost:9092`
- Regional PostgreSQL database running on `localhost:5432` with a `local_tactical_alerts` table.
- Restate Server running locally (for `restate_agents.py`).

### Installation
```bash
pip install faust-streaming asyncpg restate-sdk confluent-kafka
```

### Execution
To start the Faust worker:
```bash
faust -A faust_agents worker -l info
```

To start the Restate agent (typically via a web framework like uvicorn):
```bash
# Example assuming integration with a web server
# uvicorn restate_agents:app
```
