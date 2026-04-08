import faust
import json
import uuid
import datetime
import asyncpg
import logging
from typing import Dict, Any

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("tactical-agents")

# Faust App Initialization
app = faust.App(
    'tactical-agents-app',
    broker='kafka://localhost:9092',
    value_serializer='json',
)

# Define topics
raw_sensor_topic = app.topic('raw-sensor-stream', value_type=dict)
tactical_events_topic = app.topic('tactical-events', value_type=dict)

# Tumbling window table (10 seconds)
sensor_stats_table = app.Table(
    'sensor_stats',
    default=lambda: {'sum': 0.0, 'count': 0},
).tumbling(10.0, expires=datetime.timedelta(seconds=10))

CRITICAL_THRESHOLD = 85.0  # Example threshold for anomalies

def create_cloudevent(device_id: str, event_type: str, data: dict) -> dict:
    """Constructs a standard CloudEvents JSON envelope."""
    return {
        "id": str(uuid.uuid4()),
        "source": str(device_id),
        "type": event_type,
        "time": datetime.datetime.utcnow().isoformat() + "Z",
        "datacontenttype": "application/json",
        "data": data
    }

@app.agent(raw_sensor_topic)
async def process_sensor_data(stream):
    """
    Tumbling Window Agent:
    Reads from raw-sensor-stream, filters for temperature/telemetry events,
    groups by device_id, calculates a 10-second tumbling window average.
    If threshold is exceeded, publishes to tactical-events.
    """
    # Group the stream by device_id (source in CloudEvents)
    async for event in stream.group_by(lambda x: x.get('source')):
        event_type = event.get('type', '')
        
        # Filter relevant telemetry events
        if 'temperature' not in event_type and 'data' not in event_type:
            continue
            
        device_id = event.get('source')
        if not device_id:
            continue
            
        data = event.get('data', {})
        # Extract the sensor reading
        reading = data.get('temperature') or data.get('value')
        
        if reading is None:
            continue
            
        try:
            reading = float(reading)
        except ValueError:
            continue
            
        # Update tumbling window stats
        stats = sensor_stats_table[device_id].current()
        stats['sum'] += reading
        stats['count'] += 1
        sensor_stats_table[device_id] = stats
        
        # Calculate average in the current window
        avg_reading = stats['sum'] / stats['count']
        
        if avg_reading > CRITICAL_THRESHOLD:
            logger.warning(f"Anomaly detected for {device_id}! Avg: {avg_reading:.2f} > {CRITICAL_THRESHOLD}")
            
            alert_data = {
                "device_id": device_id,
                "average_reading": avg_reading,
                "threshold": CRITICAL_THRESHOLD,
                "severity": "HIGH"
            }
            
            # Construct CloudEvents envelope
            alert_event = create_cloudevent(
                device_id, 
                "openddil.tactical.ThresholdExceeded", 
                alert_data
            )
            
            # Publish to tactical-events topic
            await tactical_events_topic.send(key=device_id, value=alert_event)


# --- UI Read Projector (CQRS) ---

# Regional DB connection string (for local UI syncing via ElectricSQL)
DB_DSN = "postgresql://openddil:openddil@localhost:5432/regional_db"
db_pool = None

@app.on_startup
async def init_db_pool():
    global db_pool
    try:
        db_pool = await asyncpg.create_pool(dsn=DB_DSN)
        logger.info("Connected to Regional PostgreSQL database.")
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")

@app.on_shutdown
async def close_db_pool():
    if db_pool:
        await db_pool.close()
        logger.info("Closed Regional PostgreSQL database connection.")

@app.agent(tactical_events_topic)
async def project_to_ui(stream):
    """
    UI Read Projector:
    Reads from tactical-events, inserts events into local_tactical_alerts table.
    Used for local UI syncing via ElectricSQL (NOT HQ).
    """
    async for event in stream:
        event_type = event.get('type')
        
        # Only process threshold exceeded events for the UI
        if event_type != "openddil.tactical.ThresholdExceeded":
            continue
            
        event_id = event.get('id')
        device_id = event.get('source')
        data = event.get('data', {})
        severity = data.get('severity', 'UNKNOWN')
        detected_at_str = event.get('time')
        
        if not all([event_id, device_id, detected_at_str]):
            continue
            
        try:
            # Parse ISO-8601 string to datetime object
            detected_at = datetime.datetime.fromisoformat(detected_at_str.replace('Z', '+00:00'))
            
            if db_pool:
                async with db_pool.acquire() as conn:
                    await conn.execute(
                        """
                        INSERT INTO local_tactical_alerts (id, device_id, event_type, severity, detected_at)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (id) DO NOTHING
                        """,
                        event_id, device_id, event_type, severity, detected_at
                    )
                logger.info(f"Projected alert {event_id} for device {device_id} to local UI table.")
            else:
                logger.error("Database pool not initialized. Cannot project event.")
        except Exception as e:
            logger.error(f"Failed to project event to UI database: {e}", exc_info=True)

if __name__ == '__main__':
    app.main()
