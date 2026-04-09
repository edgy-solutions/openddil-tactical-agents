import faust
import json
import uuid
import datetime
import logging
from typing import Dict, Any

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("edge-faust-detector")

# Faust App Initialization
app = faust.App(
    'edge-faust-detector-app',
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

if __name__ == '__main__':
    app.main()
