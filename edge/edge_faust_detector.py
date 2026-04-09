import faust
import json
import uuid
import datetime
import logging
import sys
import os
from typing import Dict, Any

# Add parent directory to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
import cloud_event_pb2
import tactical_events_pb2
from faust.serializers import codecs as faust_codecs

class CloudEventProtobufSerializer(faust_codecs.Codec):
    def _dumps(self, obj) -> bytes:
        return obj.SerializeToString()
    
    def _loads(self, s: bytes):
        ce = cloud_event_pb2.CloudEvent()
        ce.ParseFromString(s)
        return ce

faust_codecs.register('protobuf', CloudEventProtobufSerializer())

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("edge-faust-detector")

settings = config.load_config(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config.yaml"))

# Faust App Initialization
app = faust.App(
    'edge-faust-detector-app',
    broker=f'kafka://{settings.kafka.brokers}',
    value_serializer='protobuf',
)

# Define topics
raw_sensor_topic = app.topic(settings.kafka.raw_topic, value_type=cloud_event_pb2.CloudEvent)
tactical_events_topic = app.topic(settings.kafka.tactical_topic, value_type=cloud_event_pb2.CloudEvent)

# Determine default tumbling window from config
faust_sensors = [s for s in settings.sensors if s.processing.engine == 'faust']
default_window = float(faust_sensors[0].processing.window_seconds) if faust_sensors else 10.0

# Tumbling window table
sensor_stats_table = app.Table(
    'sensor_stats',
    default=lambda: {'sum': 0.0, 'count': 0},
).tumbling(default_window, expires=datetime.timedelta(seconds=default_window))

def create_cloudevent(device_id: str, event_type: str, data_pb) -> cloud_event_pb2.CloudEvent:
    """Constructs a standard CloudEvents Protobuf envelope."""
    ce = cloud_event_pb2.CloudEvent()
    ce.id = str(uuid.uuid4())
    ce.source = str(device_id)
    ce.type = event_type
    ce.time = datetime.datetime.utcnow().isoformat() + "Z"
    ce.datacontenttype = "application/protobuf"
    ce.data.Pack(data_pb)
    return ce

@app.agent(raw_sensor_topic)
async def process_sensor_data(stream):
    """
    Tumbling Window Agent:
    Reads from raw-sensor-stream, filters for temperature/telemetry events,
    groups by device_id, calculates a tumbling window average.
    If threshold is exceeded, publishes to tactical-events.
    """
    # Group the stream by device_id (source in CloudEvents)
    async for event in stream.group_by(lambda x: x.source):
        event_type = event.type
        
        # Dynamically pull config based on CloudEvent type
        sensor_config = next((s for s in settings.sensors if s.cloudevent_type == event_type), None)
        
        # Filter relevant telemetry events that should be processed by Faust
        if not sensor_config or sensor_config.processing.engine != 'faust':
            continue
            
        critical_threshold = sensor_config.processing.critical_threshold
            
        device_id = event.source
        if not device_id:
            continue
            
        try:
            # Assuming event.data is an Any containing JSON bytes
            data = json.loads(event.data.value.decode('utf-8'))
        except Exception:
            try:
                # Fallback if it's just bytes
                data = json.loads(event.data.decode('utf-8'))
            except Exception:
                continue
                
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
        
        if avg_reading > critical_threshold:
            logger.warning(f"Anomaly detected for {device_id}! Avg: {avg_reading:.2f} > {critical_threshold}")
            
            # Construct ThresholdExceededEvent protobuf
            alert_data = tactical_events_pb2.ThresholdExceededEvent()
            alert_data.device_id = device_id
            alert_data.average_reading = avg_reading
            alert_data.threshold = critical_threshold
            alert_data.severity = "HIGH"
            
            # Construct CloudEvents envelope
            alert_event = create_cloudevent(
                device_id, 
                "openddil.tactical.ThresholdExceeded", 
                alert_data
            )
            
            # Publish to tactical-events topic
            await tactical_events_topic.send(key=device_id.encode('utf-8'), value=alert_event)

if __name__ == '__main__':
    app.main()
