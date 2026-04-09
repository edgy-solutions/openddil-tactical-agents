import json
import uuid
import datetime
import logging
from datetime import timedelta
import restate
from restate.context import ObjectContext
from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("edge-restate-detector")

KAFKA_BROKERS = "localhost:9092"
TACTICAL_EVENTS_TOPIC = "tactical-events"

def get_kafka_producer():
    """Configure and return a resilient Kafka/Redpanda producer."""
    conf = {
        'bootstrap.servers': KAFKA_BROKERS,
        'client.id': 'restate-tactical-agent',
        'acks': 'all',  # Strongest durability
        'retries': 2147483647,  # Infinite retries for DDIL resilience
        'retry.backoff.ms': 1000,
        'message.timeout.ms': 300000,
        'delivery.timeout.ms': 300000
    }
    return Producer(conf)

# Global producer instance
producer = get_kafka_producer()

def delivery_report(err, msg):
    """Callback for message delivery reports."""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")

async def publish_to_kafka(device_id: str, payload: dict):
    """Side effect function to publish to Kafka."""
    producer.produce(
        topic=TACTICAL_EVENTS_TOPIC,
        key=str(device_id).encode('utf-8'),
        value=json.dumps(payload).encode('utf-8'),
        on_delivery=delivery_report
    )
    producer.poll(0)
    producer.flush()
    return True

# Define the Virtual Object
SensorStateMonitor = restate.VirtualObject("SensorStateMonitor")

@SensorStateMonitor.handler()
async def process_event(ctx: ObjectContext, cloudevent: dict):
    """
    Processes incoming CloudEvent payloads from raw-sensor-stream.
    The key for this Virtual Object is the device_id (CloudEvents source).
    """
    event_type = cloudevent.get("type")
    device_id = ctx.key()  # Key is device_id

    if event_type == "openddil.sensor.Connection_Restored":
        logger.info(f"Connection restored for device {device_id}")
        # Mark the connection as restored in durable state
        ctx.set("connection_status", "restored")
        return

    if event_type == "openddil.sensor.Connection_Lost":
        logger.info(f"Connection lost detected for device {device_id}. Starting 45s timer.")
        
        # Reset the connection status
        ctx.set("connection_status", "lost")
        
        # Use Restate's durable execution sleep. 
        # NOTE: This sleep survives container reboots and network drops in a DDIL environment.
        await ctx.sleep(timedelta(seconds=45))
        
        # After waking up, check local Restate state to see if Connection_Restored was received
        status = await ctx.get("connection_status")
        
        if status != "restored":
            logger.warning(f"Device {device_id} is still offline after 45s. Publishing alert.")
            
            # Construct DeviceOfflineAlert wrapped in a CloudEvents JSON envelope
            alert_data = {
                "device_id": device_id,
                "status": "OFFLINE",
                "timeout_seconds": 45,
                "severity": "CRITICAL"
            }
            
            alert_event = {
                "id": str(uuid.uuid4()),
                "source": device_id,
                "type": "openddil.tactical.DeviceOfflineAlert",
                "time": datetime.datetime.utcnow().isoformat() + "Z",
                "datacontenttype": "application/json",
                "data": alert_data
            }
            
            # Use Restate's side-effect context to publish this payload to Redpanda
            await ctx.run("publish_alert", publish_to_kafka, device_id, alert_event)
            
            # Clear the state
            ctx.clear("connection_status")
        else:
            logger.info(f"Device {device_id} reconnected within the 45s window. No alert sent.")
            ctx.clear("connection_status")
            
        return

    # Ignore irrelevant event types
    logger.debug(f"Ignoring irrelevant event type: {event_type}")
    return

app = restate.app([SensorStateMonitor])

if __name__ == '__main__':
    # Typically run via a web framework or restate CLI
    pass
