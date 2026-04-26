import faust
import datetime
import logging
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("faust_edge")

app = faust.App(
    'faust_edge_app',
    broker='kafka://redpanda-edge:9092',
    value_serializer='json',
)

raw_sensor_topic = app.topic('raw-sensor-stream')
tactical_events_topic = app.topic('tactical-events')

# Tumbling window table
sensor_window = app.Table(
    'sensor_stats',
    default=lambda: {'sum_temp': 0.0, 'sum_pressure': 0.0, 'count': 0},
).tumbling(10.0, expires=datetime.timedelta(seconds=10.0))

@app.agent(raw_sensor_topic)
async def process_sensor_data(stream):
    async for event in stream.group_by(lambda x: x.get('device_id'), name='device_id_group'):
        device_id = event.get('device_id')
        if device_id != 'LTAMDS-04':
            continue
            
        core_temp = event.get('core_temp')
        coolant_pressure = event.get('coolant_pressure')
        
        if core_temp is None or coolant_pressure is None:
            continue
            
        stats = sensor_window[device_id].current()
        stats['sum_temp'] += core_temp
        stats['sum_pressure'] += coolant_pressure
        stats['count'] += 1
        sensor_window[device_id] = stats
        
        avg_temp = stats['sum_temp'] / stats['count']
        avg_pressure = stats['sum_pressure'] / stats['count']
        
        if avg_temp > 45.0 or avg_pressure < 90.0:
            logger.warning(f"CRITICAL ANOMALY DETECTED for {device_id}! Avg Temp: {avg_temp:.2f}, Avg Pressure: {avg_pressure:.2f}")
            
            payload = {
                "id": "generated-uuid", 
                "source": device_id,
                "type": "CriticalAnomaly",
                "time": datetime.datetime.utcnow().isoformat() + "Z",
                "datacontenttype": "application/json",
                "data": {
                    "avg_temp": avg_temp,
                    "avg_pressure": avg_pressure,
                    "severity": "CRITICAL"
                }
            }
            
            await tactical_events_topic.send(key=device_id.encode('utf-8'), value=payload)

if __name__ == '__main__':
    app.main()
