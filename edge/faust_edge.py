import faust
import datetime
import logging
import json
import uuid

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

recent_alerts = []
active_anomalies = {}
latest_telemetry = {"thermal": 32.0, "pressure": 120.0}

@app.page('/alerts')
class Alerts(faust.web.View):
    async def get(self, request):
        alerts = []
        for device_id, anomaly in active_anomalies.items():
            alerts.append({
                "id": anomaly["id"],
                "msg": f"CRITICAL: Thermal Runaway Predicted. Avg Temp: {anomaly['temp']:.2f}",
                "type": "crit",
                "time": anomaly["time"]
            })
            alerts.append({
                "id": anomaly["id"] + "-action",
                "msg": f"ACTION: Power Throttled via Restate Agent",
                "type": "warn",
                "time": anomaly["time"]
            })
        return self.json(alerts)

@app.page('/telemetry')
class Telemetry(faust.web.View):
    async def get(self, request):
        return self.json(latest_telemetry)

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
        
        latest_telemetry["thermal"] = avg_temp
        latest_telemetry["pressure"] = avg_pressure
        
        is_anomalous = avg_temp > 45.0 or avg_pressure < 90.0
        was_anomalous = device_id in active_anomalies
        
        if is_anomalous and not was_anomalous:
            logger.warning(f"CRITICAL ANOMALY DETECTED for {device_id}! Avg Temp: {avg_temp:.2f}, Avg Pressure: {avg_pressure:.2f}")
            
            anomaly_id = str(uuid.uuid4())
            payload = {
                "id": anomaly_id, 
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
            
            time_str = datetime.datetime.utcnow().strftime("%H:%M:%S")
            active_anomalies[device_id] = {
                "id": anomaly_id,
                "temp": avg_temp,
                "pressure": avg_pressure,
                "time": time_str
            }
            
            await tactical_events_topic.send(key=device_id.encode('utf-8'), value=payload)
        elif not is_anomalous and was_anomalous:
            logger.info(f"Anomaly resolved for {device_id}.")
            del active_anomalies[device_id]

if __name__ == '__main__':
    app.main()
