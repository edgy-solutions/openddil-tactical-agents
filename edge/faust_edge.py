import faust
import datetime
import logging
import json
import uuid
import yaml
import os
import asyncio
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("faust_edge")

RULES_FILE = os.environ.get('RULES_FILE', '/app/rules.yaml')
rules_config = {"rules": []}

def load_rules():
    global rules_config
    try:
        with open(RULES_FILE, 'r') as f:
            rules_config = yaml.safe_load(f) or {"rules": []}
            logger.info(f"Loaded rules: {rules_config}")
    except Exception as e:
        logger.error(f"Failed to load rules: {e}")

class RuleReloader(FileSystemEventHandler):
    def on_modified(self, event):
        if event.src_path == RULES_FILE or os.path.abspath(event.src_path) == os.path.abspath(RULES_FILE):
            logger.info(f"Rules file {RULES_FILE} modified. Reloading...")
            load_rules()

load_rules()

observer = Observer()
observer.schedule(RuleReloader(), path=os.path.dirname(RULES_FILE) or '.', recursive=False)
observer.start()

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
    default=lambda: {'sum': 0.0, 'count': 0, 'values': []},
).tumbling(10.0, expires=datetime.timedelta(seconds=10.0))

recent_alerts = []
active_anomalies = {}
latest_telemetry = {"thermal": 32.0, "pressure": 120.0}

@app.page('/alerts')
class Alerts(faust.web.View):
    async def get(self, request):
        alerts = []
        for anomaly_id, anomaly in active_anomalies.items():
            alerts.append({
                "id": anomaly["id"],
                "msg": anomaly["msg"],
                "type": "crit",
                "time": anomaly["time"]
            })
            if anomaly.get("action") != "LOG_ONLY":
                alerts.append({
                    "id": anomaly["id"] + "-action",
                    "msg": f"ACTION: {anomaly.get('action')} via Restate Agent",
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
        if not device_id:
            continue
            
        if "core_temp" in event:
            latest_telemetry["thermal"] = event["core_temp"]
        if "coolant_pressure" in event:
            latest_telemetry["pressure"] = event["coolant_pressure"]

        for rule in rules_config.get("rules", []):
            if rule.get("target_device") != device_id and rule.get("target_device") != "Global Fleet":
                continue
                
            sensor_field = rule.get("sensor_field")
            if sensor_field not in event:
                continue
                
            val = event[sensor_field]
            if val is None:
                continue
                
            window_key = f"{device_id}_{sensor_field}_{rule.get('id')}"
            stats = sensor_window[window_key].current()
            stats['sum'] += val
            stats['count'] += 1
            stats['values'].append(val)
            sensor_window[window_key] = stats
            
            avg_val = stats['sum'] / stats['count']
            
            algorithm = rule.get("algorithm", "Absolute Threshold")
            threshold = float(rule.get("threshold", 0.0))
            
            is_anomalous = False
            
            if algorithm == "Absolute Threshold" or algorithm == "absolute":
                is_anomalous = avg_val > threshold
            elif algorithm == "Rate of Change (Derivative)" or algorithm == "rate_of_change":
                if len(stats['values']) >= 2:
                    rate = stats['values'][-1] - stats['values'][0]
                    is_anomalous = rate > threshold
            elif algorithm == "Statistical Anomaly (Z-Score)" or algorithm == "z_score":
                if len(stats['values']) > 2:
                    mean = sum(stats['values']) / len(stats['values'])
                    variance = sum((x - mean) ** 2 for x in stats['values']) / len(stats['values'])
                    std_dev = variance ** 0.5
                    if std_dev > 0:
                        z_score = (val - mean) / std_dev
                        is_anomalous = z_score > threshold
            
            anomaly_key = f"{device_id}_{rule.get('id')}"
            was_anomalous = anomaly_key in active_anomalies
            
            if is_anomalous and not was_anomalous:
                msg = f"WARNING: Temp threshold ({threshold}C) exceeded." if sensor_field == "core_temp" else f"ANOMALY: {sensor_field} exceeded threshold."
                logger.warning(f"CRITICAL ANOMALY DETECTED for {device_id} rule {rule.get('id')}! {msg}")
                
                anomaly_id = str(uuid.uuid4())
                payload = {
                    "id": anomaly_id, 
                    "source": device_id,
                    "type": "CriticalAnomaly",
                    "time": datetime.datetime.utcnow().isoformat() + "Z",
                    "datacontenttype": "application/json",
                    "data": {
                        "rule_id": rule.get('id'),
                        "sensor_field": sensor_field,
                        "value": avg_val,
                        "threshold": threshold,
                        "action": rule.get("action_payload", "LOG_ONLY"),
                        "severity": "CRITICAL"
                    }
                }
                
                time_str = datetime.datetime.utcnow().strftime("%H:%M:%S")
                active_anomalies[anomaly_key] = {
                    "id": anomaly_id,
                    "msg": msg,
                    "action": rule.get("action_payload", "LOG_ONLY"),
                    "time": time_str
                }
                
                await tactical_events_topic.send(key=device_id.encode('utf-8'), value=payload)
            elif not is_anomalous and was_anomalous:
                logger.info(f"Anomaly resolved for {device_id} rule {rule.get('id')}.")
                del active_anomalies[anomaly_key]

if __name__ == '__main__':
    app.main()
