import restate
from restate.context import Context
import sqlite3
import httpx
import logging
import json
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("restate_hub")

DB_PATH = os.environ.get("DB_PATH", "edge_state.db")

# Ensure table exists
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS radar_state (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id TEXT,
            radar_status TEXT,
            transmit_power INTEGER,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

init_db()

TacticalSaga = restate.Service("TacticalSaga")

@TacticalSaga.handler()
async def process_anomaly(ctx: Context, event: bytes):
    try:
        payload = json.loads(event.decode('utf-8'))
    except Exception as e:
        logger.error(f"Failed to parse event: {e}")
        return
        
    event_type = payload.get("type")
    if event_type != "CriticalAnomaly":
        return
        
    device_id = payload.get("source", "LTAMDS-04")
    
    # Step 1: Local State
    def update_local_db():
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(
            'INSERT INTO radar_state (device_id, radar_status, transmit_power) VALUES (?, ?, ?)',
            (device_id, 'DEGRADED', 85)
        )
        conn.commit()
        conn.close()
        return True
        
    await ctx.run("update_local_db", update_local_db)
    logger.info(f"Updated local state for {device_id}: DEGRADED, 85% power")
    
    # Step 2: The Enterprise Saga (ALCS API)
    async def submit_work_order():
        async with httpx.AsyncClient() as client:
            response = await client.post(
                "http://hq-alcs-api:8080/work-orders",
                json={
                    "device_id": device_id,
                    "issue": "Coolant Pressure Drop",
                    "action_taken": "Throttled radar power to 85%",
                    "priority": "CRITICAL"
                },
                timeout=5.0
            )
            response.raise_for_status()
            return response.json()
            
    # Restate will automatically retry on failure with exponential backoff
    await ctx.run("submit_work_order", submit_work_order)
    logger.info(f"Successfully submitted Work Order to HQ ALCS API for {device_id}")

app = restate.app([TacticalSaga])

if __name__ == '__main__':
    # Typically served using a web framework like hypercorn or uvicorn
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=9080)
