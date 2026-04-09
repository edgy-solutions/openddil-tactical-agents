import json
import datetime
import logging
import asyncpg
import restate
from restate.context import Context

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("hub-restate-projector")

# Regional DB connection string (for local UI syncing via ElectricSQL)
DB_DSN = "postgresql://openddil:openddil@localhost:5432/regional_db"

async def insert_to_db(payload: dict) -> bool:
    """
    Idempotent database insert function.
    Connects to the Regional Postgres database and inserts the alert.
    """
    event_id = payload.get('id')
    device_id = payload.get('source')
    event_type = payload.get('type')
    data = payload.get('data', {})
    severity = data.get('severity', 'UNKNOWN')
    detected_at_str = payload.get('time')
    
    if not all([event_id, device_id, detected_at_str]):
        logger.warning(f"Missing required fields in payload: {payload}")
        return False
        
    try:
        # Parse ISO-8601 string to datetime object
        detected_at = datetime.datetime.fromisoformat(detected_at_str.replace('Z', '+00:00'))
        
        # We establish a short-lived connection for the side-effect.
        # In a high-throughput scenario, a connection pool managed outside the side-effect might be preferred,
        # but asyncpg handles short connections well and this guarantees isolation for the Restate run.
        conn = await asyncpg.connect(dsn=DB_DSN)
        try:
            await conn.execute(
                """
                INSERT INTO local_tactical_alerts (id, device_id, event_type, severity, detected_at)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (id) DO NOTHING
                """,
                event_id, device_id, event_type, severity, detected_at
            )
            logger.info(f"Successfully projected alert {event_id} for device {device_id} to UI table.")
            return True
        finally:
            await conn.close()
    except Exception as e:
        logger.error(f"Failed to insert into database: {e}", exc_info=True)
        # Raise the exception so Restate knows the side-effect failed and will retry it
        raise

# Define the Restate Service
HubUIProjector = restate.Service("HubUIProjector")

@HubUIProjector.handler()
async def project_alert(ctx: Context, cloudevent: dict):
    """
    Restate handler designed to process incoming CloudEvents from the 
    global-tactical-events Redpanda topic.
    """
    event_id = cloudevent.get("id")
    event_type = cloudevent.get("type")
    
    # We only care about tactical alerts (ThresholdExceeded, DeviceOfflineAlert, etc.)
    if not event_type or not event_type.startswith("openddil.tactical."):
        logger.debug(f"Ignoring non-tactical event type: {event_type}")
        return

    logger.info(f"Processing tactical alert {event_id} of type {event_type}")

    # Use a Restate side-effect to execute the database transaction.
    # NOTE: ctx.run ensures exactly-once UI alerts even if Postgres temporarily disconnects.
    # If the database is down, the side-effect will fail, and Restate will automatically
    # and durably retry the execution of this specific side-effect until it succeeds,
    # without re-running any prior logic.
    await ctx.run("insert_alert", insert_to_db, cloudevent)
    
    return {"status": "projected", "id": event_id}

app = restate.app([HubUIProjector])

if __name__ == '__main__':
    # Typically run via a web framework or restate CLI
    pass
