import asyncio
import json
import redis.asyncio as redis
from fastapi import FastAPI, HTTPException
from typing import Optional
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Energy Processing Service")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
STREAM_NAME = "energy_readings"
GROUP_NAME = "processing_group"
CONSUMER_NAME = os.getenv("CONSUMER_NAME", "consumer-1")

redis_client: Optional[redis.Redis] = None
consumer_task: Optional[asyncio.Task] = None


async def ensure_consumer_group():
    try:
        await redis_client.xgroup_create(STREAM_NAME, GROUP_NAME, id="0", mkstream=True)
        logger.info(f"Created consumer group '{GROUP_NAME}'")
    except redis.ResponseError as e:
        if "BUSYGROUP" in str(e):
            logger.info(f"Consumer group '{GROUP_NAME}' already exists")
        else:
            raise


async def consume_loop():
    logger.info("Starting consumer loop...")
    while True:
        try:
            messages = await redis_client.xreadgroup(
                GROUP_NAME, CONSUMER_NAME, {STREAM_NAME: ">"}, count=10, block=1000
            )
            if not messages:
                continue
            for stream, entries in messages:
                for msg_id, fields in entries:
                    site_id = fields.get("site_id")
                    if not site_id:
                        await redis_client.xack(STREAM_NAME, GROUP_NAME, msg_id)
                        continue
                    reading = {
                        "site_id": fields.get("site_id"),
                        "device_id": fields.get("device_id"),
                        "power_reading": float(fields.get("power_reading", 0)),
                        "timestamp": fields.get("timestamp"),
                        "stream_id": msg_id,
                    }
                    key = f"site_readings:{site_id}"
                    await redis_client.rpush(key, json.dumps(reading))
                    await redis_client.xack(STREAM_NAME, GROUP_NAME, msg_id)
                    logger.info(f"Processed reading for site {site_id}, msg {msg_id}")
        except asyncio.CancelledError:
            logger.info("Consumer loop cancelled")
            break
        except Exception as e:
            logger.error(f"Consumer error: {e}")
            await asyncio.sleep(2)


@app.on_event("startup")
async def startup():
    global redis_client, consumer_task
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    await ensure_consumer_group()
    consumer_task = asyncio.create_task(consume_loop())


@app.on_event("shutdown")
async def shutdown():
    if consumer_task:
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass
    if redis_client:
        await redis_client.aclose()


@app.get("/sites/{site_id}/readings")
async def get_readings(site_id: str):
    try:
        key = f"site_readings:{site_id}"
        raw = await redis_client.lrange(key, 0, -1)
        readings = [json.loads(r) for r in raw]
        return {"site_id": site_id, "count": len(readings), "readings": readings}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health():
    try:
        await redis_client.ping()
        return {"status": "ok"}
    except Exception:
        raise HTTPException(status_code=500, detail="Redis unreachable")
