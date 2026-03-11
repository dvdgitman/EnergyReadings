import redis.asyncio as redis
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, field_validator
from typing import Optional
import os

app = FastAPI(title="Energy Ingestion API")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
STREAM_NAME = "energy_readings"

redis_client: Optional[redis.Redis] = None


@app.on_event("startup")
async def startup():
    global redis_client
    redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


@app.on_event("shutdown")
async def shutdown():
    if redis_client:
        await redis_client.aclose()


class Reading(BaseModel):
    site_id: str
    device_id: str
    power_reading: float
    timestamp: str

    @field_validator("site_id", "device_id", "timestamp")
    @classmethod
    def not_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("Field must not be empty")
        return v


@app.post("/readings", status_code=201)
async def post_reading(reading: Reading):
    try:
        stream_id = await redis_client.xadd(
            STREAM_NAME,
            {
                "site_id": reading.site_id,
                "device_id": reading.device_id,
                "power_reading": str(reading.power_reading),
                "timestamp": reading.timestamp,
            },
        )
        return {"status": "accepted", "stream_id": stream_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health():
    try:
        await redis_client.ping()
        return {"status": "ok"}
    except Exception:
        raise HTTPException(status_code=500, detail="Redis unreachable")
