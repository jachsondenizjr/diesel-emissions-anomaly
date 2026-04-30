import json

from fastapi import APIRouter, HTTPException
from kafka import KafkaProducer

from app.core.config import settings
from app.models.schemas import EmissionReading

router = APIRouter()


def _producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers.split(","),
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        acks="all",
    )


@router.post("/ingest", status_code=202)
async def ingest_emission(reading: EmissionReading):
    try:
        p = _producer()
        p.send(settings.kafka_topic, reading.model_dump())
        p.flush()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Kafka unavailable: {exc}")
    return {"status": "accepted", "vehicle_id": reading.vehicle_id}


@router.post("/ingest/batch", status_code=202)
async def ingest_batch(readings: list[EmissionReading]):
    try:
        p = _producer()
        for r in readings:
            p.send(settings.kafka_topic, r.model_dump())
        p.flush()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Kafka unavailable: {exc}")
    return {"status": "accepted", "count": len(readings)}
