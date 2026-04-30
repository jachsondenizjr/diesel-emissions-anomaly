"""
Diesel fleet emissions simulator.

Publishes synthetic NOx / PM2.5 readings to Kafka.
Normal readings follow Euro-5/6 distributions; anomalies are injected
at a configurable rate to exercise the Isolation Forest consumer.

Env vars:
  KAFKA_BOOTSTRAP_SERVERS  default: localhost:9092
  KAFKA_TOPIC              default: emissions.raw
  NUM_VEHICLES             default: 10
  INTERVAL_S               default: 0.5   (seconds between batches)
  ANOMALY_RATE             default: 0.05  (5% of readings are anomalous)
  SEED                     default: None  (set for reproducible runs)
"""
import json
import logging
import os
import random
import time
from datetime import datetime, timezone

import numpy as np
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("simulator")

# ── Config ───────────────────────────────────────────────────────────────────
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC   = os.getenv("KAFKA_TOPIC",              "emissions.raw")
NUM_VEHICLES  = int(os.getenv("NUM_VEHICLES",  "10"))
INTERVAL_S    = float(os.getenv("INTERVAL_S",  "0.5"))
ANOMALY_RATE  = float(os.getenv("ANOMALY_RATE","0.05"))
SEED          = os.getenv("SEED")

VEHICLES = [f"TRUCK-{i:03d}" for i in range(1, NUM_VEHICLES + 1)]

# ── Realistic emission distributions (Euro 5/6 diesel fleet) ────────────────
# Each entry: (mean, std, low_clip, high_clip)
NORMAL = {
    "nox_mg_km":     (100,  30,   20,  250),
    "pm25_ug_m3":    ( 12,   4,    2,   24),
    "speed_kmh":     ( 65,  25,    5,  130),
    "engine_temp_c": ( 90,   5,   75,  105),
    "fuel_rate_lh":  ( 15,   5,    4,   30),
}

ANOMALOUS = {
    "nox_mg_km":     (500, 120,  271,  900),   # >> EPA Tier2-Bin9 limit (270)
    "pm25_ug_m3":    ( 50,  15,   26,  120),   # >> CONAMA limit (25)
    "speed_kmh":     ( 65,  25,    5,  130),   # speed stays realistic
    "engine_temp_c": (150,  20,  115,  220),   # overheating
    "fuel_rate_lh":  ( 40,  10,   28,   80),   # abnormal consumption
}


def _sample(dist: dict, rng: np.random.Generator) -> dict:
    return {
        k: float(np.clip(rng.normal(mu, sigma), lo, hi))
        for k, (mu, sigma, lo, hi) in dist.items()
    }


def _reading(vehicle_id: str, rng: np.random.Generator) -> dict:
    is_anomaly = rng.random() < ANOMALY_RATE
    dist       = ANOMALOUS if is_anomaly else NORMAL
    data       = _sample(dist, rng)

    # Random GPS coords within São Paulo metro area
    data["lat"] = float(rng.uniform(-23.75, -23.45))
    data["lon"] = float(rng.uniform(-46.80, -46.35))

    return {
        "vehicle_id": vehicle_id,
        "timestamp":  datetime.now(timezone.utc).isoformat(),
        "_simulated_anomaly": is_anomaly,
        **data,
    }


def _connect(retries: int = 10, delay: float = 5.0) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_SERVERS.split(","),
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
                acks="all",
                retries=3,
            )
            log.info("Connected to Kafka at %s", KAFKA_SERVERS)
            return producer
        except NoBrokersAvailable:
            log.warning("Kafka not ready — attempt %d/%d, retrying in %.0fs…", attempt, retries, delay)
            time.sleep(delay)
    raise SystemExit("Could not connect to Kafka after %d attempts" % retries)


def main() -> None:
    seed = int(SEED) if SEED else None
    rng  = np.random.default_rng(seed)
    if seed:
        random.seed(seed)

    log.info(
        "Starting simulator | vehicles=%d  interval=%.2fs  anomaly_rate=%.0f%%  topic=%s",
        NUM_VEHICLES, INTERVAL_S, ANOMALY_RATE * 100, KAFKA_TOPIC,
    )

    producer = _connect()
    total = anomalies = 0

    try:
        while True:
            batch_start = time.monotonic()

            for vid in VEHICLES:
                msg        = _reading(vid, rng)
                is_anomaly = msg.pop("_simulated_anomaly")
                producer.send(KAFKA_TOPIC, msg)

                total += 1
                if is_anomaly:
                    anomalies += 1
                    log.warning(
                        "⚠  ANOMALY | %s  nox=%.1f mg/km  pm25=%.2f µg/m³  temp=%.1f°C",
                        vid, msg["nox_mg_km"], msg["pm25_ug_m3"], msg["engine_temp_c"],
                    )

            producer.flush()

            if total % (NUM_VEHICLES * 20) == 0:
                log.info("Sent %d readings total — anomaly rate so far: %.1f%%", total, anomalies / total * 100)

            elapsed = time.monotonic() - batch_start
            time.sleep(max(0.0, INTERVAL_S - elapsed))

    except KeyboardInterrupt:
        log.info("Stopped. Total sent: %d  |  anomalies injected: %d (%.1f%%)", total, anomalies, anomalies / max(total, 1) * 100)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
