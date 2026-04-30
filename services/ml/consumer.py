"""
Kafka consumer — scores each emission reading with the Isolation Forest
loaded from the MLflow Model Registry, then persists results to Postgres.

Model loading strategy (in order):
  1. MLflow Registry  →  models:/diesel-emissions-anomaly-detector@production
  2. Local joblib file (MODEL_PATH env var)
  3. None — readings are buffered/skipped until a model is available

Every RELOAD_INTERVAL_S seconds the consumer checks whether a newer
production model has been promoted and hot-swaps it without restarting.
"""
import json
import logging
import os
import time
from datetime import datetime, timezone

import joblib
import mlflow
import pandas as pd
from kafka import KafkaConsumer
from sqlalchemy import create_engine, text

from mlflow_utils import load_production_model, setup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ml-consumer")

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_SERVERS       = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC         = os.getenv("KAFKA_TOPIC",              "emissions.raw")
KAFKA_GROUP         = os.getenv("KAFKA_GROUP_ID",           "anomaly-detector")
DATABASE_URL        = os.getenv("DATABASE_URL",             "postgresql+psycopg2://admin:admin123@localhost/emissions")
MLFLOW_URI          = os.getenv("MLFLOW_TRACKING_URI",      "http://localhost:5000")
MODEL_PATH          = os.getenv("MODEL_PATH",               "/app/models/isolation_forest.joblib")
RELOAD_INTERVAL_S   = int(os.getenv("RELOAD_INTERVAL_S",    "120"))   # re-check registry every 2 min
BATCH_LOG_EVERY     = int(os.getenv("BATCH_LOG_EVERY",      "500"))   # log inference stats every N msgs

FEATURES = ["nox_mg_km", "pm25_ug_m3", "speed_kmh", "engine_temp_c", "fuel_rate_lh"]
CONAMA_PM25_LIMIT   = 25.0
EPA_NOX_NONCOMPLIANT = 270.0


# ── Model loader ──────────────────────────────────────────────────────────────
class ModelManager:
    """Hot-swappable model with registry-first loading and file fallback."""

    def __init__(self) -> None:
        self._model       = None
        self._last_reload = 0.0
        self._version     = "none"

    def get(self):
        if time.monotonic() - self._last_reload > RELOAD_INTERVAL_S:
            self._try_reload()
        return self._model

    def _try_reload(self) -> None:
        self._last_reload = time.monotonic()

        # 1. Try MLflow Registry
        m = load_production_model(MLFLOW_URI)
        if m is not None:
            self._model   = m
            self._version = "registry@production"
            log.info("Model hot-swapped from MLflow Registry")
            return

        # 2. Fall back to local file
        if os.path.exists(MODEL_PATH):
            self._model   = joblib.load(MODEL_PATH)
            self._version = f"local:{MODEL_PATH}"
            log.info("Model loaded from local file: %s", MODEL_PATH)
            return

        if self._model is None:
            log.warning("No model available — readings will be skipped until train.py runs")


# ── DB helpers ────────────────────────────────────────────────────────────────
def _init_engine():
    return create_engine(DATABASE_URL, pool_pre_ping=True)


def _persist(engine, reading: dict, score: float, is_anomaly: bool) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO anomaly_results
                    (vehicle_id, ts, nox_mg_km, pm25_ug_m3,
                     anomaly_score, is_anomaly, epa_tier, conama_compliant)
                VALUES
                    (:vid, :ts, :nox, :pm25,
                     :score, :anomaly, :epa_tier, :conama)
                ON CONFLICT (vehicle_id, ts) DO NOTHING
            """),
            {
                "vid":      reading["vehicle_id"],
                "ts":       reading.get("timestamp", datetime.now(timezone.utc)),
                "nox":      reading["nox_mg_km"],
                "pm25":     reading["pm25_ug_m3"],
                "score":    float(score),
                "anomaly":  is_anomaly,
                "epa_tier": _epa_tier(reading["nox_mg_km"]),
                "conama":   reading["pm25_ug_m3"] <= CONAMA_PM25_LIMIT,
            },
        )


# ── Domain helpers ────────────────────────────────────────────────────────────
def _epa_tier(nox: float) -> str:
    if nox <= 44:   return "Tier3"
    if nox <= 97:   return "Tier2-Bin5"
    if nox <= 270:  return "Tier2-Bin9"
    return "Non-compliant"


def _log_batch_metrics(total: int, anomalies: int, scores: list[float]) -> None:
    """Log a snapshot of inference stats to MLflow as a background metric."""
    try:
        with mlflow.start_run(run_name="consumer-inference-stats", nested=False):
            mlflow.log_metrics({
                "inference_total":    total,
                "inference_anomalies":anomalies,
                "inference_rate":     anomalies / max(total, 1),
                "score_mean":         sum(scores) / len(scores) if scores else 0.0,
            }, step=total)
    except Exception as exc:
        log.debug("Could not log batch metrics to MLflow: %s", exc)


# ── Main loop ─────────────────────────────────────────────────────────────────
def main() -> None:
    setup(MLFLOW_URI)
    engine  = _init_engine()
    manager = ModelManager()
    manager._try_reload()   # eager first load

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_SERVERS.split(","),
        group_id=KAFKA_GROUP,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )

    log.info("Listening on topic '%s' (group=%s)", KAFKA_TOPIC, KAFKA_GROUP)

    total         = 0
    anomaly_count = 0
    recent_scores: list[float] = []

    try:
        for msg in consumer:
            reading = msg.value
            model   = manager.get()

            if model is None:
                continue

            X          = pd.DataFrame([{f: reading[f] for f in FEATURES}])
            score      = float(model.decision_function(X)[0])
            is_anomaly = model.predict(X)[0] == -1

            total         += 1
            anomaly_count += int(is_anomaly)
            recent_scores.append(score)

            if is_anomaly:
                log.warning(
                    "ANOMALY | vehicle=%-10s  nox=%6.1f mg/km  pm25=%5.2f µg/m³"
                    "  temp=%5.1f°C  score=%+.4f  epa=%s",
                    reading["vehicle_id"],
                    reading["nox_mg_km"],
                    reading["pm25_ug_m3"],
                    reading["engine_temp_c"],
                    score,
                    _epa_tier(reading["nox_mg_km"]),
                )

            try:
                _persist(engine, reading, score, is_anomaly)
            except Exception as exc:
                log.error("DB write failed: %s", exc)

            # Periodic MLflow inference stats
            if total % BATCH_LOG_EVERY == 0:
                log.info(
                    "Processed %d msgs | anomaly_rate=%.1f%% | model=%s",
                    total, anomaly_count / total * 100, manager._version,
                )
                _log_batch_metrics(total, anomaly_count, recent_scores[-BATCH_LOG_EVERY:])
                recent_scores.clear()

    except KeyboardInterrupt:
        log.info(
            "Stopped. total=%d  anomalies=%d (%.1f%%)",
            total, anomaly_count, anomaly_count / max(total, 1) * 100,
        )
    finally:
        consumer.close()
        engine.dispose()


if __name__ == "__main__":
    main()
