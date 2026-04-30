"""
Offline training for the Isolation Forest anomaly detector.

Pipeline
────────
1. Load historical readings from Postgres (raw_emissions).
2. Run a contamination sweep (cross-validated proxy score) to pick
   the best hyperparameter.
3. Train the final pipeline (StandardScaler + IsolationForest).
4. Log everything to MLflow: params, metrics, plots, data stats.
5. Register the model and promote it to @production in the Registry.

Usage
─────
  python train.py                        # defaults
  CONTAMINATION=0.08 python train.py     # override contamination
  DRY_RUN=1 python train.py             # skip DB, use synthetic data
"""
import io
import logging
import os
import sys

import joblib
import matplotlib.pyplot as plt
import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
import seaborn as sns
from sklearn.ensemble import IsolationForest
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sqlalchemy import create_engine

from mlflow_utils import EXPERIMENT_NAME, register_and_promote, setup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("train")

# ── Config ────────────────────────────────────────────────────────────────────
DATABASE_URL  = os.getenv("DATABASE_URL",        "postgresql+psycopg2://admin:admin123@localhost/emissions")
MLFLOW_URI    = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
MODEL_OUT     = os.getenv("MODEL_PATH",           "models/isolation_forest.joblib")
CONTAMINATION = float(os.getenv("CONTAMINATION", "0.05"))
N_ESTIMATORS  = int(os.getenv("N_ESTIMATORS",   "200"))
MAX_ROWS      = int(os.getenv("MAX_ROWS",        "50000"))
DRY_RUN       = os.getenv("DRY_RUN", "0") == "1"

FEATURES = ["nox_mg_km", "pm25_ug_m3", "speed_kmh", "engine_temp_c", "fuel_rate_lh"]

# Contamination values tested in the sweep
SWEEP_CONTAMINATIONS = [0.02, 0.03, 0.05, 0.08, 0.10, 0.15]


# ── Data ──────────────────────────────────────────────────────────────────────
def load_data() -> pd.DataFrame:
    if DRY_RUN:
        log.warning("DRY_RUN mode — generating synthetic training data")
        rng = np.random.default_rng(42)
        n   = 10_000
        return pd.DataFrame({
            "nox_mg_km":     np.clip(rng.normal(100,  30,  n),   0, None),
            "pm25_ug_m3":    np.clip(rng.normal( 12,   4,  n),   0, None),
            "speed_kmh":     np.clip(rng.normal( 65,  25,  n),   0, None),
            "engine_temp_c": np.clip(rng.normal( 90,   5,  n),  60, None),
            "fuel_rate_lh":  np.clip(rng.normal( 15,   5,  n),   0, None),
        })

    engine = create_engine(DATABASE_URL)
    df = pd.read_sql(
        f"SELECT {', '.join(FEATURES)} "
        f"FROM raw_emissions "
        f"ORDER BY ts DESC LIMIT {MAX_ROWS}",
        engine,
    )
    log.info("Loaded %d rows from Postgres", len(df))
    return df


# ── Artefact helpers ──────────────────────────────────────────────────────────
def _log_data_stats(df: pd.DataFrame) -> None:
    stats = df.describe().T
    buf   = io.StringIO()
    stats.to_csv(buf)
    mlflow.log_text(buf.getvalue(), "data/feature_stats.csv")
    log.info("Logged feature stats")


def _log_distribution_plot(df: pd.DataFrame) -> None:
    fig, axes = plt.subplots(1, len(FEATURES), figsize=(18, 4))
    for ax, col in zip(axes, FEATURES):
        sns.histplot(df[col], bins=40, kde=True, ax=ax, color="steelblue")
        ax.set_title(col, fontsize=9)
        ax.set_xlabel("")
    fig.suptitle("Feature Distributions — Training Set", fontsize=11)
    fig.tight_layout()
    mlflow.log_figure(fig, "plots/feature_distributions.png")
    plt.close(fig)
    log.info("Logged feature distribution plot")


def _log_score_plot(df: pd.DataFrame, pipeline: Pipeline, contamination: float) -> None:
    scores    = pipeline.decision_function(df[FEATURES])
    threshold = np.percentile(scores, contamination * 100)

    fig, ax = plt.subplots(figsize=(10, 4))
    ax.hist(scores, bins=80, color="steelblue", alpha=0.7, label="Anomaly score")
    ax.axvline(threshold, color="red", linestyle="--",
               label=f"Decision boundary (contamination={contamination:.0%})")
    ax.set_xlabel("decision_function output  (lower = more anomalous)")
    ax.set_ylabel("Count")
    ax.set_title("Isolation Forest — Score Distribution")
    ax.legend()
    fig.tight_layout()
    mlflow.log_figure(fig, "plots/score_distribution.png")
    plt.close(fig)
    log.info("Logged score distribution plot")


def _log_correlation_heatmap(df: pd.DataFrame) -> None:
    fig, ax = plt.subplots(figsize=(7, 5))
    sns.heatmap(df[FEATURES].corr(), annot=True, fmt=".2f", cmap="coolwarm",
                ax=ax, linewidths=0.5)
    ax.set_title("Feature Correlation Matrix")
    fig.tight_layout()
    mlflow.log_figure(fig, "plots/correlation_heatmap.png")
    plt.close(fig)
    log.info("Logged correlation heatmap")


# ── Sweep ─────────────────────────────────────────────────────────────────────
def _sweep_contamination(df: pd.DataFrame) -> dict[float, float]:
    """
    Proxy metric for an unsupervised model: mean absolute anomaly score
    for the predicted anomalies (higher absolute score = more confident
    separation). Returns {contamination: proxy_score}.
    """
    results = {}
    scaler  = StandardScaler()
    X       = scaler.fit_transform(df[FEATURES])

    for c in SWEEP_CONTAMINATIONS:
        model   = IsolationForest(contamination=c, n_estimators=100, random_state=42, n_jobs=-1)
        model.fit(X)
        scores  = model.decision_function(X)
        mask    = model.predict(X) == -1
        # Average absolute score of anomalies — separation strength proxy
        proxy   = float(-scores[mask].mean()) if mask.any() else 0.0
        results[c] = proxy
        log.info("  contamination=%.2f  proxy_score=%.4f  anomaly_rate=%.2f%%",
                 c, proxy, mask.mean() * 100)

    return results


# ── Main training run ─────────────────────────────────────────────────────────
def train() -> None:
    setup(MLFLOW_URI)
    df = load_data()

    log.info("Running contamination sweep over %s …", SWEEP_CONTAMINATIONS)
    sweep_scores = _sweep_contamination(df)

    # Best contamination from sweep (unless explicitly overridden via env)
    best_contamination = (
        CONTAMINATION
        if os.getenv("CONTAMINATION")
        else max(sweep_scores, key=sweep_scores.get)
    )
    log.info("Selected contamination: %.3f", best_contamination)

    pipeline = Pipeline([
        ("scaler",     StandardScaler()),
        ("iso_forest", IsolationForest(
            contamination=best_contamination,
            n_estimators=N_ESTIMATORS,
            random_state=42,
            n_jobs=-1,
        )),
    ])

    with mlflow.start_run(run_name=f"iso-forest-c{best_contamination:.3f}") as run:

        # ── autolog captures pipeline params + sklearn metrics automatically
        mlflow.sklearn.autolog(
            log_input_examples=False,   # no PII in artefacts
            log_model_signatures=True,
            log_models=True,
            registered_model_name=None, # we register manually below
            silent=False,
        )

        pipeline.fit(df[FEATURES])

        scores       = pipeline.decision_function(df[FEATURES])
        preds        = pipeline.predict(df[FEATURES])
        anomaly_mask = preds == -1

        # ── Manual metrics (supplement autolog)
        mlflow.log_params({
            "contamination":      best_contamination,
            "n_estimators":       N_ESTIMATORS,
            "features":           str(FEATURES),
            "train_rows":         len(df),
            "dry_run":            DRY_RUN,
        })

        mlflow.log_metrics({
            "anomaly_rate":           float(anomaly_mask.mean()),
            "anomaly_count":          int(anomaly_mask.sum()),
            "score_mean":             float(scores.mean()),
            "score_std":              float(scores.std()),
            "score_p5":               float(np.percentile(scores,  5)),
            "score_p95":              float(np.percentile(scores, 95)),
            "normal_score_mean":      float(scores[~anomaly_mask].mean()),
            "anomaly_score_mean":     float(scores[ anomaly_mask].mean()),
            # Sweep results logged as metrics for comparison across runs
            **{f"sweep_c{c:.2f}": v for c, v in sweep_scores.items()},
        })

        # ── Tags
        mlflow.set_tags({
            "model_type":     "IsolationForest",
            "scaler":         "StandardScaler",
            "environment":    "dev" if DRY_RUN else "prod",
            "regulations":    "EPA-Tier2-Bin9 / CONAMA-18",
            "feature_count":  str(len(FEATURES)),
        })

        # ── Artefacts
        _log_data_stats(df)
        _log_distribution_plot(df)
        _log_score_plot(df, pipeline, best_contamination)
        _log_correlation_heatmap(df)

        # Log the sweep results as a CSV artefact
        sweep_df = pd.DataFrame(
            [{"contamination": c, "proxy_score": s} for c, s in sweep_scores.items()]
        )
        buf = io.StringIO()
        sweep_df.to_csv(buf, index=False)
        mlflow.log_text(buf.getvalue(), "data/contamination_sweep.csv")

        # ── Persist model locally (for the consumer's file fallback)
        os.makedirs(os.path.dirname(MODEL_OUT) or ".", exist_ok=True)
        joblib.dump(pipeline, MODEL_OUT)
        mlflow.log_artifact(MODEL_OUT, "local_model")

        run_id = run.info.run_id
        log.info("Run %s finished — anomaly_rate=%.2f%%", run_id[:8], anomaly_mask.mean() * 100)

    # ── Register & promote outside the run context
    version = register_and_promote(run_id)
    log.info(
        "Model v%s registered as @staging and @production in registry '%s'",
        version, "diesel-emissions-anomaly-detector",
    )


if __name__ == "__main__":
    train()
