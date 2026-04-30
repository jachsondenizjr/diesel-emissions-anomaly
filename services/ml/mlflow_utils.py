"""
Shared MLflow helpers used by train.py and consumer.py.

Centralises experiment name, model name, and registry operations
so both scripts stay in sync with a single source of truth.
"""
import logging

import mlflow
import mlflow.sklearn
from mlflow import MlflowClient

log = logging.getLogger(__name__)

EXPERIMENT_NAME = "diesel-anomaly-detection"
MODEL_NAME      = "diesel-emissions-anomaly-detector"
ALIAS_STAGING   = "staging"
ALIAS_PROD      = "production"


def setup(tracking_uri: str) -> str:
    """Set tracking URI, create experiment if absent, return experiment_id."""
    mlflow.set_tracking_uri(tracking_uri)
    client = MlflowClient()

    exp = client.get_experiment_by_name(EXPERIMENT_NAME)
    if exp is None:
        exp_id = client.create_experiment(
            EXPERIMENT_NAME,
            tags={
                "project":     "diesel-emissions",
                "team":        "data-engineering",
                "regulations": "EPA-Tier2 / CONAMA-18",
            },
        )
        log.info("Created MLflow experiment '%s' (id=%s)", EXPERIMENT_NAME, exp_id)
    else:
        exp_id = exp.experiment_id
        log.info("Using MLflow experiment '%s' (id=%s)", EXPERIMENT_NAME, exp_id)

    mlflow.set_experiment(EXPERIMENT_NAME)
    return exp_id


def register_and_promote(run_id: str) -> str:
    """
    Register model artifact from *run_id*, then set both staging and
    production aliases so the consumer can load it immediately.
    Returns the new model version string.
    """
    client    = MlflowClient()
    model_uri = f"runs:/{run_id}/isolation_forest"

    mv = mlflow.register_model(model_uri, MODEL_NAME)
    log.info("Registered '%s' as version %s", MODEL_NAME, mv.version)

    client.set_registered_model_alias(MODEL_NAME, ALIAS_STAGING, mv.version)
    log.info("Alias '%s' → v%s", ALIAS_STAGING, mv.version)

    client.set_registered_model_alias(MODEL_NAME, ALIAS_PROD, mv.version)
    log.info("Alias '%s' → v%s", ALIAS_PROD, mv.version)

    return mv.version


def load_production_model(tracking_uri: str):
    """
    Load the model tagged as *production* from the MLflow Registry.
    Returns None (with a warning) if the registry is unreachable or
    no production alias exists yet.
    """
    mlflow.set_tracking_uri(tracking_uri)
    model_uri = f"models:/{MODEL_NAME}@{ALIAS_PROD}"
    try:
        model = mlflow.sklearn.load_model(model_uri)
        log.info("Loaded production model from registry: %s", model_uri)
        return model
    except Exception as exc:
        log.warning("Registry load failed (%s) — %s", model_uri, exc)
        return None


def list_versions() -> None:
    """Print all registered versions with their aliases (useful for debugging)."""
    client = MlflowClient()
    try:
        for mv in client.search_model_versions(f"name='{MODEL_NAME}'"):
            aliases = client.get_model_version_by_alias  # just enumerate
            print(f"  v{mv.version}  run={mv.run_id[:8]}  aliases={mv.aliases}")
    except Exception as exc:
        print(f"Could not list versions: {exc}")
