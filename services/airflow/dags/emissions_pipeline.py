"""
Daily emissions pipeline:
  validate_raw → dbt_run → dbt_test → check_anomaly_rate
    ├── retrain_model  (if rate > 10%)
    └── skip_retrain
  → export_compliance_report
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

DBT_DIR            = "/opt/airflow/dbt/diesel_emissions"
RETRAIN_THRESHOLD  = 0.10  # retrain when daily anomaly rate > 10%

default_args = {
    "owner":            "data-engineering",
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}


def _check_anomaly_rate(**ctx) -> str:
    hook    = PostgresHook(postgres_conn_id="emissions_postgres")
    result  = hook.get_first("""
        SELECT COALESCE(
            COUNT(*) FILTER (WHERE is_anomaly)::float / NULLIF(COUNT(*), 0),
            0
        )
        FROM anomaly_results
        WHERE ts >= NOW() - INTERVAL '24 hours'
    """)
    rate = result[0] if result else 0.0
    ctx["ti"].xcom_push(key="anomaly_rate", value=float(rate))
    return "retrain_model" if rate > RETRAIN_THRESHOLD else "skip_retrain"


def _export_compliance_report(**ctx) -> None:
    hook = PostgresHook(postgres_conn_id="emissions_postgres")
    df   = hook.get_pandas_df("""
        SELECT
            vehicle_id,
            DATE_TRUNC('day', ts)          AS day,
            AVG(nox_mg_km)                 AS avg_nox,
            AVG(pm25_ug_m3)                AS avg_pm25,
            SUM(is_anomaly::int)           AS anomaly_count,
            BOOL_AND(conama_compliant)     AS conama_compliant
        FROM anomaly_results
        WHERE ts >= NOW() - INTERVAL '24 hours'
        GROUP BY 1, 2
        ORDER BY 1, 2
    """)
    path = f"/opt/airflow/dbt/reports/compliance_{ctx['ds']}.csv"
    import os; os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)
    print(f"Compliance report → {path}  ({len(df)} vehicles)")


with DAG(
    dag_id="diesel_emissions_pipeline",
    description="Daily anomaly detection, dbt transformation, and compliance report",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args,
    tags=["emissions", "anomaly-detection", "dbt", "epa", "conama"],
) as dag:

    validate_raw = BashOperator(
        task_id="validate_raw_emissions",
        bash_command=(
            'psql "$DATABASE_URL" -c '
            '"SELECT COUNT(*) FROM raw_emissions WHERE ts >= NOW() - INTERVAL \'24 hours\'"'
        ),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
    )

    branch = BranchPythonOperator(
        task_id="check_anomaly_rate",
        python_callable=_check_anomaly_rate,
    )

    retrain = BashOperator(
        task_id="retrain_model",
        bash_command="python /opt/airflow/dbt/../ml/train.py",
    )

    skip_retrain = BashOperator(
        task_id="skip_retrain",
        bash_command="echo 'Anomaly rate within threshold — skipping retrain'",
    )

    compliance_report = PythonOperator(
        task_id="export_compliance_report",
        python_callable=_export_compliance_report,
        trigger_rule="none_failed_min_one_success",
    )

    validate_raw >> dbt_run >> dbt_test >> branch
    branch >> [retrain, skip_retrain] >> compliance_report
