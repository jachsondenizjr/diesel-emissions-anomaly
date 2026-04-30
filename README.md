# Diesel Emissions Anomaly Detection

> Real-time NOx & PM2.5 anomaly detection for a diesel fleet using an end-to-end
> streaming + ML pipeline — from sensor ingestion to regulatory compliance reporting.

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python&logoColor=white)
![Kafka](https://img.shields.io/badge/Kafka-7.6-231F20?logo=apachekafka&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-0.111-009688?logo=fastapi&logoColor=white)
![scikit-learn](https://img.shields.io/badge/scikit--learn-1.5-F7931E?logo=scikitlearn&logoColor=white)
![MLflow](https://img.shields.io/badge/MLflow-2.13-0194E2?logo=mlflow&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-1.8-FF694B?logo=dbt&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-2.9-017CEE?logo=apacheairflow&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-10.4-F46800?logo=grafana&logoColor=white)

---

## Problem

Diesel fleets emit nitrogen oxides (NOx) and particulate matter (PM2.5) that are regulated by
**EPA Tier 2** and **CONAMA Resolution 18/1986**. Detecting anomalous readings in real time —
before they become compliance violations — requires a system that can:

- Ingest high-frequency sensor data from hundreds of vehicles simultaneously
- Score each reading for anomaly in milliseconds without labelled training data
- Persist, transform, and aggregate results for regulatory reporting
- Retrain automatically when fleet behaviour shifts over time

---

## Architecture

```
┌─────────────┐   POST /ingest    ┌──────────────┐   emissions.raw   ┌──────────────────┐
│  Simulator  │ ───────────────▶  │   FastAPI    │ ────────────────▶ │      Kafka       │
│ (synthetic  │                   │  + Prometheus│                   │  (Confluent 7.6) │
│  fleet data)│                   └──────────────┘                   └────────┬─────────┘
└─────────────┘                                                                │
                                                                               ▼
                                                                    ┌──────────────────────┐
                                                                    │     ml-consumer      │
                                                                    │  Isolation Forest    │
                                                                    │  ← MLflow Registry   │
                                                                    │    @production       │
                                                                    └──────────┬───────────┘
                                                                               │ score + tier
                                                                               ▼
┌──────────────┐   daily DAG    ┌──────────────┐              ┌───────────────────────────┐
│   Airflow    │ ─────────────▶ │  dbt 1.8     │              │       PostgreSQL 16        │
│  (CeleryExec)│   dbt run/test │              │              │  ┌─────────────────────┐  │
│              │   retrain if   │  staging:    │◀─────────────│  │   raw_emissions     │  │
│              │   rate > 10%   │  view        │              │  ├─────────────────────┤  │
│              │   compliance   │              │              │  │   anomaly_results   │  │
│              │   report CSV   │  mart:       │              │  └─────────────────────┘  │
└──────────────┘                │  table/hour  │              └───────────────────────────┘
                                └──────┬───────┘                           │
                                       │                                   │
                                       ▼                                   ▼
                              ┌─────────────────┐               ┌──────────────────┐
                              │  MLflow 2.13    │               │   Grafana 10.4   │
                              │  · Experiments  │               │   · 10 panels    │
                              │  · Registry     │               │   · 5s refresh   │
                              │  · Artifacts    │               │   · EPA/CONAMA   │
                              └─────────────────┘               └──────────────────┘
```

---

## Tech Stack

| Layer | Technology | Role |
|---|---|---|
| **Ingestion** | Apache Kafka 7.6 | Message broker — `emissions.raw` topic |
| **API** | FastAPI 0.111 | REST ingest endpoint + Prometheus `/metrics` |
| **ML** | Scikit-learn 1.5 — Isolation Forest | Unsupervised anomaly scoring |
| **Experiment tracking** | MLflow 2.13 | Parameters, metrics, artifact plots, Model Registry |
| **Orchestration** | Apache Airflow 2.9 (CeleryExecutor) | Daily pipeline DAG |
| **Transformation** | dbt 1.8 | `stg_emissions` (view) → `mart_anomalies` (table) |
| **Storage** | PostgreSQL 16 | Three databases: `emissions`, `airflow`, `mlflow` |
| **Dashboard** | Grafana 10.4 | Live 10-panel dashboard, auto-provisioned |
| **Observability** | Prometheus | Scrapes FastAPI + Kafka metrics |
| **Containerisation** | Docker Compose | 13 services, named volumes, health checks |

---

## Regulatory Context

| Regulation | Pollutant | Limit | Applied in |
|---|---|---|---|
| EPA Tier 2 Bin 9 | NOx | ≤ 270 mg/km | `_epa_tier()` in consumer + dbt mart |
| EPA Tier 2 Bin 5 | NOx | ≤ 97 mg/km | EPA classification column |
| EPA Tier 3 | NOx | ≤ 44 mg/km | EPA classification column |
| CONAMA Resolution 18/1986 | PM2.5 | ≤ 25 µg/m³ | `conama_compliant` boolean |

Compliance status is tracked at every reading, aggregated hourly by dbt, and exported
as a CSV report by the Airflow DAG for submission to regulators.

---

## Key Design Decisions

| Decision | Rationale |
|---|---|
| **Isolation Forest** | Unsupervised — no labelled anomaly data available at fleet deployment |
| **Contamination sweep** | Proxy metric (anomaly score separation) replaces supervised CV for hyperparameter selection |
| **MLflow Registry `@production` alias** | Consumer hot-swaps model every 2 min without container restart |
| **`UNIQUE (vehicle_id, ts)`** | Idempotent Kafka re-processing — safe to replay any partition offset |
| **dbt staging as VIEW** | Always reflects latest raw data with zero storage overhead |
| **dbt mart as TABLE** | Pre-aggregated by hour — sub-second Grafana queries at any scale |
| **Partial index `WHERE is_anomaly = TRUE`** | EPA/CONAMA reporting only scans the ~5% flagged rows |

---

## Services & Ports

| Service | Port | Credentials |
|---|---|---|
| FastAPI | `8000` | — |
| Kafka UI | `8080` | — |
| Airflow | `8081` | `admin` / `admin` |
| MLflow | `5000` | — |
| Grafana | `3000` | `admin` / `admin` |
| Prometheus | `9090` | — |
| PostgreSQL | `5432` | see `.env` |

---

## Quick Start

### Prerequisites
- Docker Desktop ≥ 24 with Compose V2
- 6 GB RAM allocated to Docker
- `gh` CLI (optional, for GitHub operations)

### 1 — Clone & configure

```bash
git clone https://github.com/jachsondenizjr/diesel-emissions-anomaly.git
cd diesel-emissions-anomaly
cp .env.example .env          # edit credentials if needed
```

### 2 — Start the full stack

```bash
# Core stack (no simulator)
docker compose up --build -d

# Core stack + live data simulator
docker compose --profile simulator up --build -d
```

### 3 — Initialise Airflow

```bash
docker compose run --rm airflow-init
```

### 4 — Train the first model

```bash
# With real data (after the simulator has sent some readings)
docker compose run --rm ml-consumer python train.py

# Without the Docker stack (uses synthetic data)
docker compose run --rm -e DRY_RUN=1 ml-consumer python train.py
```

### 5 — Open the dashboards

| URL | What you see |
|---|---|
| http://localhost:3000 | Grafana — live NOx/PM2.5 panels |
| http://localhost:5000 | MLflow — experiment runs + model registry |
| http://localhost:8081 | Airflow — DAG status |
| http://localhost:8080 | Kafka UI — topic lag |
| http://localhost:8000/docs | FastAPI — interactive Swagger UI |

### 6 — Ingest a reading manually

```bash
curl -s -X POST http://localhost:8000/emissions/ingest \
  -H 'Content-Type: application/json' \
  -d '{
    "vehicle_id": "TRUCK-001",
    "nox_mg_km": 420,
    "pm25_ug_m3": 38,
    "speed_kmh": 72,
    "engine_temp_c": 148,
    "fuel_rate_lh": 35
  }' | python -m json.tool
```

---

## Project Structure

```
diesel-emissions-anomaly/
├── docker-compose.yml                  ← 13 services, named volumes, health checks
├── .env                                ← local credentials (git-ignored)
│
├── services/
│   ├── postgres/
│   │   ├── init-multiple-dbs.sh        ← creates airflow + mlflow databases
│   │   └── 01_schema.sql               ← raw_emissions + anomaly_results + 6 indexes
│   │
│   ├── fastapi/                        ← :8000
│   │   └── app/
│   │       ├── main.py                 ← Prometheus instrumentation
│   │       ├── routes/emissions.py     ← POST /ingest  +  /ingest/batch
│   │       └── models/schemas.py       ← Pydantic: EmissionReading, AnomalyResult
│   │
│   ├── ml/
│   │   ├── consumer.py                 ← Kafka consumer + ModelManager (hot-swap)
│   │   ├── train.py                    ← contamination sweep + autolog + Registry
│   │   └── mlflow_utils.py             ← setup(), register_and_promote(), load_production_model()
│   │
│   ├── airflow/
│   │   └── dags/emissions_pipeline.py  ← validate → dbt → branch → compliance report
│   │
│   ├── dbt/diesel_emissions/
│   │   └── models/
│   │       ├── staging/stg_emissions.sql
│   │       └── marts/mart_anomalies.sql ← EPA tier + CONAMA flags, hourly
│   │
│   ├── grafana/provisioning/
│   │   ├── datasources/postgres.yml    ← auto-provisioned PostgreSQL datasource
│   │   └── dashboards/
│   │       └── emissions_dashboard.json ← 10-panel dashboard JSON
│   │
│   └── prometheus/prometheus.yml
│
├── simulator/
│   └── simulate.py                     ← Euro 5/6 distributions + anomaly injection
│
├── data/raw/                           ← CSV exports (git-ignored except .gitkeep)
└── notebooks/
    └── 01_emissions_eda.ipynb          ← EDA + anomaly analysis + compliance report
```

---

## MLflow Experiment

Each `train.py` run logs:

| Category | Content |
|---|---|
| **Parameters** | `contamination`, `n_estimators`, `features`, `train_rows`, `dry_run` |
| **Metrics** | `anomaly_rate`, `score_mean/std/p5/p95`, `sweep_c0.02` … `sweep_c0.15` |
| **Artifacts** | `plots/feature_distributions.png`, `plots/score_distribution.png`, `plots/correlation_heatmap.png`, `data/feature_stats.csv`, `data/contamination_sweep.csv` |
| **Registry** | Promoted to `@staging` and `@production` automatically |

The consumer re-checks the registry every 2 minutes and hot-swaps the model
if a new `@production` version exists — no container restart required.

---

## Airflow DAG — `diesel_emissions_pipeline`

```
validate_raw_emissions
       │
   dbt_run
       │
   dbt_test
       │
check_anomaly_rate ──── anomaly_rate > 10% ──▶ retrain_model ──┐
       │                                                         │
       └────────────── anomaly_rate ≤ 10% ──▶ skip_retrain ────┤
                                                                 │
                                               export_compliance_report
```

Schedule: `@daily` · Retries: 2 · Retry delay: 5 min

---

## Notebook

`notebooks/01_emissions_eda.ipynb` runs standalone — if Postgres is unavailable,
it falls back to synthetic data automatically.

| Section | Content |
|---|---|
| 1 · Raw EDA | Feature distributions, time series, correlation heatmap |
| 2 · Anomaly Detection | NOx×PM2.5 scatter, score KDE, anomaly rate by vehicle, timeline |
| 3 · Regulatory Compliance | EPA tier breakdown, CONAMA timeline, vehicle×hour heatmap |
| 4 · Fleet Report | Per-vehicle compliance table with EPA ✅/❌ and CONAMA ✅/❌ |
| 5 · Lineage | dbt pipeline diagram + design decision table |

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `POSTGRES_USER` | `admin` | PostgreSQL superuser |
| `POSTGRES_PASSWORD` | `admin123` | PostgreSQL password |
| `AIRFLOW_FERNET_KEY` | — | Generate with `python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"` |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` | Kafka broker address (internal) |
| `MLFLOW_TRACKING_URI` | `http://mlflow:5000` | MLflow tracking server |
| `NUM_VEHICLES` | `10` | Simulator — number of fleet vehicles |
| `ANOMALY_RATE` | `0.05` | Simulator — fraction of anomalous readings |
| `CONTAMINATION` | `0.05` | IsolationForest contamination (overrides sweep) |
| `DRY_RUN` | `0` | Set to `1` to train on synthetic data |

---

## License

MIT
