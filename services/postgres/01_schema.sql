-- ============================================================
-- Schema: emissions database
-- Applied automatically by the Postgres Docker entrypoint.
-- POSTGRES_DB=emissions, so this runs against that database.
-- ============================================================

-- ── Raw sensor readings from the fleet ──────────────────────
CREATE TABLE IF NOT EXISTS raw_emissions (
    id            BIGSERIAL     PRIMARY KEY,
    vehicle_id    TEXT          NOT NULL,
    ts            TIMESTAMPTZ   NOT NULL,
    nox_mg_km     NUMERIC(10,4) NOT NULL CHECK (nox_mg_km  >= 0),
    pm25_ug_m3    NUMERIC(10,4) NOT NULL CHECK (pm25_ug_m3 >= 0),
    speed_kmh     NUMERIC(8,2)  NOT NULL CHECK (speed_kmh  >= 0),
    engine_temp_c NUMERIC(8,2)  NOT NULL,
    fuel_rate_lh  NUMERIC(8,4)  NOT NULL CHECK (fuel_rate_lh >= 0),
    lat           NUMERIC(10,6),
    lon           NUMERIC(10,6),
    _ingested_at  TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE  raw_emissions             IS 'Raw NOx/PM2.5 readings ingested via Kafka → FastAPI';
COMMENT ON COLUMN raw_emissions.nox_mg_km  IS 'Nitrogen oxides — EPA/CONAMA limit: 270 mg/km (Tier2-Bin9)';
COMMENT ON COLUMN raw_emissions.pm25_ug_m3 IS 'Particulate matter PM2.5 — CONAMA limit: 25 µg/m³';

-- ── Isolation Forest scoring results ────────────────────────
CREATE TABLE IF NOT EXISTS anomaly_results (
    id               BIGSERIAL        PRIMARY KEY,
    vehicle_id       TEXT             NOT NULL,
    ts               TIMESTAMPTZ      NOT NULL,
    nox_mg_km        NUMERIC(10,4)    NOT NULL,
    pm25_ug_m3       NUMERIC(10,4)    NOT NULL,
    anomaly_score    DOUBLE PRECISION NOT NULL,  -- negative = more anomalous
    is_anomaly       BOOLEAN          NOT NULL DEFAULT FALSE,
    epa_tier         TEXT             NOT NULL,  -- Tier3 | Tier2-Bin5 | Tier2-Bin9 | Non-compliant
    conama_compliant BOOLEAN          NOT NULL,  -- pm25_ug_m3 <= 25
    created_at       TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    UNIQUE (vehicle_id, ts)
);

COMMENT ON TABLE  anomaly_results                  IS 'Isolation Forest scores computed by the ml-consumer service';
COMMENT ON COLUMN anomaly_results.anomaly_score    IS 'decision_function output: lower score = more anomalous';
COMMENT ON COLUMN anomaly_results.epa_tier         IS 'EPA Tier classification based on nox_mg_km';
COMMENT ON COLUMN anomaly_results.conama_compliant IS 'CONAMA Resolution 18/1986 — PM2.5 <= 25 µg/m³';

-- ── Indexes ──────────────────────────────────────────────────
-- Time-series queries (most common in dashboard + dbt)
CREATE INDEX IF NOT EXISTS idx_raw_ts
    ON raw_emissions (ts DESC);

CREATE INDEX IF NOT EXISTS idx_raw_vehicle_ts
    ON raw_emissions (vehicle_id, ts DESC);

CREATE INDEX IF NOT EXISTS idx_anomaly_ts
    ON anomaly_results (ts DESC);

CREATE INDEX IF NOT EXISTS idx_anomaly_vehicle_ts
    ON anomaly_results (vehicle_id, ts DESC);

-- Partial index: anomaly lookups are filtered on is_anomaly=TRUE
CREATE INDEX IF NOT EXISTS idx_anomaly_flags
    ON anomaly_results (ts DESC)
    WHERE is_anomaly = TRUE;

-- EPA compliance reporting
CREATE INDEX IF NOT EXISTS idx_anomaly_epa_tier
    ON anomaly_results (epa_tier, ts DESC);
