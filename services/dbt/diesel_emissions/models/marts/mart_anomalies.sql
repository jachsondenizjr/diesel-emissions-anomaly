{{ config(materialized='table') }}

WITH base AS (
    SELECT
        vehicle_id,
        DATE_TRUNC('hour', ts)                              AS hour,
        AVG(nox_mg_km)                                      AS avg_nox_mg_km,
        MAX(nox_mg_km)                                      AS max_nox_mg_km,
        AVG(pm25_ug_m3)                                     AS avg_pm25_ug_m3,
        MAX(pm25_ug_m3)                                     AS max_pm25_ug_m3,
        COUNT(*)                                            AS total_readings,
        SUM(is_anomaly::INT)                                AS anomaly_count,
        AVG(anomaly_score)                                  AS avg_anomaly_score,
        BOOL_AND(conama_compliant)                          AS conama_compliant,
        MODE() WITHIN GROUP (ORDER BY epa_tier)             AS epa_tier
    FROM {{ source('emissions', 'anomaly_results') }}
    GROUP BY 1, 2
)

SELECT
    *,
    ROUND(anomaly_count * 100.0 / NULLIF(total_readings, 0), 2)   AS anomaly_rate_pct,
    CASE
        WHEN avg_nox_mg_km <= 44  THEN 'EPA Tier 3'
        WHEN avg_nox_mg_km <= 97  THEN 'EPA Tier 2 Bin 5'
        WHEN avg_nox_mg_km <= 270 THEN 'EPA Tier 2 Bin 9'
        ELSE                           'Non-compliant'
    END                                                            AS epa_classification,
    avg_pm25_ug_m3 <= 25                                           AS conama_pm25_ok
FROM base
