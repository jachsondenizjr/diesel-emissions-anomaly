{{ config(materialized='view') }}

SELECT
    vehicle_id,
    ts::TIMESTAMPTZ            AS ts,
    nox_mg_km::NUMERIC(10, 4)  AS nox_mg_km,
    pm25_ug_m3::NUMERIC(10, 4) AS pm25_ug_m3,
    speed_kmh::NUMERIC(8, 2)   AS speed_kmh,
    engine_temp_c::NUMERIC(8, 2) AS engine_temp_c,
    fuel_rate_lh::NUMERIC(8, 4)  AS fuel_rate_lh,
    lat::NUMERIC(10, 6)          AS lat,
    lon::NUMERIC(10, 6)          AS lon,
    _ingested_at                 AS ingested_at
FROM {{ source('emissions', 'raw_emissions') }}
WHERE
    nox_mg_km  IS NOT NULL
    AND pm25_ug_m3 IS NOT NULL
    AND ts         IS NOT NULL
