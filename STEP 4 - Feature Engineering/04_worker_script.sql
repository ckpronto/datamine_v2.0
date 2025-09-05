-- This single, memory-optimized query calculates all features for a chunk
-- and inserts them directly into the staging table, eliminating disk I/O.

INSERT INTO feature_engineering_staging
WITH
    -- Step A: Calculate base features, now with a proper spatial join for location_type.
  base_features AS (
    SELECT
      t.*,
      ST_Z(ST_Transform(t.current_position::geometry, 4326)) AS altitude,
      (t.current_speed < 0.5) AS is_stationary,
      COALESCE(zones.zone_name, 'Haul Road / Other') AS location_type
    FROM "02_raw_telemetry_transformed" t
    LEFT JOIN "00_lbp_zones" zones 
      ON ST_Intersects(zones.zone_polygon, ST_Transform(t.current_position::geometry, 4326))
    WHERE t.device_date IN :chunk_list
  ),

  -- Step B: Calculate first-level window functions.
  basic_windows AS (
    SELECT
      *,
      LAG(is_stationary, 1, is_stationary) OVER w AS prev_stationary,
      COALESCE(EXTRACT(EPOCH FROM (timestamp - LAG(timestamp, 1) OVER w)), 0) AS time_delta,
      AVG(load_weight) OVER (w ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS smoothed_load_weight
    FROM base_features
    WINDOW w AS (PARTITION BY device_id, device_date ORDER BY timestamp)
  ),

  -- Step C: Calculate duration block IDs.
  duration_prep AS (
    SELECT
      *,
      SUM(CASE WHEN is_stationary != prev_stationary THEN 1 ELSE 0 END) OVER w AS stationary_block_id
    FROM basic_windows
    WINDOW w AS (PARTITION BY device_id, device_date ORDER BY timestamp)
  )

-- Step D: Final SELECT with explicit casting to prevent data type errors.
SELECT
  timestamp,
  ingested_at,
  raw_event_hash_id::TEXT, -- FIX: Cast to TEXT
  device_id,
  device_date,
  system_engaged,
  parking_brake_applied,
  current_position::GEOGRAPHY, -- FIX: Cast to GEOGRAPHY
  current_speed,
  load_weight,
  state,
  software_state,
  prndl,
  extras,
  location_type,
  is_stationary,
  altitude,
  (altitude - LAG(altitude, 1) OVER w) AS altitude_rate_of_change,
  AVG(current_speed) OVER (w ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS speed_rolling_avg_5s,
  smoothed_load_weight AS load_weight_smoothed,
  (smoothed_load_weight - LAG(smoothed_load_weight, 1) OVER w) AS load_weight_rate_of_change,
  (STDDEV(load_weight) OVER (PARTITION BY device_id) > 1000) AS has_reliable_payload,
  CASE
    WHEN is_stationary THEN SUM(time_delta) OVER (PARTITION BY device_id, device_date, stationary_block_id ORDER BY timestamp)
    ELSE 0
  END AS time_in_stationary_state
FROM duration_prep
WINDOW w AS (PARTITION BY device_id, device_date ORDER BY timestamp);