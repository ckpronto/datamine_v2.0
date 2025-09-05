-- Ultra High-Performance Feature Engineering Pipeline
-- AHS Analytics Engine - DataMine V2
-- Refactored using subqueries instead of CTEs for maximum parallelism
-- Optimized with raw_event_hash_id joins and aggressive parallel processing

-- Enable maximum parallelism and memory usage
SET max_parallel_workers_per_gather = 32;
SET work_mem = '8GB';
SET effective_cache_size = '250GB';
SET parallel_tuple_cost = 0.01;
SET parallel_setup_cost = 10.0;

-- Drop the target table to ensure a fresh build
DROP TABLE IF EXISTS "03_primary_feature_table";

-- Create the feature table using nested subqueries for optimal parallelization
CREATE TABLE "03_primary_feature_table" AS
SELECT
  -- === ORIGINAL COLUMNS ===
  timestamp,
  ingested_at,
  raw_event_hash_id,  -- Primary key for fast joins
  device_id,
  device_date,
  system_engaged,
  parking_brake_applied,
  current_position,
  current_speed,
  load_weight,
  state,
  software_state,
  prndl,
  extras,
  
  -- === ENGINEERED FEATURES ===
  
  -- Geospatial Features
  location_type,
  
  -- Kinematic Features
  is_stationary,
  altitude,
  altitude_rate_of_change,
  speed_rolling_avg_5s,
  
  -- System State Features (PRNDL one-hot)
  (prndl = 'park') AS prndl_park,
  (prndl = 'reverse') AS prndl_reverse, 
  (prndl = 'neutral') AS prndl_neutral,
  (prndl = 'drive') AS prndl_drive,
  (prndl = 'unknown') AS prndl_unknown,
  
  -- Payload Features
  load_weight_smoothed,
  load_weight_rate_of_change,
  
  -- Sensor Health Features
  has_reliable_payload,
  
  -- Duration Features
  CASE
    WHEN is_stationary THEN time_in_block
    ELSE 0
  END AS time_in_stationary_state,
  
  -- Interaction Features (composite)
  (is_stationary AND prndl = 'neutral' AND parking_brake_applied) AS is_ready_for_load,
  (location_type = 'Haul Road / Other' AND NOT is_stationary) AS is_hauling,
  (is_stationary AND location_type LIKE '%Loading%') AS is_loading_position,
  (is_stationary AND location_type LIKE '%Dump%') AS is_dumping_position,
  (load_weight_smoothed > 50000) AS is_heavy_load

FROM (
  -- Final nested subquery with duration calculations
  SELECT *,
    -- Cumulative time within each stationary block
    SUM(time_delta) OVER (
      PARTITION BY device_id, stationary_block_id
      ORDER BY timestamp
    ) AS time_in_block
  FROM (
    -- Duration preparation with stationary block IDs
    SELECT *,
      -- Create unique IDs for consecutive stationary blocks
      SUM(CASE
        WHEN is_stationary != prev_stationary THEN 1
        ELSE 0
      END) OVER (
        PARTITION BY device_id ORDER BY timestamp
      ) AS stationary_block_id
    FROM (
      -- Add previous stationary state and time deltas
      SELECT *,
        LAG(is_stationary, 1, is_stationary) OVER (
          PARTITION BY device_id ORDER BY timestamp
        ) AS prev_stationary,
        -- Calculate time delta between consecutive records (in seconds)
        COALESCE(
          EXTRACT(EPOCH FROM (
            timestamp - LAG(timestamp, 1) OVER (
              PARTITION BY device_id ORDER BY timestamp
            )
          )), 0
        ) AS time_delta
      FROM (
        -- Sensor health and payload features
        SELECT *,
          -- has_reliable_payload flag based on load_weight variance
          (STDDEV(load_weight) OVER (PARTITION BY device_id) > 1000) AS has_reliable_payload,
          -- load_weight_rate_of_change using LAG on smoothed values
          load_weight_smoothed - LAG(load_weight_smoothed, 1) OVER (
            PARTITION BY device_id ORDER BY timestamp
          ) AS load_weight_rate_of_change
        FROM (
          -- Payload smoothing
          SELECT *,
            -- load_weight_smoothed using 5-point rolling average
            AVG(load_weight) OVER (
              PARTITION BY device_id
              ORDER BY timestamp 
              ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
            ) AS load_weight_smoothed
          FROM (
            -- Kinematic features
            SELECT *,
              -- is_stationary boolean
              (current_speed < 0.5) AS is_stationary,
              
              -- altitude_rate_of_change using LAG
              altitude - LAG(altitude, 1) OVER (
                PARTITION BY device_id ORDER BY timestamp
              ) AS altitude_rate_of_change,
              
              -- speed_rolling_avg_5s using window function
              AVG(current_speed) OVER (
                PARTITION BY device_id
                ORDER BY timestamp 
                ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
              ) AS speed_rolling_avg_5s
            FROM (
              -- Base features with geospatial
              SELECT
                t.*,
                -- Extract altitude directly from PostGIS geometry
                ST_Z(t.current_position::geometry) AS altitude,
                -- Simple geospatial classification
                CASE
                  WHEN ST_Z(t.current_position::geometry) < 200 THEN 'Loading Zone A'
                  WHEN ST_Z(t.current_position::geometry) > 250 THEN 'Dump Zone B' 
                  ELSE 'Haul Road / Other'
                END AS location_type
              FROM "02_raw_telemetry_transformed" t
              WHERE current_position IS NOT NULL
            ) AS base_features
          ) AS kinematic_features
        ) AS payload_features
      ) AS sensor_health_features
    ) AS duration_prep
  ) AS duration_blocks
) AS final_features
ORDER BY device_id, timestamp;

-- === PERFORMANCE OPTIMIZATIONS ===

-- Create primary key on raw_event_hash_id for fastest possible joins
ALTER TABLE "03_primary_feature_table" ADD PRIMARY KEY (raw_event_hash_id);

-- Create optimized indexes for common query patterns
CREATE INDEX CONCURRENTLY idx_03_primary_device_timestamp 
ON "03_primary_feature_table" (device_id, timestamp);

CREATE INDEX CONCURRENTLY idx_03_primary_stationary 
ON "03_primary_feature_table" (is_stationary) 
WHERE is_stationary = true;

CREATE INDEX CONCURRENTLY idx_03_primary_location 
ON "03_primary_feature_table" (location_type);

CREATE INDEX CONCURRENTLY idx_03_primary_heavy_load
ON "03_primary_feature_table" (is_heavy_load)
WHERE is_heavy_load = true;

-- Update table statistics for optimal query planning
ANALYZE "03_primary_feature_table";

-- Log completion statistics
SELECT 
    'Feature Engineering Complete!' AS status,
    COUNT(*) as total_records,
    COUNT(DISTINCT device_id) as unique_devices,
    MIN(timestamp) as earliest_record,
    MAX(timestamp) as latest_record,
    ROUND(AVG(CASE WHEN is_stationary THEN 1 ELSE 0 END) * 100, 1) as pct_stationary,
    ROUND(AVG(load_weight_smoothed), 1) as avg_load_weight
FROM "03_primary_feature_table";