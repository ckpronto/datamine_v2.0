-- High-Performance Feature Engineering Pipeline
-- AHS Analytics Engine - DataMine V2
-- This SQL script generates all features directly in the database using parallel processing
-- Optimized with raw_event_hash_id joins for maximum performance

-- Enable parallel execution and optimize memory
SET max_parallel_workers_per_gather = 8;
SET work_mem = '4GB';
SET effective_cache_size = '200GB';

-- Drop the target table to ensure a fresh build
DROP TABLE IF EXISTS "03_primary_feature_table";

-- Create comprehensive feature table in a single parallel operation
CREATE TABLE "03_primary_feature_table" AS
WITH
  -- Step 1: Base data with geospatial features
  base_features AS (
    SELECT
      t.*,
      -- Extract altitude directly from PostGIS geometry
      ST_Z(t.current_position::geometry) AS altitude,
      -- Simple geospatial classification (can be enhanced with actual operational_zones table)
      CASE
        WHEN ST_Z(t.current_position::geometry) < 200 THEN 'Loading Zone A'
        WHEN ST_Z(t.current_position::geometry) > 250 THEN 'Dump Zone B' 
        ELSE 'Haul Road / Other'
      END AS location_type
    FROM "02_raw_telemetry_transformed" t
    WHERE current_position IS NOT NULL
  ),
  
  -- Step 2: Kinematic features using optimized window functions
  kinematic_features AS (
    SELECT
      *,
      -- A. is_stationary
      (current_speed < 0.5) AS is_stationary,
      
      -- B. altitude_rate_of_change using LAG window function
      altitude - LAG(altitude, 1) OVER (
        PARTITION BY device_id
        ORDER BY timestamp
      ) AS altitude_rate_of_change,
      
      -- C. speed_rolling_avg_5s using optimized window
      AVG(current_speed) OVER (
        PARTITION BY device_id
        ORDER BY timestamp 
        ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
      ) AS speed_rolling_avg_5s
    FROM base_features
  ),
  
  -- Step 3: Payload features with smoothing
  payload_features AS (
    SELECT
      *,
      -- load_weight_smoothed using 5-point rolling average
      AVG(load_weight) OVER (
        PARTITION BY device_id
        ORDER BY timestamp 
        ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
      ) AS load_weight_smoothed
    FROM kinematic_features
  ),
  
  -- Step 4: Add payload rate of change (fix nested window functions)
  payload_with_rate AS (
    SELECT
      *,
      -- load_weight_rate_of_change using LAG on the smoothed column
      load_weight_smoothed - LAG(load_weight_smoothed, 1) OVER (
        PARTITION BY device_id
        ORDER BY timestamp
      ) AS load_weight_rate_of_change
    FROM payload_features
  ),
  
  -- Step 5: Sensor health flags using device-level aggregation
  sensor_health_features AS (
    SELECT
      *,
      -- has_reliable_payload flag based on load_weight variance
      (STDDEV(load_weight) OVER (PARTITION BY device_id) > 1000) AS has_reliable_payload
    FROM payload_with_rate
  ),
  
  -- Step 6: PRNDL one-hot encoding
  prndl_features AS (
    SELECT
      *,
      (prndl = 'park') AS prndl_park,
      (prndl = 'reverse') AS prndl_reverse, 
      (prndl = 'neutral') AS prndl_neutral,
      (prndl = 'drive') AS prndl_drive,
      (prndl = 'unknown') AS prndl_unknown
    FROM sensor_health_features
  ),
  
  -- Step 7: Duration features - identify stationary state changes  
  duration_prep AS (
    SELECT
      *,
      -- First identify where stationary state changes
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
    FROM prndl_features
  ),
  
  -- Step 8: Create stationary block IDs
  duration_blocks AS (
    SELECT
      *,
      -- Create unique IDs for consecutive stationary blocks
      SUM(CASE
        WHEN is_stationary != prev_stationary THEN 1
        ELSE 0
      END) OVER (
        PARTITION BY device_id ORDER BY timestamp
      ) AS stationary_block_id
    FROM duration_prep
  ),
  
  -- Step 9: Calculate cumulative time within stationary blocks
  final_features AS (
    SELECT
      *,
      -- Cumulative time within each stationary block
      SUM(time_delta) OVER (
        PARTITION BY device_id, stationary_block_id
        ORDER BY timestamp
      ) AS time_in_block
    FROM duration_blocks
  )

-- Final selection with all original and engineered features
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
  prndl_park,
  prndl_reverse,
  prndl_neutral,
  prndl_drive,
  prndl_unknown,
  
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

FROM final_features
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

-- Log completion
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT device_id) as unique_devices,
    MIN(timestamp) as earliest_record,
    MAX(timestamp) as latest_record
FROM "03_primary_feature_table";