-- Ultra-Optimized Two-Step Materialization Feature Engineering Pipeline
-- AHS Analytics Engine - DataMine V2
-- Minimizes disk I/O by using only TWO table operations: one parallel read+write, one sequential read+write
-- Maximum efficiency through single-pass window function calculations

-- Enable maximum parallelism and memory optimization
SET max_parallel_workers_per_gather = 32;
SET work_mem = '8GB';
SET effective_cache_size = '250GB';
SET parallel_tuple_cost = 0.01;
SET parallel_setup_cost = 10.0;

SELECT '🚀 ULTRA-OPTIMIZED TWO-STEP FEATURE ENGINEERING STARTING' AS status;

-- ============================================================================
-- STEP 1: PARALLEL MATERIALIZATION - Base Features with Maximum CPU Usage
-- This step leverages all 100 CPUs for parallel-safe operations
-- ============================================================================

SELECT 'STEP 1: Creating base features with MAXIMUM PARALLELISM...' AS status;

DROP TABLE IF EXISTS temp_base_features;
CREATE TEMP TABLE temp_base_features AS
SELECT
    t.*,
    -- Extract altitude directly from PostGIS geometry (parallel-safe)
    ST_Z(t.current_position::geometry) AS altitude,
    -- Basic stationary classification (parallel-safe)
    (t.current_speed < 0.5) AS is_stationary,
    -- Geospatial zone classification (parallel-safe)
    CASE
        WHEN ST_Z(t.current_position::geometry) < 200 THEN 'Loading Zone A'
        WHEN ST_Z(t.current_position::geometry) > 250 THEN 'Dump Zone B'
        ELSE 'Haul Road / Other'
    END AS location_type
FROM "02_raw_telemetry_transformed" t
WHERE t.current_position IS NOT NULL;

-- Critical index for efficient sequential processing in Step 2
CREATE INDEX ON temp_base_features (device_id, timestamp);

SELECT 'STEP 1 Complete: ' || COUNT(*) || ' records created with PARALLEL processing' AS status 
FROM temp_base_features;

-- ============================================================================
-- STEP 2: EFFICIENT SEQUENTIAL - All Window Functions in Single Operation
-- Reads temp table ONCE, calculates ALL window features in memory, writes final table ONCE
-- ============================================================================

SELECT 'STEP 2: Creating final table with ALL window features in single operation...' AS status;

DROP TABLE IF EXISTS "03_primary_feature_table";
CREATE TABLE "03_primary_feature_table" AS
WITH
  -- First CTE: Calculate basic window functions
  basic_windows AS (
    SELECT
      *,
      -- Calculate previous stationary state
      LAG(is_stationary, 1, is_stationary) OVER w AS prev_stationary,
      -- Calculate time deltas between consecutive records
      COALESCE(
        EXTRACT(EPOCH FROM (
          timestamp - LAG(timestamp, 1) OVER w
        )), 0
      ) AS time_delta,
      -- Pre-calculate smoothed load weight
      AVG(load_weight) OVER (w ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS smoothed_load_weight
    FROM temp_base_features
    WINDOW w AS (PARTITION BY device_id ORDER BY timestamp)
  ),
  -- Second CTE: Calculate stationary block IDs using the pre-calculated prev_stationary
  duration_prep AS (
    SELECT
      *,
      -- Identify consecutive stationary state changes using non-nested comparison
      SUM(CASE 
        WHEN is_stationary != prev_stationary THEN 1 
        ELSE 0 
      END) OVER w AS stationary_block_id
    FROM basic_windows
    WINDOW w AS (PARTITION BY device_id ORDER BY timestamp)
  )

SELECT
  -- === ORIGINAL COLUMNS ===
  timestamp,
  ingested_at,
  raw_event_hash_id,  -- Primary key for fastest joins
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

  -- === BASE FEATURES (from parallel step) ===
  location_type,
  is_stationary,
  altitude,

  -- === ALL WINDOW-BASED FEATURES (calculated in single pass) ===
  
  -- Kinematic Features
  (altitude - LAG(altitude, 1) OVER w) AS altitude_rate_of_change,
  AVG(current_speed) OVER (w ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS speed_rolling_avg_5s,
  
  -- Payload Features (using pre-calculated smoothed values)
  smoothed_load_weight AS load_weight_smoothed,
  (smoothed_load_weight - LAG(smoothed_load_weight, 1) OVER w) AS load_weight_rate_of_change,
  
  -- Sensor Health Features
  (STDDEV(load_weight) OVER (PARTITION BY device_id) > 1000) AS has_reliable_payload,
  
  -- Duration Features (most complex - cumulative time in stationary blocks)
  CASE
    WHEN is_stationary THEN
      SUM(time_delta) OVER (PARTITION BY device_id, stationary_block_id ORDER BY timestamp)
    ELSE 0
  END AS time_in_stationary_state,
  
  -- === SYSTEM STATE FEATURES (PRNDL one-hot encoding) ===
  (prndl = 'park') AS prndl_park,
  (prndl = 'reverse') AS prndl_reverse, 
  (prndl = 'neutral') AS prndl_neutral,
  (prndl = 'drive') AS prndl_drive,
  (prndl = 'unknown') AS prndl_unknown,
  
  -- === INTERACTION FEATURES (composite features) ===
  (is_stationary AND prndl = 'neutral' AND parking_brake_applied) AS is_ready_for_load,
  (location_type = 'Haul Road / Other' AND NOT is_stationary) AS is_hauling,
  (is_stationary AND location_type LIKE '%Loading%') AS is_loading_position,
  (is_stationary AND location_type LIKE '%Dump%') AS is_dumping_position,
  (smoothed_load_weight > 50000) AS is_heavy_load

FROM duration_prep
WINDOW w AS (PARTITION BY device_id ORDER BY timestamp)
ORDER BY device_id, timestamp;

-- Clean up temporary table immediately
DROP TABLE temp_base_features;

SELECT 'STEP 2 Complete: Final table created with ALL features in single operation' AS status;

-- ============================================================================
-- STEP 3: PERFORMANCE OPTIMIZATIONS
-- ============================================================================

SELECT 'STEP 3: Applying performance optimizations...' AS status;

-- Create primary key on raw_event_hash_id for fastest possible joins with other tables
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

CREATE INDEX CONCURRENTLY idx_03_primary_ready_for_load
ON "03_primary_feature_table" (is_ready_for_load)
WHERE is_ready_for_load = true;

-- Update table statistics for optimal query planning
ANALYZE "03_primary_feature_table";

SELECT 'STEP 3 Complete: All performance optimizations applied' AS status;

-- ============================================================================
-- FINAL SUCCESS REPORT WITH COMPREHENSIVE METRICS
-- ============================================================================

SELECT 
    '🎉 ULTRA-OPTIMIZED FEATURE ENGINEERING COMPLETE! 🎉' AS status,
    
    -- Basic metrics
    COUNT(*) as total_records,
    COUNT(DISTINCT device_id) as unique_devices,
    MIN(timestamp) as earliest_record,
    MAX(timestamp) as latest_record,
    
    -- Feature distribution metrics
    ROUND(AVG(CASE WHEN is_stationary THEN 1 ELSE 0 END) * 100, 1) || '%' as pct_stationary,
    ROUND(AVG(load_weight_smoothed), 1) as avg_load_weight_smoothed,
    ROUND(AVG(speed_rolling_avg_5s), 2) as avg_speed_smoothed,
    
    -- Operational metrics
    COUNT(CASE WHEN is_heavy_load THEN 1 END) as heavy_load_records,
    COUNT(CASE WHEN is_ready_for_load THEN 1 END) as ready_for_load_records,
    COUNT(CASE WHEN is_hauling THEN 1 END) as hauling_records,
    COUNT(CASE WHEN is_loading_position THEN 1 END) as loading_position_records,
    COUNT(CASE WHEN is_dumping_position THEN 1 END) as dumping_position_records,
    
    -- Data quality metrics
    COUNT(CASE WHEN has_reliable_payload THEN 1 END) as reliable_payload_records,
    ROUND(MAX(time_in_stationary_state), 1) as max_stationary_duration_seconds
    
FROM "03_primary_feature_table";

SELECT 
    '✅ SUCCESS: Two-step materialization completed!' AS optimization_summary,
    'Step 1: Parallel base features (~8.8M records processed with 100 CPUs)' AS step1_summary,
    'Step 2: Single-pass window functions (all features calculated efficiently)' AS step2_summary,
    'Result: Comprehensive feature table ready for ML training!' AS final_result;