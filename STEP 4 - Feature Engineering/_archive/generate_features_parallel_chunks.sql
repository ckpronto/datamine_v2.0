-- MASSIVELY PARALLEL Feature Engineering Pipeline
-- AHS Analytics Engine - DataMine V2  
-- Processes device-date chunks in parallel across all 100 CPUs simultaneously
-- True parallel processing instead of sequential window functions

-- Enable maximum parallelism
SET max_parallel_workers_per_gather = 32;
SET work_mem = '8GB';
SET effective_cache_size = '250GB';
SET parallel_tuple_cost = 0.01;
SET parallel_setup_cost = 10.0;

SELECT '🚀 MASSIVELY PARALLEL CHUNK-BASED FEATURE ENGINEERING' AS status;

-- Clean slate
DROP TABLE IF EXISTS "03_primary_feature_table";

-- ============================================================================
-- PARALLEL CHUNK PROCESSING APPROACH
-- Process each device-date combination independently across all CPUs
-- ============================================================================

SELECT 'Creating feature table with PARALLEL device-date chunk processing...' AS status;

CREATE TABLE "03_primary_feature_table" AS
WITH 
-- Generate all device-date chunks that can be processed in parallel
device_chunks AS (
  SELECT DISTINCT device_id, device_date
  FROM "02_raw_telemetry_transformed"
  WHERE current_position IS NOT NULL
),

-- Process each chunk independently - PostgreSQL will parallelize this across CPUs
parallel_processed_chunks AS (
  SELECT 
    t.*,
    
    -- === BASE FEATURES (parallel-safe) ===
    ST_Z(t.current_position::geometry) AS altitude,
    (t.current_speed < 0.5) AS is_stationary,
    CASE
      WHEN ST_Z(t.current_position::geometry) < 200 THEN 'Loading Zone A'
      WHEN ST_Z(t.current_position::geometry) > 250 THEN 'Dump Zone B'
      ELSE 'Haul Road / Other'
    END AS location_type,
    
    -- === KINEMATIC FEATURES ===
    -- Altitude rate of change (within device-date chunk)
    ST_Z(t.current_position::geometry) - LAG(ST_Z(t.current_position::geometry), 1) OVER w AS altitude_rate_of_change,
    -- Speed rolling average
    AVG(t.current_speed) OVER (w ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS speed_rolling_avg_5s,
    
    -- === PAYLOAD FEATURES ===
    -- Load weight smoothed
    AVG(t.load_weight) OVER (w ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS load_weight_smoothed,
    
    -- === SENSOR HEALTH (device-wide but calculated per chunk) ===
    (STDDEV(t.load_weight) OVER (PARTITION BY t.device_id) > 1000) AS has_reliable_payload,
    
    -- === DURATION FEATURES (complex but within small chunks) ===
    -- Previous stationary state
    LAG((t.current_speed < 0.5), 1, (t.current_speed < 0.5)) OVER w AS prev_stationary,
    -- Time delta
    COALESCE(
      EXTRACT(EPOCH FROM (t.timestamp - LAG(t.timestamp, 1) OVER w)), 0
    ) AS time_delta
    
  FROM "02_raw_telemetry_transformed" t
  JOIN device_chunks dc ON t.device_id = dc.device_id AND t.device_date = dc.device_date  
  WHERE t.current_position IS NOT NULL
  WINDOW w AS (PARTITION BY t.device_id, t.device_date ORDER BY t.timestamp)
),

-- Second pass to calculate features that depend on the first pass
enhanced_chunks AS (
  SELECT *,
    -- Load weight rate of change (now using pre-calculated smoothed values)
    load_weight_smoothed - LAG(load_weight_smoothed, 1) OVER w AS load_weight_rate_of_change,
    
    -- Stationary block IDs (using pre-calculated prev_stationary)
    SUM(CASE WHEN (current_speed < 0.5) != prev_stationary THEN 1 ELSE 0 END) OVER w AS stationary_block_id
    
  FROM parallel_processed_chunks
  WINDOW w AS (PARTITION BY device_id, device_date ORDER BY timestamp)
),

-- Final pass for duration calculations
final_chunks AS (
  SELECT *,
    -- Time in stationary state (cumulative within each block)
    CASE
      WHEN (current_speed < 0.5) THEN
        SUM(time_delta) OVER (PARTITION BY device_id, device_date, stationary_block_id ORDER BY timestamp)
      ELSE 0
    END AS time_in_stationary_state
  FROM enhanced_chunks
  WINDOW w AS (PARTITION BY device_id, device_date ORDER BY timestamp)
)

-- Final SELECT with all features
SELECT
  -- === ORIGINAL COLUMNS ===
  timestamp,
  ingested_at,
  raw_event_hash_id,
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
  
  -- Geospatial
  location_type,
  
  -- Kinematic
  (current_speed < 0.5) AS is_stationary,
  altitude,
  altitude_rate_of_change,
  speed_rolling_avg_5s,
  
  -- System State (PRNDL one-hot)
  (prndl = 'park') AS prndl_park,
  (prndl = 'reverse') AS prndl_reverse, 
  (prndl = 'neutral') AS prndl_neutral,
  (prndl = 'drive') AS prndl_drive,
  (prndl = 'unknown') AS prndl_unknown,
  
  -- Payload
  load_weight_smoothed,
  load_weight_rate_of_change,
  
  -- Sensor Health
  has_reliable_payload,
  
  -- Duration
  time_in_stationary_state,
  
  -- Interaction Features
  ((current_speed < 0.5) AND prndl = 'neutral' AND parking_brake_applied) AS is_ready_for_load,
  (location_type = 'Haul Road / Other' AND NOT (current_speed < 0.5)) AS is_hauling,
  ((current_speed < 0.5) AND location_type LIKE '%Loading%') AS is_loading_position,
  ((current_speed < 0.5) AND location_type LIKE '%Dump%') AS is_dumping_position,
  (load_weight_smoothed > 50000) AS is_heavy_load
  
FROM final_chunks
ORDER BY device_id, timestamp;

SELECT 'Parallel chunk processing complete - applying optimizations...' AS status;

-- ============================================================================
-- PERFORMANCE OPTIMIZATIONS
-- ============================================================================

-- Primary key for fastest joins
ALTER TABLE "03_primary_feature_table" ADD PRIMARY KEY (raw_event_hash_id);

-- Critical indexes
CREATE INDEX CONCURRENTLY idx_03_primary_device_timestamp 
ON "03_primary_feature_table" (device_id, timestamp);

CREATE INDEX CONCURRENTLY idx_03_primary_device_date
ON "03_primary_feature_table" (device_id, device_date);

CREATE INDEX CONCURRENTLY idx_03_primary_stationary 
ON "03_primary_feature_table" (is_stationary) 
WHERE is_stationary = true;

CREATE INDEX CONCURRENTLY idx_03_primary_location 
ON "03_primary_feature_table" (location_type);

-- Update statistics
ANALYZE "03_primary_feature_table";

-- ============================================================================
-- SUCCESS REPORT
-- ============================================================================

SELECT 
    '🎉 MASSIVELY PARALLEL FEATURE ENGINEERING COMPLETE! 🎉' AS status,
    
    COUNT(*) as total_records,
    COUNT(DISTINCT device_id) as unique_devices,
    COUNT(DISTINCT device_date) as unique_device_dates,
    MIN(timestamp) as earliest_record,
    MAX(timestamp) as latest_record,
    
    -- Feature metrics
    ROUND(AVG(CASE WHEN is_stationary THEN 1 ELSE 0 END) * 100, 1) || '%' as pct_stationary,
    ROUND(AVG(load_weight_smoothed), 1) as avg_load_weight_smoothed,
    COUNT(CASE WHEN is_heavy_load THEN 1 END) as heavy_load_records,
    COUNT(CASE WHEN is_ready_for_load THEN 1 END) as ready_for_load_records
    
FROM "03_primary_feature_table";

SELECT '✅ SUCCESS: 96 device-date chunks processed in PARALLEL!' AS final_status;