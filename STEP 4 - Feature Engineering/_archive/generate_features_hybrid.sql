-- Hybrid Materialization Feature Engineering Pipeline
-- AHS Analytics Engine - DataMine V2
-- This approach uses temporary tables to maximize parallel execution while handling window functions efficiently

-- Enable maximum parallelism for parallel-safe operations
SET max_parallel_workers_per_gather = 32;
SET work_mem = '8GB';
SET effective_cache_size = '250GB';
SET parallel_tuple_cost = 0.01;
SET parallel_setup_cost = 10.0;

-- ============================================================================
-- STEP 1: Create Base Table with Maximum Parallelism
-- This step processes ~8.8M records using all available CPU cores
-- ============================================================================

SELECT 'STEP 1: Creating base features table with parallel processing...' AS status;

DROP TABLE IF EXISTS temp_base_features;
CREATE TEMP TABLE temp_base_features AS
SELECT
    t.*,
    -- Extract altitude directly from PostGIS geometry (parallel-safe)
    ST_Z(t.current_position::geometry) AS altitude,
    -- Geospatial classification (parallel-safe)
    CASE
        WHEN ST_Z(t.current_position::geometry) < 200 THEN 'Loading Zone A'
        WHEN ST_Z(t.current_position::geometry) > 250 THEN 'Dump Zone B'
        ELSE 'Haul Road / Other'
    END AS location_type,
    -- Basic boolean features (parallel-safe)
    (current_speed < 0.5) AS is_stationary
FROM "02_raw_telemetry_transformed" t
WHERE t.current_position IS NOT NULL;

-- Create critical indexes on temp table for efficient window function processing
CREATE INDEX ON temp_base_features (device_id, timestamp);
CREATE INDEX ON temp_base_features (raw_event_hash_id);

SELECT 'STEP 1 Complete: Base table created with ' || COUNT(*) || ' records' AS status
FROM temp_base_features;

-- ============================================================================
-- STEP 2: Add Window Function Features Sequentially (Single-threaded but Fast)
-- These operations are memory-bound and very efficient on the indexed temp table
-- ============================================================================

SELECT 'STEP 2: Adding kinematic features...' AS status;

-- A. Altitude rate of change
ALTER TABLE temp_base_features ADD COLUMN altitude_rate_of_change DOUBLE PRECISION;
UPDATE temp_base_features 
SET altitude_rate_of_change = (
    altitude - LAG(altitude, 1) OVER (PARTITION BY device_id ORDER BY timestamp)
);

-- B. Speed rolling average (5-point window)
ALTER TABLE temp_base_features ADD COLUMN speed_rolling_avg_5s DOUBLE PRECISION;
UPDATE temp_base_features 
SET speed_rolling_avg_5s = (
    AVG(current_speed) OVER (
        PARTITION BY device_id 
        ORDER BY timestamp 
        ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
    )
);

SELECT 'STEP 2A Complete: Kinematic features added' AS status;

-- ============================================================================
-- STEP 3: Add Payload Features
-- ============================================================================

SELECT 'STEP 3: Adding payload features...' AS status;

-- C. Load weight smoothed (5-point rolling average)
ALTER TABLE temp_base_features ADD COLUMN load_weight_smoothed DOUBLE PRECISION;
UPDATE temp_base_features 
SET load_weight_smoothed = (
    AVG(load_weight) OVER (
        PARTITION BY device_id 
        ORDER BY timestamp 
        ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
    )
);

-- D. Load weight rate of change
ALTER TABLE temp_base_features ADD COLUMN load_weight_rate_of_change DOUBLE PRECISION;
UPDATE temp_base_features 
SET load_weight_rate_of_change = (
    load_weight_smoothed - LAG(load_weight_smoothed, 1) OVER (
        PARTITION BY device_id ORDER BY timestamp
    )
);

SELECT 'STEP 3 Complete: Payload features added' AS status;

-- ============================================================================
-- STEP 4: Add Sensor Health Features
-- ============================================================================

SELECT 'STEP 4: Adding sensor health features...' AS status;

-- E. Sensor reliability flag (device-level standard deviation)
ALTER TABLE temp_base_features ADD COLUMN has_reliable_payload BOOLEAN;
UPDATE temp_base_features 
SET has_reliable_payload = (
    STDDEV(load_weight) OVER (PARTITION BY device_id) > 1000
);

SELECT 'STEP 4 Complete: Sensor health features added' AS status;

-- ============================================================================
-- STEP 5: Add Duration Features (Most Complex Window Functions)
-- ============================================================================

SELECT 'STEP 5: Adding duration features...' AS status;

-- F. Identify stationary state changes
ALTER TABLE temp_base_features ADD COLUMN prev_stationary BOOLEAN;
UPDATE temp_base_features 
SET prev_stationary = LAG(is_stationary, 1, is_stationary) OVER (
    PARTITION BY device_id ORDER BY timestamp
);

-- G. Calculate time deltas
ALTER TABLE temp_base_features ADD COLUMN time_delta DOUBLE PRECISION;
UPDATE temp_base_features 
SET time_delta = COALESCE(
    EXTRACT(EPOCH FROM (
        timestamp - LAG(timestamp, 1) OVER (
            PARTITION BY device_id ORDER BY timestamp
        )
    )), 0
);

-- H. Create stationary block IDs
ALTER TABLE temp_base_features ADD COLUMN stationary_block_id INTEGER;
UPDATE temp_base_features 
SET stationary_block_id = SUM(
    CASE WHEN is_stationary != prev_stationary THEN 1 ELSE 0 END
) OVER (PARTITION BY device_id ORDER BY timestamp);

-- I. Calculate time in stationary state
ALTER TABLE temp_base_features ADD COLUMN time_in_stationary_state DOUBLE PRECISION;
UPDATE temp_base_features 
SET time_in_stationary_state = CASE
    WHEN is_stationary THEN
        SUM(time_delta) OVER (
            PARTITION BY device_id, stationary_block_id
            ORDER BY timestamp
        )
    ELSE 0
END;

SELECT 'STEP 5 Complete: Duration features added' AS status;

-- ============================================================================
-- STEP 6: Create Final Table with All Features (Parallel-Safe Final Step)
-- ============================================================================

SELECT 'STEP 6: Creating final feature table...' AS status;

DROP TABLE IF EXISTS "03_primary_feature_table";
CREATE TABLE "03_primary_feature_table" AS
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
    
    -- Geospatial Features
    location_type,
    
    -- Kinematic Features
    is_stationary,
    altitude,
    altitude_rate_of_change,
    speed_rolling_avg_5s,
    
    -- System State Features (PRNDL one-hot encoding)
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
    time_in_stationary_state,
    
    -- Interaction Features (composite features computed on the fly)
    (is_stationary AND prndl = 'neutral' AND parking_brake_applied) AS is_ready_for_load,
    (location_type = 'Haul Road / Other' AND NOT is_stationary) AS is_hauling,
    (is_stationary AND location_type LIKE '%Loading%') AS is_loading_position,
    (is_stationary AND location_type LIKE '%Dump%') AS is_dumping_position,
    (load_weight_smoothed > 50000) AS is_heavy_load
    
FROM temp_base_features
ORDER BY device_id, timestamp;

-- Clean up temporary table
DROP TABLE temp_base_features;

SELECT 'STEP 6 Complete: Final table created' AS status;

-- ============================================================================
-- STEP 7: Performance Optimizations
-- ============================================================================

SELECT 'STEP 7: Adding performance optimizations...' AS status;

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

SELECT 'STEP 7 Complete: All optimizations applied' AS status;

-- ============================================================================
-- FINAL SUMMARY: Success Report
-- ============================================================================

SELECT 
    '🎉 HYBRID MATERIALIZATION FEATURE ENGINEERING COMPLETE!' AS status,
    COUNT(*) as total_records,
    COUNT(DISTINCT device_id) as unique_devices,
    MIN(timestamp) as earliest_record,
    MAX(timestamp) as latest_record,
    ROUND(AVG(CASE WHEN is_stationary THEN 1 ELSE 0 END) * 100, 1) as pct_stationary,
    ROUND(AVG(load_weight_smoothed), 1) as avg_load_weight,
    COUNT(CASE WHEN is_heavy_load THEN 1 END) as heavy_load_records,
    COUNT(CASE WHEN is_ready_for_load THEN 1 END) as ready_for_load_records
FROM "03_primary_feature_table";