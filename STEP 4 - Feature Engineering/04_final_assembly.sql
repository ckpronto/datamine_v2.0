-- This script runs once at the end to assemble the results from the staging table.
DROP TABLE IF EXISTS "03_primary_feature_table";

CREATE TABLE "03_primary_feature_table" AS
SELECT
    -- Original & Engineered Features
    timestamp, ingested_at, raw_event_hash_id, device_id, device_date, system_engaged,
    parking_brake_applied, current_position, current_speed, load_weight, state,
    software_state, prndl, extras, location_type, is_stationary, altitude,
    altitude_rate_of_change, speed_rolling_avg_5s, load_weight_smoothed,
    load_weight_rate_of_change, has_reliable_payload, time_in_stationary_state,
    
    -- PRNDL one-hot encoding
    (prndl = 'park') AS prndl_park,
    (prndl = 'reverse') AS prndl_reverse,
    (prndl = 'neutral') AS prndl_neutral,
    (prndl = 'drive') AS prndl_drive,
    (prndl = 'unknown') AS prndl_unknown,
    
    -- Interaction Features (is_heavy_load must be defined first)
    (load_weight_smoothed > 50000) AS is_heavy_load,
    (is_stationary AND NOT (load_weight_smoothed > 50000)) AS is_ready_for_load,
    ((load_weight_smoothed > 50000) AND NOT is_stationary) AS is_hauling,
    ((location_type LIKE 'Pit%' OR location_type LIKE 'Stockpile%')) AS is_in_loading_zone,
    ((location_type = 'Crusher' OR location_type LIKE 'Stockpile%')) AS is_in_dumping_zone

FROM feature_engineering_staging
ORDER BY device_id, timestamp;

-- Apply final optimizations
ALTER TABLE "03_primary_feature_table" ADD PRIMARY KEY (raw_event_hash_id);

CREATE INDEX idx_03_primary_device_timestamp 
ON "03_primary_feature_table" (device_id, timestamp);

CREATE INDEX idx_03_primary_stationary 
ON "03_primary_feature_table" (is_stationary) 
WHERE is_stationary = true;

CREATE INDEX idx_03_primary_location 
ON "03_primary_feature_table" (location_type);

CREATE INDEX idx_03_primary_heavy_load
ON "03_primary_feature_table" (is_heavy_load)
WHERE is_heavy_load = true;

CREATE INDEX idx_03_primary_ready_for_load
ON "03_primary_feature_table" (is_ready_for_load)
WHERE is_ready_for_load = true;

ANALYZE "03_primary_feature_table";