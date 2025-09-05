-- Optimized migration script using create-and-swap method
-- This avoids slow UPDATE operations and table bloat by creating a new table
-- and swapping it atomically

-- Step 1: Create the new table with the complete desired schema
CREATE TABLE "02_raw_telemetry_transformed_new" (
    timestamp TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    raw_event_hash_id CHAR(64) PRIMARY KEY,
    device_id TEXT NOT NULL,
    device_date TEXT NOT NULL,  -- New field as NOT NULL from the start
    system_engaged BOOLEAN,
    parking_brake_applied BOOLEAN,
    current_position GEOGRAPHY(POINTZ, 4326),
    current_speed DOUBLE PRECISION,
    load_weight DOUBLE PRECISION,
    state telemetry_state_enum DEFAULT 'unknown'::telemetry_state_enum,
    software_state software_state_enum DEFAULT 'unknown'::software_state_enum,
    prndl prndl_enum DEFAULT 'unknown'::prndl_enum,
    extras JSONB
);

-- Step 2: Efficiently copy and transform data from old table to new table
-- This single INSERT INTO...SELECT is much faster than row-by-row UPDATE
INSERT INTO "02_raw_telemetry_transformed_new" (
    timestamp, 
    ingested_at, 
    raw_event_hash_id, 
    device_id, 
    device_date,  -- Generate this field on-the-fly
    system_engaged, 
    parking_brake_applied, 
    current_position, 
    current_speed, 
    load_weight, 
    state, 
    software_state, 
    prndl, 
    extras
)
SELECT 
    timestamp,
    ingested_at,
    raw_event_hash_id,
    device_id,
    device_id || '_' || DATE(timestamp)::TEXT AS device_date,  -- Generate device_date
    system_engaged,
    parking_brake_applied,
    current_position,
    current_speed,
    load_weight,
    state,
    software_state,
    prndl,
    extras
FROM "02_raw_telemetry_transformed";

-- Step 3: Create all indexes on the new table
-- (This is much faster on the new table before it gets traffic)

-- Primary key is already created, now add all other indexes
CREATE INDEX idx_02_raw_telemetry_transformed_timestamp_new 
ON "02_raw_telemetry_transformed_new" (timestamp);

CREATE INDEX idx_02_raw_telemetry_transformed_device_id_new 
ON "02_raw_telemetry_transformed_new" (device_id);

CREATE INDEX idx_02_raw_telemetry_transformed_device_timestamp_new 
ON "02_raw_telemetry_transformed_new" (device_id, timestamp);

CREATE INDEX idx_02_raw_telemetry_transformed_device_date_new 
ON "02_raw_telemetry_transformed_new" (device_date);

CREATE INDEX idx_02_raw_telemetry_transformed_device_date_timestamp_new 
ON "02_raw_telemetry_transformed_new" (device_date, timestamp);

CREATE INDEX idx_02_raw_telemetry_transformed_ingested_at_new 
ON "02_raw_telemetry_transformed_new" (ingested_at);

CREATE INDEX idx_02_raw_telemetry_transformed_state_new 
ON "02_raw_telemetry_transformed_new" (state);

CREATE INDEX idx_02_raw_telemetry_transformed_software_state_new 
ON "02_raw_telemetry_transformed_new" (software_state);

CREATE INDEX idx_02_raw_telemetry_transformed_position_gist_new 
ON "02_raw_telemetry_transformed_new" USING GIST (current_position);

CREATE INDEX idx_02_raw_telemetry_transformed_extras_gin_new 
ON "02_raw_telemetry_transformed_new" USING GIN (extras);

-- Display progress before the atomic swap
SELECT 
    'New table created and populated successfully' as status,
    COUNT(*) as total_rows_in_new_table,
    COUNT(DISTINCT device_date) as unique_device_dates,
    MIN(device_date) as earliest_device_date,
    MAX(device_date) as latest_device_date
FROM "02_raw_telemetry_transformed_new";