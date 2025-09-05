-- SQL Script to create 02_raw_telemetry_transformed table
-- This script creates the transformed telemetry table with PostGIS geography support

-- Enable PostGIS extension if not already enabled
CREATE EXTENSION IF NOT EXISTS postgis;

-- Create enum types for the transformed table
CREATE TYPE telemetry_state_enum AS ENUM (
    'unknown',
    'idle',
    'active',
    'loading',
    'hauling',
    'dumping',
    'maintenance',
    'stopped'
);

CREATE TYPE software_state_enum AS ENUM (
    'unknown',
    'manual',
    'autonomous',
    'intervention',
    'fault',
    'disabled',
    'calibrating'
);

CREATE TYPE prndl_enum AS ENUM (
    'unknown',
    'park',
    'reverse',
    'neutral',
    'drive',
    'low'
);

-- Create function to generate SHA-256 hash from device_id + timestamp
CREATE OR REPLACE FUNCTION generate_raw_event_hash_id(device_id TEXT, timestamp_val TIMESTAMPTZ)
RETURNS CHAR(64)
LANGUAGE plpgsql
IMMUTABLE
AS $$
BEGIN
    RETURN encode(digest(device_id || '|' || timestamp_val::TEXT, 'sha256'), 'hex');
END;
$$;

-- Create the 02_raw_telemetry_transformed table
CREATE TABLE "02_raw_telemetry_transformed" (
    timestamp TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    raw_event_hash_id CHAR(64) PRIMARY KEY,
    device_id TEXT NOT NULL,
    device_date TEXT NOT NULL,
    system_engaged BOOLEAN,
    parking_brake_applied BOOLEAN,
    current_position GEOGRAPHY(POINTZ, 4326),
    current_speed DOUBLE PRECISION,
    load_weight DOUBLE PRECISION,
    state telemetry_state_enum DEFAULT 'unknown',
    software_state software_state_enum DEFAULT 'unknown', 
    prndl prndl_enum DEFAULT 'unknown',
    extras JSONB
);

-- Create indexes for common query patterns
CREATE INDEX idx_02_raw_telemetry_transformed_timestamp 
ON "02_raw_telemetry_transformed" (timestamp);

CREATE INDEX idx_02_raw_telemetry_transformed_device_id 
ON "02_raw_telemetry_transformed" (device_id);

CREATE INDEX idx_02_raw_telemetry_transformed_device_timestamp 
ON "02_raw_telemetry_transformed" (device_id, timestamp);

CREATE INDEX idx_02_raw_telemetry_transformed_device_date 
ON "02_raw_telemetry_transformed" (device_date);

CREATE INDEX idx_02_raw_telemetry_transformed_device_date_timestamp 
ON "02_raw_telemetry_transformed" (device_date, timestamp);

CREATE INDEX idx_02_raw_telemetry_transformed_ingested_at 
ON "02_raw_telemetry_transformed" (ingested_at);

CREATE INDEX idx_02_raw_telemetry_transformed_state 
ON "02_raw_telemetry_transformed" (state);

CREATE INDEX idx_02_raw_telemetry_transformed_software_state 
ON "02_raw_telemetry_transformed" (software_state);

-- Create spatial index for geography field
CREATE INDEX idx_02_raw_telemetry_transformed_position_gist 
ON "02_raw_telemetry_transformed" USING GIST (current_position);

-- Create index for JSONB extras field
CREATE INDEX idx_02_raw_telemetry_transformed_extras_gin 
ON "02_raw_telemetry_transformed" USING GIN (extras);

-- Add comments for documentation
COMMENT ON TABLE "02_raw_telemetry_transformed" IS 'Transformed telemetry data from raw telemetry with proper data types and PostGIS geography support';
COMMENT ON COLUMN "02_raw_telemetry_transformed".raw_event_hash_id IS 'SHA-256 hash of device_id + timestamp for immutable unique identification';
COMMENT ON COLUMN "02_raw_telemetry_transformed".current_position IS 'PostGIS geography field in POINTZ format (3D point with SRID 4326)';
COMMENT ON COLUMN "02_raw_telemetry_transformed".timestamp IS 'Original timestamp from raw telemetry data';
COMMENT ON COLUMN "02_raw_telemetry_transformed".ingested_at IS 'Timestamp when the row was transformed and inserted';
COMMENT ON COLUMN "02_raw_telemetry_transformed".device_date IS 'Combined device_id and date (YYYY-MM-DD) for efficient device-day partitioning and sorting';