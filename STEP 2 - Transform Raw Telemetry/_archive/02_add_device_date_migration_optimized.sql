-- Optimized migration script to add device_date field to existing table
-- This breaks the operation into smaller, faster steps

-- Step 1: Add the device_date column as nullable
ALTER TABLE "02_raw_telemetry_transformed" 
ADD COLUMN device_date TEXT;