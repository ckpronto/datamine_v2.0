-- Migration script to add device_date field to existing 02_raw_telemetry_transformed table
-- This is a one-time operation to add the device_date field without re-running the entire transformation

-- Start transaction for atomic operation
BEGIN;

-- Step 1: Add the device_date column as nullable first
ALTER TABLE "02_raw_telemetry_transformed" 
ADD COLUMN device_date TEXT;

-- Step 2: Populate the device_date field from existing data
-- Use batch updates for better performance on large tables
UPDATE "02_raw_telemetry_transformed" 
SET device_date = device_id || '_' || DATE(timestamp)::TEXT
WHERE device_date IS NULL;

-- Step 3: Make the column NOT NULL after it's populated
ALTER TABLE "02_raw_telemetry_transformed" 
ALTER COLUMN device_date SET NOT NULL;

-- Step 4: Create indexes for performance
CREATE INDEX idx_02_raw_telemetry_transformed_device_date 
ON "02_raw_telemetry_transformed" (device_date);

CREATE INDEX idx_02_raw_telemetry_transformed_device_date_timestamp 
ON "02_raw_telemetry_transformed" (device_date, timestamp);

-- Step 5: Add column comment
COMMENT ON COLUMN "02_raw_telemetry_transformed".device_date 
IS 'Combined device_id and date (YYYY-MM-DD) for efficient device-day partitioning and sorting';

-- Step 6: Update table statistics for query planner
ANALYZE "02_raw_telemetry_transformed";

-- Commit the transaction
COMMIT;

-- Display summary of the migration
SELECT 
    'Migration completed successfully' as status,
    COUNT(*) as total_rows_updated,
    COUNT(DISTINCT device_date) as unique_device_dates,
    MIN(device_date) as earliest_device_date,
    MAX(device_date) as latest_device_date
FROM "02_raw_telemetry_transformed"
WHERE device_date IS NOT NULL;