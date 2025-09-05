-- Part 2: Atomic table swap (run this AFTER the main migration completes)
-- This script performs the final atomic swap and cleanup

-- WARNING: This will briefly lock the table during the rename operations
-- Run this only when you're ready to complete the migration

BEGIN;

-- Step 4: Drop the foreign key constraint temporarily (so we can rename tables)
ALTER TABLE "03_telemetry_data_inferences" 
DROP CONSTRAINT "fk_telemetry_inferences_raw_event";

-- Step 5: Atomic table swap - this is very fast (metadata-only operation)
ALTER TABLE "02_raw_telemetry_transformed" RENAME TO "02_raw_telemetry_transformed_old";
ALTER TABLE "02_raw_telemetry_transformed_new" RENAME TO "02_raw_telemetry_transformed";

-- Step 6: Rename all the indexes to match the original names
ALTER INDEX idx_02_raw_telemetry_transformed_timestamp_new 
RENAME TO idx_02_raw_telemetry_transformed_timestamp;

ALTER INDEX idx_02_raw_telemetry_transformed_device_id_new 
RENAME TO idx_02_raw_telemetry_transformed_device_id;

ALTER INDEX idx_02_raw_telemetry_transformed_device_timestamp_new 
RENAME TO idx_02_raw_telemetry_transformed_device_timestamp;

ALTER INDEX idx_02_raw_telemetry_transformed_device_date_new 
RENAME TO idx_02_raw_telemetry_transformed_device_date;

ALTER INDEX idx_02_raw_telemetry_transformed_device_date_timestamp_new 
RENAME TO idx_02_raw_telemetry_transformed_device_date_timestamp;

ALTER INDEX idx_02_raw_telemetry_transformed_ingested_at_new 
RENAME TO idx_02_raw_telemetry_transformed_ingested_at;

ALTER INDEX idx_02_raw_telemetry_transformed_state_new 
RENAME TO idx_02_raw_telemetry_transformed_state;

ALTER INDEX idx_02_raw_telemetry_transformed_software_state_new 
RENAME TO idx_02_raw_telemetry_transformed_software_state;

ALTER INDEX idx_02_raw_telemetry_transformed_position_gist_new 
RENAME TO idx_02_raw_telemetry_transformed_position_gist;

ALTER INDEX idx_02_raw_telemetry_transformed_extras_gin_new 
RENAME TO idx_02_raw_telemetry_transformed_extras_gin;

-- Step 7: Recreate the foreign key constraint
ALTER TABLE "03_telemetry_data_inferences" 
ADD CONSTRAINT "fk_telemetry_inferences_raw_event" 
FOREIGN KEY (raw_event_hash_id) 
REFERENCES "02_raw_telemetry_transformed"(raw_event_hash_id) 
ON DELETE CASCADE;

COMMIT;

-- Step 8: Add column comment and update statistics
COMMENT ON COLUMN "02_raw_telemetry_transformed".device_date 
IS 'Combined device_id and date (YYYY-MM-DD) for efficient device-day partitioning and sorting';

ANALYZE "02_raw_telemetry_transformed";

-- Step 9: Display final migration summary
SELECT 
    'Migration completed successfully!' as status,
    COUNT(*) as total_rows_migrated,
    COUNT(DISTINCT device_date) as unique_device_dates,
    MIN(device_date) as earliest_device_date,
    MAX(device_date) as latest_device_date,
    COUNT(CASE WHEN device_date IS NOT NULL THEN 1 END) as rows_with_device_date
FROM "02_raw_telemetry_transformed";

-- Note: The old table "02_raw_telemetry_transformed_old" is kept for safety
-- Drop it manually after verifying the migration worked correctly:
-- DROP TABLE "02_raw_telemetry_transformed_old";