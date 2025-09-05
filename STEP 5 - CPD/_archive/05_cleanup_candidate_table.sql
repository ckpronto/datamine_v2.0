-- First, clear any old test data from the table
TRUNCATE TABLE "05_candidate_events";

-- Drop the unnecessary columns that are no longer populated
ALTER TABLE "05_candidate_events"
DROP COLUMN IF EXISTS timestamp_end,
DROP COLUMN IF EXISTS cpd_confidence_score,
DROP COLUMN IF EXISTS detection_method,
DROP COLUMN IF EXISTS raw_data_points,
DROP COLUMN IF EXISTS change_point_index;

-- We are intentionally keeping: raw_event_hash_id, device_id, and timestamp_start
-- as they are essential for the next Machine Learning stage.
