-- Change Point Detection Pipeline - End-to-End SQL Script
-- TICKET-115: Production CPD Pipeline via PL/Python UDF
-- 
-- This script processes the 04_primary_feature_table to detect change points
-- using the PELT algorithm and writes results to 05_candidate_events table

-- Step 1: Create the candidate events table
CREATE TABLE IF NOT EXISTS "05_candidate_events" (
    event_id SERIAL PRIMARY KEY,
    device_id TEXT NOT NULL,
    timestamp_start TIMESTAMPTZ NOT NULL,
    timestamp_end TIMESTAMPTZ,
    cpd_confidence_score FLOAT,
    detection_method TEXT DEFAULT 'PELT',
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    raw_data_points INTEGER,
    change_point_index INTEGER
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_candidate_events_device_id ON "05_candidate_events"(device_id);
CREATE INDEX IF NOT EXISTS idx_candidate_events_timestamp ON "05_candidate_events"(timestamp_start);
CREATE INDEX IF NOT EXISTS idx_candidate_events_device_timestamp ON "05_candidate_events"(device_id, timestamp_start);

-- Step 2: Process change point detection
WITH 
-- Aggregate signals by device and date
device_signals AS (
    SELECT 
        device_id,
        DATE(timestamp) as processing_date,
        -- Collect load_weight_rate_of_change values in chronological order
        ARRAY_AGG(COALESCE(load_weight_rate_of_change, 0.0) ORDER BY timestamp) as signal_array,
        -- Also collect timestamps for mapping back to original data
        ARRAY_AGG(timestamp ORDER BY timestamp) as timestamp_array,
        MIN(timestamp) as date_start,
        MAX(timestamp) as date_end,
        COUNT(*) as total_points
    FROM "04_primary_feature_table"
    WHERE load_weight_rate_of_change IS NOT NULL
    AND timestamp >= CURRENT_DATE - INTERVAL '30 days'  -- Process recent data
    GROUP BY device_id, DATE(timestamp)
    HAVING COUNT(*) >= 10  -- Minimum points needed for PELT
),

-- Detect change points using the UDF
change_points AS (
    SELECT 
        device_id,
        processing_date,
        signal_array,
        timestamp_array,
        date_start,
        date_end,
        total_points,
        -- Apply the detect_change_points UDF
        detect_change_points(signal_array::REAL[]) as cp_indexes
    FROM device_signals
),

-- Unnest change points and map back to timestamps
change_points_expanded AS (
    SELECT 
        device_id,
        processing_date,
        timestamp_array,
        total_points,
        UNNEST(cp_indexes) as cp_index
    FROM change_points
    WHERE array_length(cp_indexes, 1) > 0  -- Only process if change points found
),

-- Map change point indexes to actual timestamps
change_points_with_timestamps AS (
    SELECT 
        device_id,
        processing_date,
        cp_index,
        total_points,
        -- Get timestamp at change point index
        timestamp_array[cp_index] as cp_timestamp,
        -- Calculate confidence score based on signal strength
        CASE 
            WHEN cp_index <= total_points * 0.1 OR cp_index >= total_points * 0.9 
            THEN 0.7  -- Lower confidence for edge points
            ELSE 0.9  -- Higher confidence for internal points
        END as confidence_score
    FROM change_points_expanded
    WHERE cp_index <= array_length(timestamp_array, 1)  -- Ensure valid index
),

-- Create event windows around change points
candidate_events_prep AS (
    SELECT 
        device_id,
        cp_timestamp as timestamp_start,
        -- Create 2-minute window after change point
        cp_timestamp + INTERVAL '2 minutes' as timestamp_end,
        confidence_score as cpd_confidence_score,
        'PELT' as detection_method,
        total_points as raw_data_points,
        cp_index as change_point_index
    FROM change_points_with_timestamps
)

-- Step 3: Insert results into candidate events table
INSERT INTO "05_candidate_events" 
    (device_id, timestamp_start, timestamp_end, cpd_confidence_score, detection_method, raw_data_points, change_point_index)
SELECT 
    device_id,
    timestamp_start,
    timestamp_end,
    cpd_confidence_score,
    detection_method,
    raw_data_points,
    change_point_index
FROM candidate_events_prep
ON CONFLICT DO NOTHING;  -- Avoid duplicates

-- Step 4: Summary statistics
WITH processing_summary AS (
    SELECT 
        COUNT(*) as total_events_detected,
        COUNT(DISTINCT device_id) as devices_processed,
        MIN(timestamp_start) as earliest_event,
        MAX(timestamp_start) as latest_event,
        AVG(cpd_confidence_score) as avg_confidence,
        AVG(raw_data_points) as avg_signal_length
    FROM "05_candidate_events"
    WHERE created_at >= CURRENT_TIMESTAMP - INTERVAL '1 minute'  -- Recent run
)
SELECT 
    'CPD Pipeline Execution Complete' as status,
    total_events_detected,
    devices_processed,
    earliest_event,
    latest_event,
    ROUND(avg_confidence::numeric, 3) as avg_confidence,
    ROUND(avg_signal_length::numeric, 0) as avg_signal_length
FROM processing_summary;

-- Additional verification query
SELECT 
    device_id,
    COUNT(*) as events_detected,
    MIN(timestamp_start) as first_event,
    MAX(timestamp_start) as last_event
FROM "05_candidate_events"
WHERE created_at >= CURRENT_TIMESTAMP - INTERVAL '1 minute'
GROUP BY device_id
ORDER BY events_detected DESC;