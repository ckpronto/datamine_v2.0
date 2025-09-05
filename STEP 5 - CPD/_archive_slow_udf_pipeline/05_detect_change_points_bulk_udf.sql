-- detect_change_points_bulk UDF for Bulk Change Point Detection using PELT algorithm
-- TICKET-132 Part 1: Critical Bulk Set-Returning UDF Implementation
-- Eliminates N+1 query pattern by processing multiple device_dates in a single function call
-- Uses ruptures library with pre-tuned parameters and optimized bulk processing architecture

CREATE OR REPLACE FUNCTION detect_change_points_bulk(device_dates TEXT[])
RETURNS TABLE(device_date TEXT, cp_timestamp TIMESTAMPTZ)
AS $BODY$
    import ruptures as rpt
    import numpy as np
    from datetime import datetime, timezone
    from itertools import groupby
    
    # Validate input
    if not device_dates or len(device_dates) == 0:
        plpy.warning("No device_dates provided to bulk function")
        return
    
    try:
        # Create optimized prepared statement for bulk data retrieval
        # Single query to fetch all data for all device_dates in one execution
        bulk_query = """
            SELECT 
                device_date,
                timestamp,
                load_weight_rate_of_change
            FROM "04_primary_feature_table"
            WHERE device_date = ANY($1)
                AND load_weight_rate_of_change IS NOT NULL
            ORDER BY device_date, timestamp ASC
        """
        
        # Use plpy.prepare() for optimal query performance with reusable plan
        prepared_plan = plpy.prepare(bulk_query, ["text[]"])
        
        # Execute the bulk query once for all device_dates
        result = plpy.execute(prepared_plan, [device_dates])
        
        if not result or len(result) == 0:
            plpy.warning(f"No data found for any of the {len(device_dates)} device_dates")
            return
        
        plpy.info(f"Bulk query retrieved {len(result)} total data points for {len(device_dates)} device_dates")
        
        # Group results by device_date using itertools.groupby for memory efficiency
        # Data is already sorted by device_date, timestamp from the query
        grouped_data = groupby(result, key=lambda row: row['device_date'])
        
        total_change_points = 0
        processed_devices = 0
        
        # Process each device_date group
        for current_device_date, group_rows in grouped_data:
            processed_devices += 1
            
            # Convert group iterator to list for processing
            device_data = list(group_rows)
            
            # Handle edge cases - require minimum data points for PELT algorithm
            if len(device_data) < 10:  # min_size requirement
                plpy.warning(f"Insufficient data points ({len(device_data)}) for device_date: {current_device_date}")
                continue
            
            # Extract signal data and timestamps for this device_date
            timestamps = []
            signal_values = []
            
            for row in device_data:
                timestamps.append(row['timestamp'])
                signal_values.append(float(row['load_weight_rate_of_change']))
            
            # Convert signal to numpy array for ruptures processing
            signal_array = np.array(signal_values, dtype=float).reshape(-1, 1)
            
            # Initialize PELT algorithm with pre-tuned parameters
            # pen=0.05: Optimal penalty from TICKET-124 two-phase validation (100% recall)
            # model="l2": L2 norm for continuous signals (load_weight_rate_of_change)
            # min_size=10: Minimum segment length to avoid noise
            # jump=1: Full resolution analysis
            algo = rpt.Pelt(model="l2", min_size=10, jump=1)
            
            try:
                # Fit the model for this device_date
                algo.fit(signal_array)
                
                # Predict change points (returns array indices)
                change_point_indices = algo.predict(pen=0.05)
                
                # Remove the last point (it's always the end of signal)
                if len(change_point_indices) > 0 and change_point_indices[-1] == len(signal_values):
                    change_point_indices = change_point_indices[:-1]
                
                # Convert indices to actual timestamps and yield results
                device_change_points = 0
                for idx in change_point_indices:
                    if 0 <= idx < len(timestamps):
                        yield (current_device_date, timestamps[idx])
                        device_change_points += 1
                        total_change_points += 1
                
                plpy.debug(f"Device {current_device_date}: {len(device_data)} points -> {device_change_points} change points")
                
            except Exception as device_error:
                # Log error for this specific device but continue processing others
                plpy.warning(f"Change point detection failed for {current_device_date}: {str(device_error)}")
                continue
        
        plpy.info(f"Bulk processing completed: {processed_devices} devices processed, {total_change_points} total change points detected")
        
    except Exception as e:
        # Log detailed error information for bulk processing failure
        plpy.error(f"Bulk change point detection failed: {str(e)}")
        return
        
$BODY$ LANGUAGE plpython3u;

-- Grant execute permissions to ahs_user
GRANT EXECUTE ON FUNCTION detect_change_points_bulk(TEXT[]) TO ahs_user;

-- Performance note: This function eliminates the N+1 query pattern by:
-- 1. Using a single prepared statement to fetch all data for all device_dates
-- 2. Processing results in-memory using itertools.groupby for efficiency  
-- 3. Yielding results as a set-returning function rather than building arrays
-- 4. Providing bulk processing with optimal database interaction patterns