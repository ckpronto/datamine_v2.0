-- detect_change_points UDF for Change Point Detection using PELT algorithm
-- TICKET-131: Critical UDF Architecture Redesign - Push-Down Processing
-- Eliminates memory-intensive array aggregation bottleneck by querying data directly within UDF
-- Uses ruptures library with pre-tuned parameters from prototyping phase

CREATE OR REPLACE FUNCTION detect_change_points(device_date_identifier TEXT)
RETURNS TIMESTAMPTZ[]
AS $BODY$
    import ruptures as rpt
    import numpy as np
    from datetime import datetime, timezone
    
    # Validate input
    if not device_date_identifier or device_date_identifier.strip() == '':
        plpy.warning("Invalid device_date_identifier provided")
        return []
    
    try:
        # Query data directly from 04_primary_feature_table using push-down processing
        # This eliminates the memory-intensive ARRAY_AGG bottleneck
        query = """
            SELECT 
                timestamp,
                load_weight_rate_of_change
            FROM "04_primary_feature_table"
            WHERE device_date = %s
                AND load_weight_rate_of_change IS NOT NULL
            ORDER BY timestamp ASC
        """
        
        # Execute query using plpy for direct database access
        # Use format() for parameter substitution since plpy.execute doesn't support parameterized queries the same way
        formatted_query = query % (plpy.quote_literal(device_date_identifier),)
        result = plpy.execute(formatted_query)
        
        if not result or len(result) == 0:
            plpy.warning(f"No data found for device_date: {device_date_identifier}")
            return []
        
        # Handle edge cases - require minimum data points for PELT algorithm
        if len(result) < 10:  # min_size requirement
            plpy.warning(f"Insufficient data points ({len(result)}) for device_date: {device_date_identifier}")
            return []
        
        # Extract signal data and timestamps
        timestamps = []
        signal_values = []
        
        for row in result:
            timestamps.append(row['timestamp'])
            signal_values.append(float(row['load_weight_rate_of_change']))
        
        # Convert signal to numpy array for ruptures processing
        signal_array = np.array(signal_values, dtype=float).reshape(-1, 1)
        
        # Initialize PELT algorithm with pre-tuned parameters from prototype
        # pen=0.05: Optimal penalty from TICKET-124 two-phase validation (100% recall)
        # model="l2": L2 norm for continuous signals (load_weight_rate_of_change)
        # min_size=10: Minimum segment length to avoid noise
        # jump=1: Full resolution analysis
        algo = rpt.Pelt(model="l2", min_size=10, jump=1)
        
        # Fit the model
        algo.fit(signal_array)
        
        # Predict change points (returns array indices)
        change_point_indices = algo.predict(pen=0.05)
        
        # Remove the last point (it's always the end of signal)
        if len(change_point_indices) > 0 and change_point_indices[-1] == len(signal_values):
            change_point_indices = change_point_indices[:-1]
        
        # Convert indices to actual timestamps
        change_point_timestamps = []
        for idx in change_point_indices:
            if 0 <= idx < len(timestamps):
                change_point_timestamps.append(timestamps[idx])
        
        plpy.info(f"Processed {len(result)} data points for {device_date_identifier}, found {len(change_point_timestamps)} change points")
        
        return change_point_timestamps
        
    except Exception as e:
        # Log detailed error information but return empty array rather than failing
        plpy.error(f"Change point detection failed for {device_date_identifier}: {str(e)}")
        return []
        
$BODY$ LANGUAGE plpython3u;

-- Grant execute permissions to ahs_user
GRANT EXECUTE ON FUNCTION detect_change_points(TEXT) TO ahs_user;