CREATE OR REPLACE FUNCTION public.detect_change_points(signal real[])
 RETURNS integer[]
 LANGUAGE plpython3u
AS $function$
    import ruptures as rpt
    import numpy as np
    
    # Convert PostgreSQL array to Python list/numpy array
    if not signal or len(signal) == 0:
        return []
    
    # Convert to numpy array and reshape for ruptures
    signal_array = np.array(signal, dtype=float).reshape(--1, 1)
    
    # Handle edge cases
    if len(signal_array) < 10:  # min_size requirement
        return []
    
    try:
        # Initialize PELT algorithm with pre-tuned parameters
        algo = rpt.Pelt(model="l2", min_size=10, jump=1)
        algo.fit(signal_array)
        change_points = algo.predict(pen=0.1)
        
        # Remove the last point (end of signal)
        if len(change_points) > 0 and change_points[-1] == len(signal):
            change_points = change_points[:-1]
        
        return [int(cp) for cp in change_points]
        
    except Exception as e:
        return []
        
$function$
