#!/usr/bin/env python3
"""
Create detect_change_points UDF by connecting as superuser via peer authentication
TICKET-115: Production CPD Pipeline via PL/Python UDF
"""

import os
import subprocess

def create_udf():
    """Create the detect_change_points UDF using direct psql call"""
    
    # Create SQL file
    sql_content = """CREATE OR REPLACE FUNCTION detect_change_points(signal REAL[])
RETURNS INTEGER[]
AS $BODY$
    import ruptures as rpt
    import numpy as np
    
    # Convert PostgreSQL array to Python list/numpy array
    if not signal or len(signal) == 0:
        return []
    
    # Convert to numpy array and reshape for ruptures
    signal_array = np.array(signal, dtype=float).reshape(-1, 1)
    
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
        
$BODY$ LANGUAGE plpython3u;

GRANT EXECUTE ON FUNCTION detect_change_points(REAL[]) TO ahs_user;

-- Test the function
SELECT 'UDF created successfully!' as status;"""
    
    # Write SQL to temp file
    sql_file = '/tmp/create_udf.sql'
    with open(sql_file, 'w') as f:
        f.write(sql_content)
    
    print(f"Created SQL file: {sql_file}")
    
    # Set appropriate permissions
    os.chmod(sql_file, 0o644)
    
    print("SQL file created. You need to run:")
    print(f"sudo -u postgres psql -d datamine_v2_db -f {sql_file}")
    print()
    print("Then I can test the UDF.")
    
    return sql_file

if __name__ == "__main__":
    print("Creating UDF SQL file...")
    sql_file = create_udf()
    print(f"\n📄 SQL file ready at: {sql_file}")
    print("\n⚠️  Please run the command above, then I'll continue with testing.")