#!/usr/bin/env python3
"""
Create detect_change_points UDF using subprocess to call postgres user directly
TICKET-115: Production CPD Pipeline via PL/Python UDF
"""

import subprocess
import sys

def create_udf():
    """Create the detect_change_points UDF using subprocess"""
    
    # Full UDF creation command
    sql_command = """
    CREATE OR REPLACE FUNCTION detect_change_points(signal REAL[])
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
    """
    
    try:
        # Execute via subprocess as postgres user using su instead
        cmd = ['su', '-c', f'psql -d datamine_v2_db -c "{sql_command}"', 'postgres']
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            print("✅ UDF created successfully!")
            print(result.stdout)
            
            # Test the function
            test_cmd = ['su', '-c', 'psql -d datamine_v2_db -c "SELECT detect_change_points(ARRAY[1.0, 2.0, 10.0, 11.0, 12.0, 1.0, 2.0, 3.0, 15.0, 16.0, 17.0, 2.0, 3.0, 4.0]);"', 'postgres']
            
            test_result = subprocess.run(test_cmd, capture_output=True, text=True, timeout=15)
            
            if test_result.returncode == 0:
                print("✅ UDF test successful!")
                print(test_result.stdout)
            else:
                print("❌ UDF test failed:")
                print(test_result.stderr)
            
        else:
            print("❌ UDF creation failed:")
            print(result.stderr)
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ Command timed out")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    
    return True

if __name__ == "__main__":
    print("Creating detect_change_points UDF...")
    success = create_udf()
    if success:
        print("\n🎉 UDF creation complete!")
    else:
        print("\n💥 UDF creation failed!")
        sys.exit(1)