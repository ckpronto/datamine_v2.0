#!/usr/bin/env python3
"""
Create detect_change_points UDF for Change Point Detection
TICKET-115: Production CPD Pipeline via PL/Python UDF
"""

import psycopg2

# Database connection as postgres superuser
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'user': 'postgres',
    'password': '',  # postgres user typically has no password on local systems
    'database': 'datamine_v2_db'
}

def create_udf():
    """Create the detect_change_points UDF"""
    
    udf_sql = """
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
            # Initialize PELT algorithm with pre-tuned parameters from prototype
            # pen=0.1: Optimal penalty from parameter tuning phase
            # model="l2": L2 norm for continuous signals (load_weight_rate_of_change)
            # min_size=10: Minimum segment length to avoid noise
            # jump=1: Full resolution analysis
            algo = rpt.Pelt(model="l2", min_size=10, jump=1)
            
            # Fit the model
            algo.fit(signal_array)
            
            # Predict change points
            change_points = algo.predict(pen=0.1)
            
            # Remove the last point (it's always the end of signal)
            if len(change_points) > 0 and change_points[-1] == len(signal):
                change_points = change_points[:-1]
            
            # Convert to list of integers for PostgreSQL
            return [int(cp) for cp in change_points]
            
        except Exception as e:
            # Log error but return empty array rather than failing
            plpy.warning(f"Change point detection failed: {str(e)}")
            return []
            
    $BODY$ LANGUAGE plpython3u;
    
    -- Grant execute permissions to ahs_user
    GRANT EXECUTE ON FUNCTION detect_change_points(REAL[]) TO ahs_user;
    """
    
    try:
        # Try connecting as postgres user first
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        print("Creating detect_change_points UDF...")
        cursor.execute(udf_sql)
        conn.commit()
        
        print("✅ UDF created successfully!")
        
        # Test the function
        test_sql = "SELECT detect_change_points(ARRAY[1.0, 2.0, 10.0, 11.0, 12.0, 1.0, 2.0, 3.0, 15.0, 16.0, 17.0, 2.0, 3.0, 4.0]);"
        cursor.execute(test_sql)
        result = cursor.fetchone()
        print(f"✅ Test result: {result[0]}")
        
        cursor.close()
        conn.close()
        
        return True
        
    except psycopg2.OperationalError as e:
        if "authentication failed" in str(e):
            print("❌ Cannot connect as postgres user without password.")
            print("Please run manually: sudo -u postgres psql -d datamine_v2_db")
            print("Then paste the UDF creation SQL.")
            return False
        else:
            raise e

if __name__ == "__main__":
    create_udf()