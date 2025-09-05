#!/usr/bin/env python3
"""
TICKET-133: Performance Test - UDF-Free Processing with Polars
High-Performance Change Point Detection using Polars Vector Engine

This script implements a "hybrid" architectural pattern that completely eliminates 
the PL/Python UDF bottleneck by leveraging pure Python + Polars for vectorized,
in-memory computation with optimized database I/O patterns.

Key Performance Features:
- Single COPY extraction (eliminates N+1 query pattern)  
- Polars vectorized processing (no pandas overhead)
- Bulk insert with execute_values (optimal write performance)
- No UDF overhead or intermediate serialization
- In-memory processing with minimal database roundtrips

Architecture: PostgreSQL COPY -> Polars -> Ruptures PELT -> Bulk Insert

Author: Claude Code Development Team
Date: 2025-09-04
Ticket: TICKET-133
"""

import logging
import psycopg2
import psycopg2.extras
import polars as pl
import ruptures as rpt
import numpy as np
import time
import io
from datetime import datetime, timezone
from pathlib import Path

# Configure logging
log_file = Path(__file__).parent / f'cpd_polars_test_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Database connection configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'datamine_v2_db',
    'user': 'ahs_user',
    'password': 'ahs_password'
}

# Hardcoded test chunk for performance benchmarking
TEST_CHUNK = [
    'lake-605-10-0050_2025-07-30', 'lake-605-10-0050_2025-07-31',
    'lake-605-10-0050_2025-08-01', 'lake-605-10-0050_2025-08-02',
    'lake-605-10-0050_2025-08-03', 'lake-605-10-0050_2025-08-04',
    'lake-605-10-0050_2025-08-05', 'lake-605-10-0050_2025-08-06'
]

def extract_data_with_copy(conn, device_dates):
    """
    High-speed data extraction using PostgreSQL COPY command.
    Returns CSV data as string buffer for Polars processing.
    
    This eliminates the N+1 query pattern by extracting all required
    device-dates in a single optimized COPY operation.
    """
    logger.info(f"Extracting data for {len(device_dates)} device-dates using COPY")
    
    try:
        with conn.cursor() as cur:
            # Create a string buffer to capture COPY output
            copy_buffer = io.StringIO()
            
            # Build the array literal for the SQL query
            device_date_array = "ARRAY[" + ",".join([f"'{dd}'" for dd in device_dates]) + "]"
            
            # High-performance COPY query - single extraction
            copy_query = f"""
                COPY (
                    SELECT device_date, timestamp, load_weight_rate_of_change
                    FROM "04_primary_feature_table"
                    WHERE device_date = ANY({device_date_array}) AND load_weight_rate_of_change IS NOT NULL
                    ORDER BY device_date, timestamp
                ) TO STDOUT WITH CSV HEADER;
            """
            
            # Execute COPY command
            start_time = time.time()
            cur.copy_expert(copy_query, copy_buffer, size=8192)  # 8KB buffer for optimal I/O
            copy_time = time.time() - start_time
            
            # Get the CSV data
            csv_data = copy_buffer.getvalue()
            copy_buffer.close()
            
            logger.info(f"COPY extraction completed in {copy_time:.2f}s, extracted {len(csv_data)} bytes")
            
            return csv_data, copy_time
            
    except Exception as e:
        logger.error(f"Data extraction failed: {e}")
        raise

def process_with_polars(csv_data):
    """
    High-performance processing using Polars vectorized engine.
    Groups by device_date and applies ruptures PELT algorithm to each group.
    
    Polars provides zero-copy operations and vectorized processing that
    significantly outperforms pandas for this type of time-series analysis.
    """
    logger.info("Processing data with Polars vectorized engine")
    
    try:
        start_time = time.time()
        
        # Parse CSV data directly into Polars DataFrame
        # Polars is optimized for CSV parsing and memory efficiency
        # For simplicity, we'll work with strings and convert to numpy later
        df = pl.read_csv(
            io.StringIO(csv_data),
            schema_overrides={
                'device_date': pl.Utf8,
                'timestamp': pl.Utf8,  # Keep as string for now
                'load_weight_rate_of_change': pl.Float64
            }
        )
        
        parse_time = time.time() - start_time
        logger.info(f"Polars CSV parsing completed in {parse_time:.2f}s, loaded {len(df)} rows")
        
        # Initialize change points collector
        all_change_points = []
        processing_start = time.time()
        
        # Group by device_date and process each group with ruptures
        # Polars groupby is highly optimized and maintains data locality
        groups = df.group_by('device_date')
        
        for device_date, group_df in groups:
            device_date_str = device_date[0]  # Extract string from tuple
            
            # Convert to numpy array for ruptures processing
            # Polars to_numpy() is zero-copy when possible
            timestamp_strings = group_df['timestamp'].to_numpy()
            signal_values = group_df['load_weight_rate_of_change'].to_numpy()
            
            # For performance benchmark, we'll use timestamps as they are
            # since the PELT algorithm only needs the signal values
            # We'll just create synthetic timestamps for the return values
            import pandas as pd
            timestamps = pd.to_datetime(timestamp_strings, format='mixed').to_numpy()
            
            # Validate minimum data requirements
            if len(signal_values) < 10:
                logger.warning(f"Insufficient data points ({len(signal_values)}) for {device_date_str}")
                continue
            
            # Apply ruptures PELT algorithm
            change_points = detect_change_points_pelt(
                signal_values, 
                timestamps, 
                device_date_str
            )
            
            # Collect results for bulk insert
            # Extract device_id from device_date (format: device_id_date)
            device_id = device_date_str.rsplit('_', 1)[0]  # Remove the date part
            for cp_timestamp in change_points:
                all_change_points.append((device_id, cp_timestamp))
        
        processing_time = time.time() - processing_start
        total_time = time.time() - start_time
        
        logger.info(f"Polars processing completed in {total_time:.2f}s "
                   f"(parse: {parse_time:.2f}s, processing: {processing_time:.2f}s)")
        logger.info(f"Detected {len(all_change_points)} total change points")
        
        return all_change_points, total_time
        
    except Exception as e:
        logger.error(f"Polars processing failed: {e}")
        raise

def detect_change_points_pelt(signal_values, timestamps, device_date_str):
    """
    Apply ruptures PELT algorithm to detect change points.
    Uses the same optimized parameters from the UDF implementation.
    """
    try:
        # Reshape for ruptures (expects 2D array)
        signal_array = signal_values.reshape(-1, 1)
        
        # Initialize PELT with pre-tuned parameters
        # pen=0.05: Optimal penalty from TICKET-124 validation (100% recall)
        # model="l2": L2 norm for continuous signals
        # min_size=10: Minimum segment length to avoid noise
        # jump=1: Full resolution analysis
        algo = rpt.Pelt(model="l2", min_size=10, jump=1)
        
        # Fit and predict change points
        algo.fit(signal_array)
        change_point_indices = algo.predict(pen=0.05)
        
        # Remove the last point (always end of signal)
        if len(change_point_indices) > 0 and change_point_indices[-1] == len(signal_values):
            change_point_indices = change_point_indices[:-1]
        
        # Convert indices to timestamps
        change_point_timestamps = []
        for idx in change_point_indices:
            if 0 <= idx < len(timestamps):
                change_point_timestamps.append(timestamps[idx])
        
        logger.info(f"Processed {len(signal_values)} points for {device_date_str}, "
                   f"found {len(change_point_timestamps)} change points")
        
        return change_point_timestamps
        
    except Exception as e:
        logger.error(f"PELT algorithm failed for {device_date_str}: {e}")
        return []

def bulk_insert_change_points(conn, change_points):
    """
    High-performance bulk insert using psycopg2.extras.execute_values.
    This is the most efficient method for bulk inserts in PostgreSQL.
    """
    if not change_points:
        logger.warning("No change points to insert")
        return 0
        
    logger.info(f"Bulk inserting {len(change_points)} change points")
    
    try:
        start_time = time.time()
        
        with conn.cursor() as cur:
            # Prepare data for bulk insert
            insert_data = [
                (device_id, cp_timestamp, cp_timestamp, None, 'polars_test', datetime.now(timezone.utc))
                for device_id, cp_timestamp in change_points
            ]
            
            # Use execute_values for optimal bulk insert performance
            # This method is significantly faster than executemany or individual inserts
            insert_query = """
                INSERT INTO "05_candidate_events" 
                (device_id, timestamp_start, timestamp_end, cpd_confidence_score, detection_method, created_at)
                VALUES %s
            """
            
            psycopg2.extras.execute_values(
                cur, 
                insert_query, 
                insert_data,
                template=None,
                page_size=1000  # Optimize for memory usage
            )
            
            conn.commit()
            
        insert_time = time.time() - start_time
        logger.info(f"Bulk insert completed in {insert_time:.2f}s")
        
        return insert_time
        
    except Exception as e:
        logger.error(f"Bulk insert failed: {e}")
        conn.rollback()
        raise

def ensure_target_table_exists(conn):
    """
    Verify the 05_candidate_events table exists for storing results.
    The table already exists with the proper schema.
    """
    try:
        with conn.cursor() as cur:
            # Just verify the table exists
            cur.execute("SELECT 1 FROM \"05_candidate_events\" LIMIT 1;")
            
        logger.info("Target table 05_candidate_events verified")
        
    except Exception as e:
        logger.error(f"Failed to verify target table: {e}")
        raise

def run_performance_benchmark():
    """
    Execute the complete UDF-free Polars performance benchmark.
    
    Architecture Flow:
    1. Single COPY extraction from PostgreSQL
    2. Polars vectorized processing with groupby
    3. Ruptures PELT algorithm application
    4. Bulk insert with execute_values
    """
    logger.info("=" * 80)
    logger.info("TICKET-133: UDF-Free Polars Performance Benchmark")
    logger.info("=" * 80)
    logger.info(f"Test chunk: {len(TEST_CHUNK)} device-dates")
    logger.info(f"Device range: {TEST_CHUNK[0]} to {TEST_CHUNK[-1]}")
    
    # Start end-to-end timer
    total_start_time = time.time()
    
    try:
        # Establish database connection
        logger.info("Connecting to database...")
        conn = psycopg2.connect(**DB_CONFIG)
        
        # Ensure target table exists
        ensure_target_table_exists(conn)
        
        # Phase 1: High-speed data extraction with COPY
        logger.info("\n--- Phase 1: Data Extraction ---")
        csv_data, copy_time = extract_data_with_copy(conn, TEST_CHUNK)
        
        # Phase 2: Polars vectorized processing
        logger.info("\n--- Phase 2: Polars Processing ---")
        change_points, processing_time = process_with_polars(csv_data)
        
        # Phase 3: Bulk insert results
        logger.info("\n--- Phase 3: Bulk Insert ---")
        insert_time = bulk_insert_change_points(conn, change_points)
        
        # Calculate total performance metrics
        total_time = time.time() - total_start_time
        
        # Performance summary
        logger.info("\n" + "=" * 80)
        logger.info("PERFORMANCE BENCHMARK RESULTS")
        logger.info("=" * 80)
        logger.info(f"Total execution time: {total_time:.2f}s")
        logger.info(f"Data extraction (COPY): {copy_time:.2f}s ({copy_time/total_time*100:.1f}%)")
        logger.info(f"Polars processing: {processing_time:.2f}s ({processing_time/total_time*100:.1f}%)")
        logger.info(f"Bulk insert: {insert_time:.2f}s ({insert_time/total_time*100:.1f}%)")
        logger.info(f"Device-dates processed: {len(TEST_CHUNK)}")
        logger.info(f"Change points detected: {len(change_points)}")
        logger.info(f"Throughput: {len(TEST_CHUNK)/total_time:.2f} device-dates/second")
        logger.info("=" * 80)
        
        # Performance metrics for comparison with UDF approach
        performance_metrics = {
            'total_time': total_time,
            'copy_time': copy_time,
            'processing_time': processing_time,
            'insert_time': insert_time,
            'device_dates_processed': len(TEST_CHUNK),
            'change_points_detected': len(change_points),
            'throughput_per_second': len(TEST_CHUNK)/total_time,
            'architecture': 'udf_free_polars',
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        
        logger.info(f"Performance metrics: {performance_metrics}")
        
        return performance_metrics
        
    except Exception as e:
        logger.error(f"Performance benchmark failed: {e}")
        raise
        
    finally:
        if 'conn' in locals():
            conn.close()
            logger.info("Database connection closed")

def main():
    """
    Main entry point for the Polars performance benchmark.
    """
    try:
        logger.info("Starting UDF-Free Polars Performance Test...")
        metrics = run_performance_benchmark()
        logger.info("Performance benchmark completed successfully!")
        return metrics
        
    except Exception as e:
        logger.error(f"Benchmark execution failed: {e}")
        raise

if __name__ == "__main__":
    main()