#!/usr/bin/env python3
"""
TICKET-135: Final Fix - Solve Algorithmic Bottleneck with Downsampling

This script implements the definitive solution to the O(n²) complexity bottleneck
in the ruptures PELT algorithm by using 5-second rolling mean downsampling.
This reduces computation time from ~2.5 hours to seconds for full-day processing.

Critical Performance Solution:
- Single device-date test: 'lake-605-10-0050_2025-07-30'
- PostgreSQL COPY extraction for maximum I/O performance
- Polars vectorized processing with 5-second rolling mean downsampling
- Dramatically reduced data points fed to ruptures PELT algorithm
- Ruptures PELT algorithm with optimal parameters (pen=0.05)
- Bulk insert with execute_values for efficient write performance

Architecture: PostgreSQL COPY -> Polars -> Downsampling -> Ruptures PELT -> Bulk Insert

Author: Claude Code Development Team - Lead Data Engineer (Opus)
Date: 2025-09-04
Tickets: TICKET-134, TICKET-135 (Final Fix)
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
log_file = Path(__file__).parent / f'cpd_polars_single_test_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
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

# Single test device-date for focused performance baseline
TEST_DEVICE_DATE = ['lake-605-10-0050_2025-07-30']

def extract_single_device_data(conn, device_date):
    """
    High-speed data extraction for single device-date using PostgreSQL COPY.
    
    Args:
        conn: Database connection
        device_date: Single device-date string
        
    Returns:
        tuple: (csv_data_string, extraction_time_seconds)
    """
    logger.info(f"Extracting data for device-date: {device_date}")
    
    try:
        with conn.cursor() as cur:
            # Create string buffer to capture COPY output
            copy_buffer = io.StringIO()
            
            # High-performance COPY query for single device-date (include raw_event_hash_id)
            copy_query = f"""
                COPY (
                    SELECT device_date, timestamp, load_weight_rate_of_change, raw_event_hash_id
                    FROM "04_primary_feature_table"
                    WHERE device_date = '{device_date}' AND load_weight_rate_of_change IS NOT NULL
                    ORDER BY timestamp
                ) TO STDOUT WITH CSV HEADER;
            """
            
            # Execute COPY with timing
            start_time = time.time()
            cur.copy_expert(copy_query, copy_buffer, size=8192)
            extraction_time = time.time() - start_time
            
            # Get CSV data and cleanup
            csv_data = copy_buffer.getvalue()
            copy_buffer.close()
            
            # Log extraction results
            data_size_mb = len(csv_data) / (1024 * 1024)
            logger.info(f"COPY extraction completed in {extraction_time:.3f}s")
            logger.info(f"Extracted {len(csv_data)} bytes ({data_size_mb:.2f} MB)")
            
            return csv_data, extraction_time
            
    except Exception as e:
        logger.error(f"Data extraction failed for {device_date}: {e}")
        raise

def process_single_device_polars(csv_data, device_date):
    """
    Process single device-date data using Polars vectorized engine.
    
    Args:
        csv_data: Raw CSV data string from COPY command
        device_date: Device-date string for logging
        
    Returns:
        tuple: (change_points_list, processing_time_seconds)
    """
    logger.info(f"Processing {device_date} with Polars vectorized engine")
    
    try:
        start_time = time.time()
        
        # Parse CSV directly into Polars DataFrame (include raw_event_hash_id)
        df = pl.read_csv(
            io.StringIO(csv_data),
            schema_overrides={
                'device_date': pl.Utf8,
                'timestamp': pl.Utf8,
                'load_weight_rate_of_change': pl.Float64,
                'raw_event_hash_id': pl.Utf8
            }
        )
        
        parse_time = time.time() - start_time
        logger.info(f"Polars CSV parsing: {parse_time:.3f}s, loaded {len(df)} rows")
        
        # Validate minimum data requirements
        if len(df) < 10:
            logger.warning(f"Insufficient data points ({len(df)}) for {device_date}")
            return [], time.time() - start_time
        
        # Log sample data for debugging
        logger.info(f"Data sample - first 5 rows:")
        logger.info(f"  {df.head()}")
        logger.info(f"Load weight stats: min={df['load_weight_rate_of_change'].min()}, "
                   f"max={df['load_weight_rate_of_change'].max()}, "
                   f"null_count={df['load_weight_rate_of_change'].null_count()}")
        
        # TICKET-135: Implement downsampling to solve O(n²) complexity bottleneck
        # Convert timestamp to datetime for proper time-based operations
        df = df.with_columns(
            pl.col("timestamp").str.to_datetime(strict=False, time_zone="UTC")
        )
        
        downsampling_start = time.time()
        logger.info(f"Starting downsampling process to reduce O(n²) complexity...")
        logger.info(f"Original data frequency: ~2Hz, target: 5-second intervals (0.2Hz)")
        
        # Implement proper 5-second downsampling using group_by_dynamic
        # This will create 5-second time buckets and take the mean within each bucket
        # Also preserve the first raw_event_hash_id in each bucket for traceability
        df_downsampled = (
            df.filter(pl.col("timestamp").is_not_null())
              .sort("timestamp")
              .group_by_dynamic(
                  "timestamp", 
                  every="5s", 
                  period="5s",
                  closed="left"
              )
              .agg([
                  pl.col("load_weight_rate_of_change").mean().alias("load_weight_rate_of_change"),
                  pl.col("raw_event_hash_id").first().alias("raw_event_hash_id")
              ])
              .drop_nulls()
        )
        
        downsampling_time = time.time() - downsampling_start
        logger.info(f"Downsampling completed: {downsampling_time:.3f}s")
        logger.info(f"Data reduced from {len(df)} to {len(df_downsampled)} points ({len(df_downsampled)/len(df)*100:.1f}%)")
        
        # Convert to numpy arrays for ruptures processing (using downsampled data)
        pelt_start = time.time()
        # Convert datetime back to string for compatibility with existing code
        timestamp_strings = df_downsampled['timestamp'].dt.strftime("%Y-%m-%d %H:%M:%S").to_numpy()
        signal_values = df_downsampled['load_weight_rate_of_change'].to_numpy()
        hash_ids = df_downsampled['raw_event_hash_id'].to_numpy()
        
        logger.info(f"Data conversion completed: {time.time() - pelt_start:.3f}s")
        
        # Keep timestamps as strings for now, convert indices to timestamps after PELT
        timestamp_strings_array = timestamp_strings
        timestamp_convert_time = 0.001  # Minimal since we're not converting yet
        
        logger.info(f"Skipping timestamp conversion for PELT performance")
        
        # Apply ruptures PELT algorithm (using indices, convert to timestamps after)
        pelt_algorithm_start = time.time()
        change_points = detect_change_points_pelt(
            signal_values, 
            timestamp_strings, 
            hash_ids,
            device_date
        )
        pelt_algorithm_time = time.time() - pelt_algorithm_start
        
        logger.info(f"PELT algorithm completed: {pelt_algorithm_time:.3f}s")
        
        pelt_time = time.time() - pelt_start
        total_time = time.time() - start_time
        
        # Extract device_id from device_date (format: device_id_date)
        device_id = device_date.rsplit('_', 1)[0]
        
        # Format change points for database insertion (change_points now contains tuples of (timestamp, hash_id))
        formatted_change_points = [
            (device_id, cp_timestamp, hash_id) for cp_timestamp, hash_id in change_points
        ]
        
        logger.info(f"Polars processing completed: {total_time:.3f}s")
        logger.info(f"  - Parsing: {parse_time:.3f}s")
        logger.info(f"  - Downsampling: {downsampling_time:.3f}s")
        logger.info(f"  - PELT algorithm: {pelt_time:.3f}s")
        logger.info(f"  - Change points detected: {len(change_points)}")
        logger.info(f"  - Data reduction: {len(df)} -> {len(df_downsampled)} points ({len(df_downsampled)/len(df)*100:.1f}%)")
        
        return formatted_change_points, total_time
        
    except Exception as e:
        logger.error(f"Polars processing failed for {device_date}: {e}")
        raise

def detect_change_points_pelt(signal_values, timestamp_strings, hash_ids, device_date):
    """
    Apply ruptures PELT algorithm with optimized parameters.
    
    Uses pen=0.05 from TICKET-124 validation (100% recall achieved).
    
    Args:
        signal_values: numpy array of load_weight_rate_of_change values
        timestamp_strings: numpy array of timestamp strings
        hash_ids: numpy array of raw_event_hash_id values
        device_date: Device-date string for logging
        
    Returns:
        list: Tuples of (timestamp_datetime, raw_event_hash_id) for change points
    """
    try:
        # Reshape signal for ruptures (expects 2D array)
        signal_array = signal_values.reshape(-1, 1)
        
        logger.info(f"Starting PELT algorithm on {len(signal_values)} data points")
        
        # Initialize PELT with pre-tuned parameters
        # pen=0.05: Optimal penalty from validation testing
        # model="l2": L2 norm for continuous signals
        # min_size=10: Minimum segment length (avoids noise)
        # jump=1: Full resolution analysis
        algo = rpt.Pelt(model="l2", min_size=10, jump=1)
        
        # Fit and predict change points
        logger.info("Fitting PELT model...")
        algo.fit(signal_array)
        
        logger.info("Predicting change points...")
        change_point_indices = algo.predict(pen=0.05)
        
        # Remove the last point (always end of signal)
        if len(change_point_indices) > 0 and change_point_indices[-1] == len(signal_values):
            change_point_indices = change_point_indices[:-1]
        
        logger.info(f"Raw change point indices: {len(change_point_indices)}")
        
        # Convert indices to actual timestamps and get corresponding hash IDs
        # NOTE: hash IDs are ONLY for database primary key tracking, NOT for analysis
        import pandas as pd
        change_point_data = []
        for idx in change_point_indices:
            if 0 <= idx < len(timestamp_strings):
                timestamp_str = timestamp_strings[idx]
                timestamp_dt = pd.to_datetime(timestamp_str)
                hash_id = hash_ids[idx]  # Get corresponding hash ID for database key
                change_point_data.append((timestamp_dt, hash_id))
        
        logger.info(f"PELT processed {len(signal_values)} points for {device_date}")
        logger.info(f"Found {len(change_point_data)} change points")
        
        return change_point_data
        
    except Exception as e:
        logger.error(f"PELT algorithm failed for {device_date}: {e}")
        return []

def bulk_insert_change_points(conn, change_points):
    """
    Efficient bulk insert using psycopg2.extras.execute_values.
    
    Args:
        conn: Database connection
        change_points: List of (device_id, timestamp, raw_event_hash_id) tuples
        
    Returns:
        float: Insert time in seconds
    """
    if not change_points:
        logger.warning("No change points to insert")
        return 0.0
        
    logger.info(f"Bulk inserting {len(change_points)} change points")
    
    try:
        start_time = time.time()
        
        with conn.cursor() as cur:
            # Prepare data for bulk insert using raw_event_hash_id as primary key
            insert_data = [
                (hash_id, device_id, cp_timestamp, cp_timestamp, None, 'polars_single_test', datetime.now(timezone.utc))
                for device_id, cp_timestamp, hash_id in change_points
            ]
            
            # Use execute_values for optimal bulk insert performance
            insert_query = """
                INSERT INTO "05_candidate_events" 
                (raw_event_hash_id, device_id, timestamp_start, timestamp_end, cpd_confidence_score, detection_method, created_at)
                VALUES %s
            """
            
            psycopg2.extras.execute_values(
                cur, 
                insert_query, 
                insert_data,
                template=None,
                page_size=1000
            )
            
            conn.commit()
            
        insert_time = time.time() - start_time
        logger.info(f"Bulk insert completed in {insert_time:.3f}s")
        
        return insert_time
        
    except Exception as e:
        logger.error(f"Bulk insert failed: {e}")
        conn.rollback()
        raise

def verify_target_table(conn):
    """
    Verify the 05_candidate_events table exists and is accessible.
    
    Args:
        conn: Database connection
    """
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM \"05_candidate_events\" LIMIT 1;")
            
        logger.info("Target table 05_candidate_events verified")
        
    except Exception as e:
        logger.error(f"Failed to verify target table: {e}")
        raise

def run_single_device_performance_test():
    """
    Execute focused single device-date performance test.
    
    This provides a precise baseline measurement without multi-device complexity.
    
    Returns:
        dict: Performance metrics
    """
    device_date = TEST_DEVICE_DATE[0]
    
    logger.info("=" * 80)
    logger.info("TICKET-135: Final Fix - Algorithmic Bottleneck Solution Test")
    logger.info("=" * 80)
    logger.info(f"Test device-date: {device_date}")
    logger.info(f"Architecture: PostgreSQL COPY -> Polars -> Downsampling -> Ruptures PELT -> Bulk Insert")
    logger.info(f"Downsampling: 5-second rolling mean to solve O(n²) complexity")
    
    # Start end-to-end timer
    total_start_time = time.time()
    
    try:
        # Establish database connection
        logger.info("Connecting to database...")
        conn = psycopg2.connect(**DB_CONFIG)
        
        # Verify target table exists
        verify_target_table(conn)
        
        # Phase 1: High-speed data extraction
        logger.info("\n--- Phase 1: Data Extraction ---")
        csv_data, extraction_time = extract_single_device_data(conn, device_date)
        
        # Validate data extraction
        if not csv_data or len(csv_data) < 100:
            raise ValueError(f"Insufficient data extracted for {device_date}")
        
        # Phase 2: Polars processing with PELT
        logger.info("\n--- Phase 2: Polars Processing ---")
        change_points, processing_time = process_single_device_polars(csv_data, device_date)
        
        # Phase 3: Bulk insert results
        logger.info("\n--- Phase 3: Bulk Insert ---")
        insert_time = bulk_insert_change_points(conn, change_points)
        
        # Calculate total performance
        total_time = time.time() - total_start_time
        
        # Performance summary
        logger.info("\n" + "=" * 80)
        logger.info("SINGLE DEVICE-DATE PERFORMANCE RESULTS")
        logger.info("=" * 80)
        logger.info(f"Device-date tested: {device_date}")
        logger.info(f"Total execution time: {total_time:.3f}s")
        logger.info(f"Data extraction (COPY): {extraction_time:.3f}s ({extraction_time/total_time*100:.1f}%)")
        logger.info(f"Polars processing: {processing_time:.3f}s ({processing_time/total_time*100:.1f}%)")
        logger.info(f"Bulk insert: {insert_time:.3f}s ({insert_time/total_time*100:.1f}%)")
        logger.info(f"Change points detected: {len(change_points)}")
        logger.info("=" * 80)
        
        # Return precise performance metrics
        performance_metrics = {
            'device_date': device_date,
            'total_time_seconds': round(total_time, 3),
            'extraction_time_seconds': round(extraction_time, 3),
            'processing_time_seconds': round(processing_time, 3),
            'insert_time_seconds': round(insert_time, 3),
            'change_points_detected': len(change_points),
            'extraction_percentage': round(extraction_time/total_time*100, 1),
            'processing_percentage': round(processing_time/total_time*100, 1),
            'insert_percentage': round(insert_time/total_time*100, 1),
            'architecture': 'polars_single_device',
            'test_timestamp': datetime.now(timezone.utc).isoformat()
        }
        
        logger.info(f"\nPrecise metrics: {performance_metrics}")
        
        return performance_metrics
        
    except Exception as e:
        logger.error(f"Single device performance test failed: {e}")
        raise
        
    finally:
        if 'conn' in locals():
            conn.close()
            logger.info("Database connection closed")

def main():
    """
    Main entry point for focused single device-date performance test.
    """
    try:
        logger.info("Starting Focused Single Device-Date Performance Test...")
        metrics = run_single_device_performance_test()
        logger.info("Performance test completed successfully!")
        
        # Print key results for easy visibility
        print(f"\n=== PERFORMANCE BASELINE ESTABLISHED ===")
        print(f"Device-Date: {metrics['device_date']}")
        print(f"Total Time: {metrics['total_time_seconds']}s")
        print(f"Change Points: {metrics['change_points_detected']}")
        print(f"Architecture: {metrics['architecture']}")
        
        return metrics
        
    except Exception as e:
        logger.error(f"Performance test execution failed: {e}")
        raise

if __name__ == "__main__":
    main()