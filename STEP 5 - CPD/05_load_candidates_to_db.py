#!/usr/bin/env python3
"""
TICKET-140: Load CPD Candidate Events to Database

This script loads the final CPD candidate events from CSV into the database
using high-performance PostgreSQL COPY commands. It includes comprehensive
validation, error handling, and metrics reporting.

Key Features:
- High-performance bulk loading via COPY command
- Comprehensive data validation before and after loading
- Transactional safety with rollback on errors
- Detailed logging and performance metrics
- Raw event hash ID primary key validation

Author: Claude Code Development Team - Lead Data Engineer
Date: 2025-09-04
Tickets: TICKET-140 (Final Pipeline Phase)
"""

import logging
import psycopg2
import psycopg2.extras
import pandas as pd
import json
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import time
import sys
import os

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'datamine_v2_db',
    'user': 'ahs_user',
    'password': 'ahs_password'
}

# Configure logging with production format
log_file = Path(__file__).parent / f'load_candidates_to_db_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DatabaseLoader:
    """Production-grade database loader for candidate events"""
    
    def __init__(self):
        self.conn = None
        self.cursor = None
        
    def connect(self):
        """Establish database connection with proper error handling"""
        try:
            self.conn = psycopg2.connect(**DB_CONFIG)
            self.conn.autocommit = False  # Ensure transactional safety
            self.cursor = self.conn.cursor()
            logger.info("Database connection established successfully")
            return True
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False
            
    def close(self):
        """Safely close database connections"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Database connection closed")
    
    def validate_table_schema(self):
        """Validate that 05_candidate_events table exists with correct schema"""
        try:
            self.cursor.execute("""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_name = '05_candidate_events'
                ORDER BY ordinal_position;
            """)
            
            columns = self.cursor.fetchall()
            if not columns:
                logger.error("Table 05_candidate_events does not exist")
                return False
                
            expected_columns = {
                'device_id', 'timestamp_start', 'raw_event_hash_id'
            }
            
            actual_columns = {col[0] for col in columns}
            
            if not expected_columns.issubset(actual_columns):
                missing = expected_columns - actual_columns
                logger.error(f"Missing required columns: {missing}")
                return False
                
            logger.info(f"Schema validation passed - {len(columns)} columns found")
            return True
            
        except Exception as e:
            logger.error(f"Schema validation failed: {e}")
            return False
    
    def get_current_row_count(self):
        """Get current row count in candidate events table"""
        try:
            self.cursor.execute("SELECT COUNT(*) FROM \"05_candidate_events\";")
            count = self.cursor.fetchone()[0]
            logger.info(f"Current table row count: {count:,}")
            return count
        except Exception as e:
            logger.error(f"Failed to get row count: {e}")
            return -1
    
    def truncate_table(self):
        """Safely truncate the candidate events table"""
        try:
            logger.info("Truncating 05_candidate_events table...")
            self.cursor.execute('TRUNCATE TABLE "05_candidate_events" RESTART IDENTITY;')
            logger.info("Table truncated successfully")
            return True
        except Exception as e:
            logger.error(f"Table truncation failed: {e}")
            return False
    
    def validate_csv_structure(self, csv_file):
        """Validate CSV file structure before loading"""
        try:
            # Read just the header and first few rows for validation
            df_sample = pd.read_csv(csv_file, nrows=5)
            
            required_columns = {
                'device_id', 'timestamp_start', 'raw_event_hash_id'
            }
            
            csv_columns = set(df_sample.columns)
            
            if not required_columns.issubset(csv_columns):
                missing = required_columns - csv_columns
                logger.error(f"CSV missing required columns: {missing}")
                return False, 0
            
            # Get total row count
            total_rows = sum(1 for line in open(csv_file)) - 1  # Subtract header
            
            logger.info(f"CSV validation passed - {len(csv_columns)} columns, {total_rows:,} rows")
            return True, total_rows
            
        except Exception as e:
            logger.error(f"CSV validation failed: {e}")
            return False, 0
    
    def load_csv_via_copy(self, csv_file):
        """Load CSV using high-performance COPY command"""
        try:
            # Use absolute path for COPY command
            csv_path = Path(csv_file).resolve()
            
            logger.info(f"Starting COPY operation from: {csv_path}")
            start_time = time.time()
            
            # Use copy_from with file handle for better compatibility
            with open(csv_path, 'r') as f:
                # Skip header line
                next(f)
                self.cursor.copy_from(
                    f, 
                    '05_candidate_events',
                    columns=('device_id', 'timestamp_start', 'raw_event_hash_id'),
                    sep=','
                )
            rows_copied = self.cursor.rowcount
            
            load_time = time.time() - start_time
            
            logger.info(f"COPY completed successfully:")
            logger.info(f"  Rows loaded: {rows_copied:,}")
            logger.info(f"  Load time: {load_time:.2f} seconds")
            logger.info(f"  Rate: {rows_copied/load_time:,.0f} rows/second")
            
            return True, rows_copied, load_time
            
        except Exception as e:
            logger.error(f"COPY operation failed: {e}")
            return False, 0, 0
    
    def validate_loaded_data(self, expected_rows):
        """Comprehensive validation of loaded data"""
        try:
            logger.info("Performing post-load validation...")
            
            # Row count validation
            self.cursor.execute("SELECT COUNT(*) FROM \"05_candidate_events\";")
            actual_rows = self.cursor.fetchone()[0]
            
            if actual_rows != expected_rows:
                logger.error(f"Row count mismatch: expected {expected_rows:,}, got {actual_rows:,}")
                return False
            
            # Primary key validation
            self.cursor.execute("""
                SELECT COUNT(*) as total_rows, 
                       COUNT(DISTINCT raw_event_hash_id) as unique_hash_ids
                FROM "05_candidate_events";
            """)
            total_rows, unique_hashes = self.cursor.fetchone()
            
            if total_rows != unique_hashes:
                logger.error(f"Primary key validation failed: {total_rows:,} rows but only {unique_hashes:,} unique hash IDs")
                return False
            
            # Data quality checks
            self.cursor.execute("""
                SELECT 
                    COUNT(*) as total_events,
                    COUNT(DISTINCT device_id) as unique_devices,
                    MIN(timestamp_start) as earliest_timestamp,
                    MAX(timestamp_start) as latest_timestamp
                FROM "05_candidate_events";
            """)
            
            stats = self.cursor.fetchone()
            total_events, unique_devices, earliest, latest = stats
            
            logger.info("Data validation results:")
            logger.info(f"  Total events: {total_events:,}")
            logger.info(f"  Unique devices: {unique_devices}")
            logger.info(f"  Time range: {earliest} to {latest}")
            logger.info("  CPD processing completed successfully")
            
            # All events successfully loaded and validated
            
            # Check for any null critical fields
            self.cursor.execute("""
                SELECT 
                    COUNT(*) FILTER (WHERE device_id IS NULL) as null_device_id,
                    COUNT(*) FILTER (WHERE timestamp_start IS NULL) as null_timestamp,
                    COUNT(*) FILTER (WHERE raw_event_hash_id IS NULL) as null_hash_id
                FROM "05_candidate_events";
            """)
            
            null_device, null_timestamp, null_hash = self.cursor.fetchone()
            
            if null_device > 0 or null_timestamp > 0 or null_hash > 0:
                logger.error(f"Null value validation failed: device_id={null_device}, timestamp={null_timestamp}, hash_id={null_hash}")
                return False
            
            logger.info("All data validation checks passed!")
            return True
            
        except Exception as e:
            logger.error(f"Data validation failed: {e}")
            return False

def main():
    """Main execution function"""
    logger.info("=" * 80)
    logger.info("CPD CANDIDATE EVENTS DATABASE LOADER - PRODUCTION")
    logger.info("=" * 80)
    logger.info(f"Start time: {datetime.now()}")
    
    # Define expected CSV file
    csv_file = Path(__file__).parent / "05_candidate_events_final.csv"
    
    # Check if CSV file exists
    if not csv_file.exists():
        logger.error(f"CSV file not found: {csv_file}")
        logger.error("Please ensure the CPD orchestrator has completed successfully")
        sys.exit(1)
    
    loader = DatabaseLoader()
    success = False
    
    try:
        # Step 1: Connect to database
        if not loader.connect():
            sys.exit(1)
        
        # Step 2: Validate table schema
        if not loader.validate_table_schema():
            sys.exit(1)
        
        # Step 3: Get initial row count
        initial_rows = loader.get_current_row_count()
        
        # Step 4: Validate CSV structure
        csv_valid, expected_rows = loader.validate_csv_structure(csv_file)
        if not csv_valid:
            sys.exit(1)
        
        logger.info(f"Ready to load {expected_rows:,} records")
        
        # Step 5: Begin transaction and truncate table
        # Transaction will be handled automatically by psycopg2
        
        if not loader.truncate_table():
            loader.conn.rollback()
            sys.exit(1)
        
        # Step 6: Load data via COPY command
        copy_success, rows_loaded, load_time = loader.load_csv_via_copy(csv_file)
        
        if not copy_success or rows_loaded == 0:
            logger.error("COPY operation failed or loaded 0 rows")
            loader.conn.rollback()
            sys.exit(1)
        
        # Step 7: Validate loaded data
        if not loader.validate_loaded_data(expected_rows):
            logger.error("Data validation failed - rolling back transaction")
            loader.conn.rollback()
            sys.exit(1)
        
        # Step 8: Commit transaction
        loader.conn.commit()
        success = True
        
        logger.info("=" * 80)
        logger.info("DATABASE LOADING COMPLETED SUCCESSFULLY!")
        logger.info("=" * 80)
        logger.info(f"Records loaded: {rows_loaded:,}")
        logger.info(f"Load time: {load_time:.2f} seconds")
        logger.info(f"Performance: {rows_loaded/load_time:,.0f} rows/second")
        logger.info(f"End time: {datetime.now()}")
        
        # Create success metrics file
        metrics = {
            'load_timestamp': datetime.now().isoformat(),
            'records_loaded': rows_loaded,
            'load_time_seconds': load_time,
            'load_rate_per_second': rows_loaded/load_time,
            'csv_file': str(csv_file),
            'validation_passed': True
        }
        
        metrics_file = Path(__file__).parent / f'load_metrics_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        with open(metrics_file, 'w') as f:
            json.dump(metrics, f, indent=2)
        
        logger.info(f"Metrics saved to: {metrics_file}")
        
    except Exception as e:
        logger.error(f"Critical error during loading: {e}")
        if loader.conn:
            loader.conn.rollback()
            logger.info("Transaction rolled back")
        sys.exit(1)
        
    finally:
        loader.close()
    
    if success:
        logger.info("CPD candidate events are now loaded and ready for production use!")
        sys.exit(0)
    else:
        logger.error("Database loading failed - see logs for details")
        sys.exit(1)

if __name__ == "__main__":
    main()