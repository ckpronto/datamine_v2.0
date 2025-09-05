#!/usr/bin/env python3
"""
01_ingest_raw_telemetry.py

This script ingests raw telemetry data from CSV files into the datamine_main_db database.
It creates the 01_raw_telemetry table and uses PostgreSQL's COPY command for high-performance bulk loading.

Performance optimizations:
- Uses PostgreSQL's native COPY command for 10-50x faster bulk loading
- Creates indexes AFTER data ingestion is complete
- Lets PostgreSQL handle data type conversions automatically
- Stores extras as raw TEXT without JSON processing (JSON validation handled later in transform)
- Checks if table exists before creation for faster repeated runs
- Optional --recreate-table flag to force table recreation

Usage:
    python3 01_ingest_raw_telemetry.py [csv_file_path] [--recreate-table]

Examples:
    python3 01_ingest_raw_telemetry.py /home/ck/DataMine/lake_snapshots_2025-07-30_2025-08-12.csv
    python3 01_ingest_raw_telemetry.py /path/to/data.csv --recreate-table
"""

import sys
import os
import psycopg2
import psycopg2.extras
import csv
import logging
import tempfile
import io
import argparse
from datetime import datetime
from pathlib import Path

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'datamine_v2_db',
    'user': 'ahs_user',
    'password': 'ahs_password'
}

def setup_logging():
    """Set up logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f'/home/ck/DataMine/scripts_database_build/ingestion_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
            logging.StreamHandler()
        ]
    )

def get_db_connection():
    """Create and return a database connection."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        return conn
    except psycopg2.Error as e:
        logging.error(f"Failed to connect to database: {e}")
        raise

def table_exists(conn, table_name):
    """Check if a table exists in the database."""
    check_sql = """
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = %s
    );
    """
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(check_sql, (table_name,))
            exists = cursor.fetchone()[0]
            return exists
    except psycopg2.Error as e:
        logging.error(f"Failed to check if table exists: {e}")
        raise

def drop_table_if_exists(conn, table_name):
    """Drop table if it exists (used with --recreate-table flag)."""
    drop_sql = f'DROP TABLE IF EXISTS "{table_name}" CASCADE;'
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(drop_sql)
            conn.commit()
            logging.info(f"Dropped existing table: {table_name}")
    except psycopg2.Error as e:
        conn.rollback()
        logging.error(f"Failed to drop table: {e}")
        raise

def create_raw_telemetry_table(conn, force_recreate=False):
    """Create the 01_raw_telemetry table without indexes for faster loading."""
    table_name = "01_raw_telemetry"
    
    # Check if table already exists
    if table_exists(conn, table_name):
        if force_recreate:
            logging.info(f"Table {table_name} exists, but --recreate-table flag specified. Dropping and recreating...")
            drop_table_if_exists(conn, table_name)
        else:
            logging.info(f"Table {table_name} already exists. Skipping table creation for faster ingestion.")
            return False  # Table was not created
    
    create_table_sql = """
    CREATE TABLE "01_raw_telemetry" (
        id SERIAL PRIMARY KEY,
        timestamp TIMESTAMPTZ NOT NULL,
        device_id VARCHAR(100) NOT NULL,
        state VARCHAR(100),
        software_state VARCHAR(50),
        system_engaged BOOLEAN,
        current_speed DOUBLE PRECISION,
        current_position TEXT,  -- Storing as text initially, can parse coordinates later
        load_weight INTEGER,
        prndl VARCHAR(20),
        parking_brake_applied BOOLEAN,
        extras TEXT,
        ingestion_timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(create_table_sql)
            conn.commit()
            logging.info("Successfully created 01_raw_telemetry table (without indexes for faster loading)")
            return True  # Table was created
    except psycopg2.Error as e:
        conn.rollback()
        logging.error(f"Failed to create table: {e}")
        raise

def create_indexes_after_ingestion(conn):
    """Create indexes after data ingestion is complete for optimal performance."""
    index_commands = [
        "CREATE INDEX IF NOT EXISTS idx_01_raw_telemetry_timestamp ON \"01_raw_telemetry\" (timestamp);",
        "CREATE INDEX IF NOT EXISTS idx_01_raw_telemetry_device_id ON \"01_raw_telemetry\" (device_id);", 
        "CREATE INDEX IF NOT EXISTS idx_01_raw_telemetry_state ON \"01_raw_telemetry\" (state);",
        "CREATE INDEX IF NOT EXISTS idx_01_raw_telemetry_device_timestamp ON \"01_raw_telemetry\" (device_id, timestamp);",
        "CREATE INDEX IF NOT EXISTS idx_01_raw_telemetry_ingestion ON \"01_raw_telemetry\" (ingestion_timestamp);"
    ]
    
    try:
        logging.info("Creating indexes after data ingestion...")
        with conn.cursor() as cursor:
            for i, index_cmd in enumerate(index_commands, 1):
                logging.info(f"Creating index {i}/{len(index_commands)}...")
                cursor.execute(index_cmd)
            
            # Run ANALYZE to update query planner statistics
            logging.info("Running ANALYZE to update query planner statistics...")
            cursor.execute('ANALYZE "01_raw_telemetry";')
            
            conn.commit()
            logging.info("Successfully created all indexes and updated statistics")
    except psycopg2.Error as e:
        conn.rollback()
        logging.error(f"Failed to create indexes: {e}")
        raise

def validate_csv_file(csv_path):
    """Validate that the CSV file exists and has the expected structure."""
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    expected_headers = [
        'timestamp', 'device_id', 'state', 'software_state', 'system_engaged',
        'current_speed', 'current_position', 'load_weight', 'prndl', 
        'parking_brake_applied', 'extras'
    ]
    
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader)
            
            if headers != expected_headers:
                raise ValueError(f"CSV headers don't match expected format.\nExpected: {expected_headers}\nFound: {headers}")
            
        logging.info(f"CSV file validation successful: {csv_path}")
        return True
        
    except Exception as e:
        logging.error(f"CSV validation failed: {e}")
        raise

def prepare_csv_for_copy(input_csv_path, temp_csv_path):
    """Prepare CSV file for PostgreSQL COPY command by directly copying data without processing."""
    logging.info(f"Preparing CSV file for COPY command...")
    
    # Get total row count for progress tracking
    with open(input_csv_path, 'r', encoding='utf-8') as f:
        total_rows = sum(1 for line in f) - 1  # Subtract 1 for header
    
    logging.info(f"Total rows to process: {total_rows:,}")
    
    processed_rows = 0
    skipped_rows = 0
    
    with open(input_csv_path, 'r', encoding='utf-8') as infile, \
         open(temp_csv_path, 'w', encoding='utf-8', newline='') as outfile:
        
        reader = csv.DictReader(infile)
        writer = csv.writer(outfile, delimiter='\t')  # Use tab delimiter for COPY
        
        for row in reader:
            try:
                # Store extras as raw text without any JSON processing
                extras_str = row['extras']
                # Only set to None if completely empty, otherwise store as-is
                extras_raw = extras_str if extras_str and extras_str.strip() else None
                
                # Prepare the row for COPY (tab-delimited, NULL for empty values)
                copy_row = [
                    row['timestamp'] or None,
                    row['device_id'] or None,
                    row['state'] or None,
                    row['software_state'] or None,
                    row['system_engaged'] or None,
                    row['current_speed'] or None,
                    row['current_position'] or None,
                    row['load_weight'] or None,
                    row['prndl'] or None,
                    row['parking_brake_applied'] or None,
                    extras_raw
                ]
                
                # Convert None values to \N for PostgreSQL COPY
                copy_row = ['\\N' if x is None or x == '' else str(x) for x in copy_row]
                
                writer.writerow(copy_row)
                processed_rows += 1
                
                if processed_rows % 100000 == 0:
                    logging.info(f"Prepared {processed_rows:,} / {total_rows:,} rows ({(processed_rows/total_rows*100):.1f}%)")
                    
            except Exception as e:
                logging.error(f"Error processing row {processed_rows + 1}: {e}")
                skipped_rows += 1
                continue
    
    logging.info(f"CSV preparation complete. Processed: {processed_rows:,}, Skipped: {skipped_rows:,}")
    return processed_rows

def ingest_csv_data(conn, csv_path):
    """Ingest data from CSV file using PostgreSQL's high-performance COPY command."""
    logging.info(f"Starting high-performance ingestion of CSV file: {csv_path}")
    
    # Create temporary file for prepared CSV data
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.tsv') as temp_file:
        temp_csv_path = temp_file.name
    
    try:
        # Prepare CSV file for COPY command
        expected_rows = prepare_csv_for_copy(csv_path, temp_csv_path)
        
        # Use PostgreSQL COPY command for ultra-fast bulk loading
        copy_sql = """
        COPY "01_raw_telemetry" (
            timestamp, device_id, state, software_state, system_engaged,
            current_speed, current_position, load_weight, prndl, 
            parking_brake_applied, extras
        ) FROM STDIN WITH (
            FORMAT text,
            DELIMITER E'\t',
            NULL '\\N',
            ENCODING 'UTF8'
        )
        """
        
        logging.info("Executing COPY command for bulk data loading...")
        start_time = datetime.now()
        
        with conn.cursor() as cursor:
            with open(temp_csv_path, 'r', encoding='utf-8') as f:
                cursor.copy_expert(copy_sql, f)
            
            # Get actual inserted row count
            cursor.execute('SELECT COUNT(*) FROM "01_raw_telemetry"')
            actual_rows = cursor.fetchone()[0]
            
        conn.commit()
        end_time = datetime.now()
        
        duration = end_time - start_time
        rate = actual_rows / duration.total_seconds() if duration.total_seconds() > 0 else 0
        
        logging.info(f"COPY command completed successfully!")
        logging.info(f"Rows inserted: {actual_rows:,}")
        logging.info(f"Duration: {duration}")
        logging.info(f"Ingestion rate: {rate:,.0f} rows/second")
        
        return actual_rows
        
    except psycopg2.Error as e:
        conn.rollback()
        logging.error(f"Database error during COPY ingestion: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error during ingestion: {e}")
        raise
    finally:
        # Clean up temporary file
        try:
            os.unlink(temp_csv_path)
        except OSError:
            pass

def get_table_stats(conn):
    """Get statistics about the ingested data."""
    stats_sql = """
    SELECT 
        COUNT(*) as total_rows,
        MIN(timestamp) as earliest_timestamp,
        MAX(timestamp) as latest_timestamp,
        COUNT(DISTINCT device_id) as unique_devices,
        COUNT(DISTINCT state) as unique_states
    FROM "01_raw_telemetry";
    """
    
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute(stats_sql)
            stats = cursor.fetchone()
            
            logging.info("=== INGESTION STATISTICS ===")
            logging.info(f"Total rows: {stats['total_rows']:,}")
            logging.info(f"Date range: {stats['earliest_timestamp']} to {stats['latest_timestamp']}")
            logging.info(f"Unique devices: {stats['unique_devices']}")
            logging.info(f"Unique states: {stats['unique_states']}")
            
            return dict(stats)
            
    except psycopg2.Error as e:
        logging.error(f"Failed to get table statistics: {e}")
        return None

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Ingest raw telemetry data from CSV files into PostgreSQL database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 01_ingest_raw_telemetry.py /path/to/data.csv
  python3 01_ingest_raw_telemetry.py /path/to/data.csv --recreate-table
        """
    )
    
    parser.add_argument(
        'csv_file',
        nargs='?',
        default="/home/ck/DataMine/APPLICATION/RAW TELEMETRY DATA/lake_snapshots_2025-07-30_2025-08-12.csv",
        help='Path to the CSV file to ingest (default: %(default)s)'
    )
    
    parser.add_argument(
        '--recreate-table',
        action='store_true',
        help='Force recreation of the table even if it already exists'
    )
    
    return parser.parse_args()

def main():
    """Main execution function with optimized performance workflow."""
    args = parse_arguments()
    csv_file = args.csv_file
    force_recreate = args.recreate_table
    
    if args.csv_file == "/home/ck/DataMine/APPLICATION/RAW TELEMETRY DATA/lake_snapshots_2025-07-30_2025-08-12.csv":
        print(f"Using default CSV file: {csv_file}")
    
    setup_logging()
    
    try:
        # Validate CSV file format first (always done)
        logging.info(f"Validating CSV file format: {csv_file}")
        validate_csv_file(csv_file)
        
        # Connect to database
        logging.info("Connecting to database...")
        conn = get_db_connection()
        
        # Check if table exists and create if necessary
        logging.info("Checking if table exists and creating if necessary...")
        table_was_created = create_raw_telemetry_table(conn, force_recreate)
        
        if table_was_created:
            logging.info("Table was created - this is a fresh setup")
        else:
            logging.info("Table already existed - optimized for repeated runs")
        
        # High-performance data ingestion using COPY
        logging.info("Starting high-performance data ingestion using COPY command...")
        total_start_time = datetime.now()
        rows_inserted = ingest_csv_data(conn, csv_file)
        ingestion_end_time = datetime.now()
        
        ingestion_duration = ingestion_end_time - total_start_time
        logging.info(f"Data ingestion completed in {ingestion_duration}")
        
        # Always create/update indexes after ingestion for optimal performance
        index_start_time = datetime.now()
        create_indexes_after_ingestion(conn)
        index_end_time = datetime.now()
        
        index_duration = index_end_time - index_start_time
        total_duration = index_end_time - total_start_time
        
        logging.info(f"Index creation/update completed in {index_duration}")
        logging.info(f"Total pipeline duration: {total_duration}")
        
        # Calculate performance metrics
        if ingestion_duration.total_seconds() > 0:
            ingestion_rate = rows_inserted / ingestion_duration.total_seconds()
            logging.info(f"Ingestion performance: {ingestion_rate:,.0f} rows/second")
        
        # Get final statistics
        get_table_stats(conn)
        
        conn.close()
        logging.info("Database connection closed.")
        logging.info("=== OPTIMIZATION SUMMARY ===")
        logging.info("✓ Used PostgreSQL COPY command for 10-50x faster bulk loading")
        logging.info("✓ Checked table existence before creation for faster repeated runs")
        logging.info("✓ Created/updated indexes AFTER data ingestion for optimal performance")
        logging.info("✓ Let PostgreSQL handle data type conversions automatically")
        logging.info("✓ Stored extras as raw TEXT without JSON processing (validation deferred to transform stage)")
        logging.info("✓ Updated query planner statistics with ANALYZE")
        
    except Exception as e:
        logging.error(f"Script execution failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()