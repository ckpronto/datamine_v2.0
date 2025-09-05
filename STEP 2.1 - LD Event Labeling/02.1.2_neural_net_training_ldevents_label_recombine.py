#!/usr/bin/env python3
"""
02.2_neural_net_training_ldevents_label_recombine.py

This script processes Label Studio JSON annotation files and recombines them with
telemetry data from the 02_raw_telemetry_transformed table to create labeled training data.

Key Features:
- Scans labelstudio_output/ for JSON annotation files
- Parses Label Studio JSON format to extract annotations with start/end timestamps  
- For each truck-date in labeled data, extracts ALL telemetry from source table
- Uses SQL bulk operations for maximum performance
- Applies labels based on timestamp ranges (load_event, dump_event, "background")
- Creates labeled training dataset with optimized performance

Output:
- Creates table: 02.1_labeled_training_data_loaddump
- Indexes on (device_id, timestamp) and truck_date for performance
- All telemetry data with corresponding ML event labels

Usage:
    python3 02.2_neural_net_training_ldevents_label_recombine.py --input-dir /path/to/labelstudio_output
    python3 02.2_neural_net_training_ldevents_label_recombine.py --dry-run
    python3 02.2_neural_net_training_ldevents_label_recombine.py --recreate-table

Examples:
    # Process all JSON files in default directory
    python3 02.2_neural_net_training_ldevents_label_recombine.py
    
    # Process specific directory
    python3 02.2_neural_net_training_ldevents_label_recombine.py --input-dir /custom/path
    
    # Dry run to see what would be processed
    python3 02.2_neural_net_training_ldevents_label_recombine.py --dry-run
"""

import sys
import os
import psycopg2
import psycopg2.extras
import json
import logging
import argparse
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Tuple, Optional
import glob
import re

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'datamine_v2_db',
    'user': 'ahs_user',
    'password': 'ahs_password'
}

# Default input directory for Label Studio output
DEFAULT_INPUT_DIR = "/home/ck/DataMine/APPLICATION/V2/STEP 2.1 - LD Event Labeling/labelstudio_output"

def setup_logging(log_dir: str = None):
    """Set up logging configuration."""
    if log_dir is None:
        log_dir = os.path.dirname(os.path.abspath(__file__))
    
    log_filename = os.path.join(log_dir, f'label_recombine_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    
    # Clear any existing handlers
    logging.getLogger().handlers.clear()
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler()
        ]
    )
    logging.info(f"Logging to file: {log_filename}")

def get_db_connection():
    """Create and return a database connection."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        return conn
    except psycopg2.Error as e:
        logging.error(f"Failed to connect to database: {e}")
        raise

def create_labeled_training_table(conn):
    """Create the labeled training data table with proper schema and indexes."""
    create_table_sql = """
    DROP TABLE IF EXISTS "02.1_labeled_training_data_loaddump";
    
    CREATE TABLE "02.1_labeled_training_data_loaddump" (
        -- All columns from 02_raw_telemetry_transformed
        timestamp timestamp with time zone NOT NULL,
        ingested_at timestamp with time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
        raw_event_hash_id character(64) NOT NULL,
        device_id text NOT NULL,
        system_engaged boolean,
        parking_brake_applied boolean,
        current_position geography(PointZ,4326),
        current_speed double precision,
        load_weight double precision,
        state telemetry_state_enum DEFAULT 'unknown'::telemetry_state_enum,
        software_state software_state_enum DEFAULT 'unknown'::software_state_enum,
        prndl prndl_enum DEFAULT 'unknown'::prndl_enum,
        extras jsonb,
        
        -- Additional ML labeling columns
        ml_event_label text,
        truck_date text,
        
        PRIMARY KEY (raw_event_hash_id)
    );
    
    -- Create indexes for performance
    CREATE INDEX idx_labeled_training_device_timestamp ON "02.1_labeled_training_data_loaddump" (device_id, timestamp);
    CREATE INDEX idx_labeled_training_truck_date ON "02.1_labeled_training_data_loaddump" (truck_date);
    CREATE INDEX idx_labeled_training_ml_label ON "02.1_labeled_training_data_loaddump" (ml_event_label);
    CREATE INDEX idx_labeled_training_device_id ON "02.1_labeled_training_data_loaddump" (device_id);
    CREATE INDEX idx_labeled_training_timestamp ON "02.1_labeled_training_data_loaddump" (timestamp);
    
    -- BRIN index for time-series data optimization
    CREATE INDEX idx_labeled_training_timestamp_brin ON "02.1_labeled_training_data_loaddump" USING BRIN (timestamp);
    """
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(create_table_sql)
            conn.commit()
            logging.info("Created labeled training data table with indexes")
    except psycopg2.Error as e:
        conn.rollback()
        logging.error(f"Failed to create table: {e}")
        raise

def verify_source_table_exists(conn):
    """Verify that the source table exists and has data."""
    check_sql = """
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = '02_raw_telemetry_transformed'
    );
    """
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(check_sql)
            exists = cursor.fetchone()[0]
            
            if not exists:
                raise ValueError("Source table '02_raw_telemetry_transformed' does not exist. Please run transformation scripts first.")
            
            # Check if table has data
            cursor.execute('SELECT COUNT(*) FROM "02_raw_telemetry_transformed"')
            count = cursor.fetchone()[0]
            
            if count == 0:
                raise ValueError("Source table '02_raw_telemetry_transformed' exists but contains no data.")
            
            logging.info(f"Source table verified: {count:,} rows available")
            return count
            
    except psycopg2.Error as e:
        logging.error(f"Failed to verify source table: {e}")
        raise

def find_label_studio_json_files(input_dir: str) -> List[str]:
    """Find all Label Studio JSON files in the input directory."""
    json_files = []
    
    # Look for files matching the pattern: labels_*.json
    pattern = os.path.join(input_dir, "labels_*.json")
    files = glob.glob(pattern)
    
    for file_path in files:
        if os.path.isfile(file_path):
            json_files.append(file_path)
    
    logging.info(f"Found {len(json_files)} Label Studio JSON files in {input_dir}")
    for file_path in json_files:
        logging.info(f"  - {os.path.basename(file_path)}")
    
    return json_files

def extract_truck_date_from_filename(filename: str) -> Tuple[Optional[str], Optional[str]]:
    """Extract truck ID and date from Label Studio filename."""
    # Pattern: labels_lake-605-8-0883_round1.json
    # Extract truck ID from the filename
    basename = os.path.basename(filename)
    
    # Remove 'labels_' prefix and '.json' suffix
    if basename.startswith('labels_') and basename.endswith('.json'):
        middle_part = basename[7:-5]  # Remove 'labels_' and '.json'
        
        # Split by '_' and take everything except the last part (round1)
        parts = middle_part.split('_')
        if len(parts) >= 2:
            truck_id = '_'.join(parts[:-1])  # Everything except the last part
            return truck_id, None  # We'll get dates from the annotations
    
    logging.warning(f"Could not extract truck ID from filename: {filename}")
    return None, None

def parse_label_studio_json(file_path: str) -> List[Dict]:
    """Parse Label Studio JSON file and extract annotations with timestamps."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        annotations = []
        
        for task in data:
            if 'annotations' not in task or not task['annotations']:
                continue
            
            # Get truck_id and date from task data
            truck_data = task.get('data', {})
            truck_id = truck_data.get('truck_id')
            date = truck_data.get('date')
            
            if not truck_id:
                # Try to extract from filename if not in data
                truck_id_from_file, _ = extract_truck_date_from_filename(file_path)
                if truck_id_from_file:
                    truck_id = truck_id_from_file
            
            # If still no truck_id, skip this task
            if not truck_id:
                logging.warning(f"No truck_id found for task {task.get('id')} in {file_path}")
                continue
            
            for annotation in task['annotations']:
                if 'result' not in annotation:
                    continue
                
                for result in annotation['result']:
                    if result.get('type') == 'timeserieslabels' and 'value' in result:
                        value = result['value']
                        if 'start' in value and 'end' in value:
                            label = value.get('timeserieslabels', [])
                            if label and len(label) > 0:
                                # Extract date from timestamp if not provided in data
                                task_date = date
                                if not task_date:
                                    try:
                                        # Parse ISO timestamp and extract date
                                        dt = datetime.fromisoformat(value['start'].replace('Z', '+00:00'))
                                        task_date = dt.strftime('%Y-%m-%d')
                                    except:
                                        logging.warning(f"Could not extract date from timestamp: {value['start']}")
                                        continue
                                
                                annotations.append({
                                    'truck_id': truck_id,
                                    'date': task_date,
                                    'start_time': value['start'],
                                    'end_time': value['end'],
                                    'label': label[0],  # Take first label
                                    'task_id': task.get('id'),
                                    'annotation_id': annotation.get('id')
                                })
        
        logging.info(f"Extracted {len(annotations)} annotations from {os.path.basename(file_path)}")
        return annotations
        
    except Exception as e:
        logging.error(f"Failed to parse JSON file {file_path}: {e}")
        return []

def get_unique_truck_dates(annotations: List[Dict]) -> List[Tuple[str, str]]:
    """Get unique truck-date combinations from annotations."""
    truck_dates = set()
    
    for ann in annotations:
        truck_id = ann['truck_id']
        
        # Extract date from start_time if date is None
        date = ann['date']
        if not date and ann['start_time']:
            try:
                # Parse ISO timestamp and extract date
                dt = datetime.fromisoformat(ann['start_time'].replace('Z', '+00:00'))
                date = dt.strftime('%Y-%m-%d')
            except:
                continue
        
        if truck_id and date:
            truck_dates.add((truck_id, date))
    
    result = list(truck_dates)
    logging.info(f"Found {len(result)} unique truck-date combinations:")
    for truck_id, date in sorted(result):
        logging.info(f"  - {truck_id} on {date}")
    
    return result

def create_label_mapping_temp_table(conn, annotations: List[Dict]):
    """Create temporary table with label mappings for efficient joins."""
    create_temp_sql = """
    CREATE TEMPORARY TABLE temp_label_mappings (
        truck_id text,
        start_time timestamp with time zone,
        end_time timestamp with time zone,
        label text
    );
    
    CREATE INDEX idx_temp_label_truck_time ON temp_label_mappings (truck_id, start_time, end_time);
    """
    
    insert_sql = """
    INSERT INTO temp_label_mappings (truck_id, start_time, end_time, label) 
    VALUES %s
    """
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(create_temp_sql)
            
            # Prepare data for bulk insert
            label_data = []
            for ann in annotations:
                if ann['truck_id'] and ann['start_time'] and ann['end_time'] and ann['label']:
                    try:
                        # Convert ISO timestamps to PostgreSQL format
                        start_time = ann['start_time'].replace('Z', '+00:00')
                        end_time = ann['end_time'].replace('Z', '+00:00')
                        
                        label_data.append((
                            ann['truck_id'],
                            start_time,
                            end_time,
                            ann['label']
                        ))
                    except Exception as e:
                        logging.warning(f"Skipping invalid annotation: {e}")
                        continue
            
            if label_data:
                psycopg2.extras.execute_values(cursor, insert_sql, label_data, template=None)
                logging.info(f"Created temporary label mappings table with {len(label_data)} entries")
            else:
                logging.warning("No valid label data to insert")
        
    except psycopg2.Error as e:
        logging.error(f"Failed to create label mapping table: {e}")
        raise

def insert_labeled_training_data(conn, truck_dates: List[Tuple[str, str]]):
    """Insert labeled training data using optimized bulk operations."""
    
    # SQL query to extract and label all telemetry data for truck-dates
    insert_sql = """
    WITH telemetry_data AS (
        SELECT 
            t.*,
            %s || '_' || %s as truck_date
        FROM "02_raw_telemetry_transformed" t
        WHERE t.device_id = %s 
        AND t.timestamp::date = %s::date
    ),
    labeled_data AS (
        SELECT 
            td.*,
            CASE 
                WHEN EXISTS (
                    SELECT 1 FROM temp_label_mappings lm 
                    WHERE lm.truck_id = td.device_id 
                    AND td.timestamp >= lm.start_time 
                    AND td.timestamp <= lm.end_time
                    AND lm.label = 'load_event'
                ) THEN 'load_event'
                WHEN EXISTS (
                    SELECT 1 FROM temp_label_mappings lm 
                    WHERE lm.truck_id = td.device_id 
                    AND td.timestamp >= lm.start_time 
                    AND td.timestamp <= lm.end_time
                    AND lm.label = 'dump_event'
                ) THEN 'dump_event'
                ELSE 'background'
            END as ml_event_label
        FROM telemetry_data td
    )
    INSERT INTO "02.1_labeled_training_data_loaddump" (
        timestamp, ingested_at, raw_event_hash_id, device_id, system_engaged,
        parking_brake_applied, current_position, current_speed, load_weight,
        state, software_state, prndl, extras, ml_event_label, truck_date
    )
    SELECT 
        timestamp, ingested_at, raw_event_hash_id, device_id, system_engaged,
        parking_brake_applied, current_position, current_speed, load_weight,
        state, software_state, prndl, extras, ml_event_label, truck_date
    FROM labeled_data;
    """
    
    total_inserted = 0
    
    try:
        with conn.cursor() as cursor:
            for i, (truck_id, date) in enumerate(truck_dates, 1):
                logging.info(f"Processing {i}/{len(truck_dates)}: {truck_id} on {date}")
                
                cursor.execute(insert_sql, (truck_id, date, truck_id, date))
                row_count = cursor.rowcount
                total_inserted += row_count
                
                logging.info(f"  Inserted {row_count:,} labeled records")
                
                # Commit after each truck-date to avoid long transactions
                conn.commit()
        
        logging.info(f"Successfully inserted {total_inserted:,} total labeled training records")
        return total_inserted
        
    except psycopg2.Error as e:
        conn.rollback()
        logging.error(f"Failed to insert labeled training data: {e}")
        raise

def generate_summary_statistics(conn):
    """Generate summary statistics for the labeled training data."""
    stats_sql = """
    SELECT 
        ml_event_label,
        COUNT(*) as record_count,
        COUNT(DISTINCT device_id) as unique_trucks,
        COUNT(DISTINCT truck_date) as unique_truck_dates,
        MIN(timestamp) as earliest_timestamp,
        MAX(timestamp) as latest_timestamp
    FROM "02.1_labeled_training_data_loaddump"
    GROUP BY ml_event_label
    ORDER BY record_count DESC;
    """
    
    total_sql = """
    SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT device_id) as total_trucks,
        COUNT(DISTINCT truck_date) as total_truck_dates,
        MIN(timestamp) as earliest_timestamp,
        MAX(timestamp) as latest_timestamp
    FROM "02.1_labeled_training_data_loaddump";
    """
    
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            logging.info("=== LABELED TRAINING DATA STATISTICS ===")
            
            # Get overall statistics
            cursor.execute(total_sql)
            total_stats = cursor.fetchone()
            
            logging.info(f"Total Records: {total_stats['total_records']:,}")
            logging.info(f"Unique Trucks: {total_stats['total_trucks']}")
            logging.info(f"Unique Truck-Dates: {total_stats['total_truck_dates']}")
            logging.info(f"Time Range: {total_stats['earliest_timestamp']} to {total_stats['latest_timestamp']}")
            
            # Get label distribution
            cursor.execute(stats_sql)
            label_stats = cursor.fetchall()
            
            logging.info("\nLabel Distribution:")
            for row in label_stats:
                percentage = (row['record_count'] / total_stats['total_records']) * 100
                logging.info(f"  {row['ml_event_label']:12}: {row['record_count']:8,} records ({percentage:5.1f}%)")
                logging.info(f"                   {row['unique_trucks']} trucks, {row['unique_truck_dates']} truck-dates")
            
    except psycopg2.Error as e:
        logging.error(f"Failed to generate statistics: {e}")

def validate_data_quality(conn):
    """Validate the quality of the labeled training data."""
    validation_queries = {
        "Null Labels": """
            SELECT COUNT(*) FROM "02.1_labeled_training_data_loaddump" 
            WHERE ml_event_label IS NULL
        """,
        "Invalid Labels": """
            SELECT COUNT(*) FROM "02.1_labeled_training_data_loaddump" 
            WHERE ml_event_label NOT IN ('load_event', 'dump_event', 'background')
        """,
        "Missing Truck Dates": """
            SELECT COUNT(*) FROM "02.1_labeled_training_data_loaddump" 
            WHERE truck_date IS NULL OR truck_date = ''
        """,
        "Duplicate Hash IDs": """
            SELECT COUNT(*) - COUNT(DISTINCT raw_event_hash_id) 
            FROM "02.1_labeled_training_data_loaddump"
        """
    }
    
    try:
        with conn.cursor() as cursor:
            logging.info("=== DATA QUALITY VALIDATION ===")
            
            all_good = True
            for check_name, query in validation_queries.items():
                cursor.execute(query)
                count = cursor.fetchone()[0]
                
                if count > 0:
                    logging.warning(f"{check_name}: {count} issues found")
                    all_good = False
                else:
                    logging.info(f"{check_name}: PASSED")
            
            if all_good:
                logging.info("All data quality checks PASSED")
            else:
                logging.warning("Some data quality issues detected - review the data")
                
    except psycopg2.Error as e:
        logging.error(f"Failed to validate data quality: {e}")

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Process Label Studio annotations and create labeled training data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process all JSON files in default directory
  python3 02.2_neural_net_training_ldevents_label_recombine.py
  
  # Process specific directory
  python3 02.2_neural_net_training_ldevents_label_recombine.py --input-dir /custom/path
  
  # Dry run to see what would be processed
  python3 02.2_neural_net_training_ldevents_label_recombine.py --dry-run
        """
    )
    
    parser.add_argument(
        '--input-dir',
        type=str,
        default=DEFAULT_INPUT_DIR,
        help=f'Input directory containing Label Studio JSON files (default: {DEFAULT_INPUT_DIR})'
    )
    
    parser.add_argument(
        '--recreate-table',
        action='store_true',
        help='Recreate the labeled training data table (drops existing data)'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be processed without actually modifying the database'
    )
    
    return parser.parse_args()

def main():
    """Main execution function."""
    args = parse_arguments()
    
    # Setup logging
    setup_logging()
    logging.info("Label recombination script started successfully")
    
    try:
        # Verify input directory exists
        if not os.path.exists(args.input_dir):
            raise ValueError(f"Input directory does not exist: {args.input_dir}")
        
        # Find Label Studio JSON files
        json_files = find_label_studio_json_files(args.input_dir)
        if not json_files:
            logging.warning(f"No Label Studio JSON files found in {args.input_dir}")
            return
        
        # Parse all JSON files and extract annotations
        all_annotations = []
        for json_file in json_files:
            annotations = parse_label_studio_json(json_file)
            all_annotations.extend(annotations)
        
        if not all_annotations:
            logging.error("No valid annotations found in JSON files")
            return
        
        logging.info(f"Total annotations extracted: {len(all_annotations)}")
        
        # Get unique truck-date combinations
        truck_dates = get_unique_truck_dates(all_annotations)
        if not truck_dates:
            logging.error("No valid truck-date combinations found")
            return
        
        if args.dry_run:
            logging.info("DRY RUN - No database changes will be made")
            logging.info(f"Would process {len(truck_dates)} truck-date combinations:")
            for truck_id, date in sorted(truck_dates):
                logging.info(f"  - {truck_id} on {date}")
            
            logging.info(f"Would create labels for {len(all_annotations)} annotation ranges")
            return
        
        # Connect to database
        logging.info("Connecting to database...")
        conn = get_db_connection()
        
        # Verify source table exists
        verify_source_table_exists(conn)
        
        # Create/recreate labeled training table
        if args.recreate_table:
            logging.info("Recreating labeled training data table...")
            create_labeled_training_table(conn)
        else:
            # Check if table exists, create if not
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = '02.1_labeled_training_data_loaddump'
                    );
                """)
                if not cursor.fetchone()[0]:
                    logging.info("Creating labeled training data table...")
                    create_labeled_training_table(conn)
        
        # Create temporary label mapping table
        logging.info("Creating temporary label mappings...")
        create_label_mapping_temp_table(conn, all_annotations)
        
        # Insert labeled training data
        logging.info("Inserting labeled training data...")
        total_inserted = insert_labeled_training_data(conn, truck_dates)
        
        # Generate statistics and validation
        generate_summary_statistics(conn)
        validate_data_quality(conn)
        
        conn.close()
        logging.info("Database connection closed.")
        logging.info("=== SCRIPT COMPLETED SUCCESSFULLY ===")
        logging.info(f"Created labeled training dataset with {total_inserted:,} records")
        
    except Exception as e:
        logging.error(f"Script execution failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()