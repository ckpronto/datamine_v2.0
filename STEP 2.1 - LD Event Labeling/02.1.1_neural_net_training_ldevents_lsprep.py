#!/usr/bin/env python3
"""
03_neural_net_training_ldevents_lsprep.py

This script extracts telemetry data from the 02_raw_telemetry_transformed table
and formats it for Label Studio import for loading/dumping event labeling.

Key Features:
- Extracts data by truck ID(s) and date range(s) 
- Generates Label Studio-optimized JSON time series data files and task files
- Optimized for time-series labeling of loading/dumping events
- High-performance database queries with proper indexing
- Flexible truck and date selection with comprehensive validation
- Progress reporting and comprehensive logging

Output Format:
- Data files: {truck_id}_{date}_data.json (time series telemetry data optimized for Label Studio)
- Task files: {truck_id}_{date}_labelstudio.json (Label Studio task definitions)

Usage:
    python3 03_neural_net_training_ldevents_lsprep.py --trucks T001,T002 --start-date 2025-08-01 --end-date 2025-08-15 --output-dir /path/to/output
    python3 03_neural_net_training_ldevents_lsprep.py --trucks T001 --date 2025-08-01 --output-dir ./labelstudio_data
    python3 03_neural_net_training_ldevents_lsprep.py --all-trucks --start-date 2025-08-01 --end-date 2025-08-03

Examples:
    # Single truck, single date
    python3 03_neural_net_training_ldevents_lsprep.py --trucks T001 --date 2025-08-01
    
    # Multiple trucks, date range
    python3 03_neural_net_training_ldevents_lsprep.py --trucks T001,T002,T003 --start-date 2025-08-01 --end-date 2025-08-15
    
    # All trucks for specific date range
    python3 03_neural_net_training_ldevents_lsprep.py --all-trucks --start-date 2025-08-01 --end-date 2025-08-03
"""

import sys
import os
import psycopg2
import psycopg2.extras
import csv
import json
import logging
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Tuple, Optional
import urllib.parse

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'datamine_main_db',
    'user': 'ahs_user',
    'password': 'ahs_password'
}

# Default output directory
DEFAULT_OUTPUT_DIR = "/home/ck/DataMine/labelstudio_input"

def setup_logging(output_dir: str):
    """Set up logging configuration."""
    log_filename = os.path.join(output_dir, f'lsprep_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    
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

def get_available_trucks(conn) -> List[str]:
    """Get list of all available truck IDs from the database."""
    sql = """
    SELECT DISTINCT device_id 
    FROM "02_raw_telemetry_transformed" 
    ORDER BY device_id
    """
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            trucks = [row[0] for row in cursor.fetchall()]
            logging.info(f"Found {len(trucks)} unique trucks: {', '.join(trucks)}")
            return trucks
    except psycopg2.Error as e:
        logging.error(f"Failed to get available trucks: {e}")
        raise

def get_date_range_for_trucks(conn, truck_ids: List[str]) -> Tuple[datetime, datetime]:
    """Get the available date range for specified trucks."""
    placeholders = ','.join(['%s'] * len(truck_ids))
    sql = f"""
    SELECT 
        MIN(timestamp::date) as min_date,
        MAX(timestamp::date) as max_date
    FROM "02_raw_telemetry_transformed" 
    WHERE device_id IN ({placeholders})
    """
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql, truck_ids)
            result = cursor.fetchone()
            min_date, max_date = result
            
            logging.info(f"Date range for trucks {truck_ids}: {min_date} to {max_date}")
            return min_date, max_date
    except psycopg2.Error as e:
        logging.error(f"Failed to get date range: {e}")
        raise

def validate_trucks(conn, truck_ids: List[str]) -> List[str]:
    """Validate that all specified truck IDs exist in the database."""
    available_trucks = get_available_trucks(conn)
    invalid_trucks = [truck for truck in truck_ids if truck not in available_trucks]
    
    if invalid_trucks:
        raise ValueError(f"Invalid truck IDs: {invalid_trucks}. Available trucks: {available_trucks}")
    
    logging.info(f"All specified trucks validated: {truck_ids}")
    return truck_ids

def extract_truck_date_data(conn, truck_id: str, date: datetime) -> List[Dict]:
    """Extract telemetry data for a specific truck and date."""
    sql = """
    SELECT 
        raw_event_hash_id,
        timestamp,
        current_speed,
        load_weight,
        state
    FROM "02_raw_telemetry_transformed"
    WHERE device_id = %s 
    AND timestamp::date = %s
    ORDER BY timestamp ASC
    """
    
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute(sql, (truck_id, date))
            rows = cursor.fetchall()
            
            # Convert to regular dicts and handle None values
            data = []
            for row in rows:
                data.append({
                    'hash_id': row['raw_event_hash_id'],
                    'timestamp': row['timestamp'].isoformat(),
                    'current_speed': row['current_speed'] if row['current_speed'] is not None else 0.0,
                    'load_weight': row['load_weight'] if row['load_weight'] is not None else 0.0,
                    'state': str(row['state']) if row['state'] is not None else 'unknown'
                })
            
            logging.info(f"Extracted {len(data)} records for truck {truck_id} on {date}")
            return data
            
    except psycopg2.Error as e:
        logging.error(f"Failed to extract data for {truck_id} on {date}: {e}")
        raise

def create_csv_file(data: List[Dict], truck_id: str, date: datetime, output_dir: str) -> Dict:
    """Create single CSV file for Label Studio direct import."""
    if not data:
        logging.warning(f"No data to process for {truck_id} on {date}")
        return None
    
    try:
        # Normalize and sort time series data
        time_series_data = []
        for record in data:
            # Ensure numeric fields are proper floats or empty strings
            current_speed = record['current_speed']
            load_weight = record['load_weight']
            
            if current_speed is not None:
                try:
                    current_speed = float(current_speed)
                except (ValueError, TypeError):
                    current_speed = ""
            else:
                current_speed = ""
                    
            if load_weight is not None:
                try:
                    load_weight = float(load_weight) if load_weight != -99.0 else ""
                except (ValueError, TypeError):
                    load_weight = ""
            else:
                load_weight = ""
            
            # Strip timezone offset from timestamp - format as YYYY-MM-DDTHH:MM:SS.ffffff
            timestamp = record['timestamp']
            if isinstance(timestamp, str) and timestamp.endswith('+00:00'):
                timestamp = timestamp[:-6]  # Remove '+00:00'
            elif isinstance(timestamp, str) and 'T' in timestamp and '+' in timestamp:
                timestamp = timestamp.split('+')[0]  # Remove any timezone offset
            
            time_series_data.append({
                "timestamp": timestamp,
                "current_speed": current_speed,
                "load_weight": load_weight,
                "state": record['state'] if record['state'] else "",
                "hash_id": record['hash_id'] if record['hash_id'] else ""
            })
        
        # Sort by timestamp ascending
        time_series_data.sort(key=lambda x: x['timestamp'])
        
        # Create single CSV file for truck-date combination
        date_str = date.strftime("%Y-%m-%d")
        csv_filename = f"{truck_id}_{date_str}.csv"
        csv_path = os.path.join(output_dir, csv_filename)
        
        # Write CSV file with exact header format
        with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            # Exact header as specified
            writer.writerow(['timestamp', 'current_speed', 'load_weight', 'state', 'hash_id'])
            
            for record in time_series_data:
                writer.writerow([
                    record['timestamp'],
                    record['current_speed'] if record['current_speed'] != "" else "",
                    record['load_weight'] if record['load_weight'] != "" else "",
                    record['state'],
                    record['hash_id']
                ])
        
        logging.info(f"Created CSV file: {csv_path} ({len(time_series_data)} records)")
        
        # Create individual JSON task file with csv_url field
        task_filename = f"{truck_id}_{date_str}_task.json"
        task_path = os.path.join(output_dir, task_filename)
        
        task_data = {
            "data": {
                "truck_id": truck_id,
                "date": date_str,
                "csv_url": f"/data/local-files/?d=datamine-input/{csv_filename}"
            }
        }
        
        with open(task_path, 'w', encoding='utf-8') as taskfile:
            json.dump(task_data, taskfile, indent=2, ensure_ascii=False)
        
        logging.info(f"Created JSON task file: {task_path}")
        
        return {
            'csv_file': csv_path,
            'csv_filename': csv_filename,
            'task_file': task_path,
            'task_filename': task_filename,
            'record_count': len(time_series_data)
        }
        
    except Exception as e:
        logging.error(f"Failed to create CSV file: {e}")
        raise




def process_truck_date_combination(conn, truck_id: str, date: datetime, output_dir: str) -> Dict:
    """Process a single truck-date combination and generate single CSV file."""
    date_str = date.strftime("%Y-%m-%d")
    
    # Extract data from database
    data = extract_truck_date_data(conn, truck_id, date)
    
    if not data:
        logging.warning(f"No data found for truck {truck_id} on {date_str}")
        return {
            'truck_id': truck_id,
            'date': date_str,
            'csv_file': None,
            'record_count': 0,
            'status': 'no_data'
        }
    
    # Create single CSV file
    created_file = create_csv_file(data, truck_id, date, output_dir)
    
    if created_file:
        return {
            'truck_id': truck_id,
            'date': date_str,
            'csv_file': created_file['csv_file'],
            'task_file': created_file['task_file'],
            'record_count': created_file['record_count'],
            'status': 'success'
        }
    else:
        return {
            'truck_id': truck_id,
            'date': date_str,
            'csv_file': None,
            'task_file': None,
            'record_count': 0,
            'status': 'failure'
        }

def generate_date_range(start_date: datetime, end_date: datetime) -> List[datetime]:
    """Generate list of dates between start_date and end_date (inclusive)."""
    dates = []
    current = start_date
    while current <= end_date:
        dates.append(current)
        current += timedelta(days=1)
    return dates

def create_output_directory(output_dir: str) -> str:
    """Create output directory if it doesn't exist."""
    try:
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        logging.info(f"Output directory ready: {output_dir}")
        return output_dir
    except Exception as e:
        logging.error(f"Failed to create output directory {output_dir}: {e}")
        raise

def validate_date_range(start_date: datetime, end_date: datetime):
    """Validate that date range is reasonable."""
    if start_date > end_date:
        raise ValueError(f"Start date {start_date} is after end date {end_date}")
    
    delta = end_date - start_date
    if delta.days > 365:
        logging.warning(f"Large date range specified: {delta.days} days. This may generate many files.")
    
    logging.info(f"Date range validated: {start_date} to {end_date} ({delta.days + 1} days)")

def generate_summary_report(results: List[Dict], output_dir: str):
    """Generate a summary report of all processed files."""
    successful_results = [r for r in results if r['status'] == 'success']
    failed_results = [r for r in results if r['status'] != 'success']
    
    total_records = sum(r['record_count'] for r in successful_results)
    
    
    logging.info("=== PROCESSING SUMMARY ===")
    logging.info(f"Total truck-date combinations processed: {len(results)}")
    logging.info(f"Successful: {len(successful_results)}")
    logging.info(f"Failed/No Data: {len(failed_results)}")
    logging.info(f"Total telemetry records processed: {total_records:,}")
    
    # Create summary file
    summary_path = os.path.join(output_dir, f"processing_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    
    summary_data = {
        'timestamp': datetime.now().isoformat(),
        'total_combinations': len(results),
        'successful': len(successful_results),
        'failed': len(failed_results),
        'total_records': total_records,
        'output_directory': output_dir,
        'results': results
    }
    
    try:
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary_data, f, indent=2, ensure_ascii=False)
        logging.info(f"Summary report created: {summary_path}")
    except Exception as e:
        logging.error(f"Failed to create summary report: {e}")

def parse_truck_ids(trucks_str: str) -> List[str]:
    """Parse comma-separated truck IDs."""
    if not trucks_str:
        return []
    return [truck.strip() for truck in trucks_str.split(',') if truck.strip()]

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Extract telemetry data and format for Label Studio import",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Single truck, single date
  python3 03_neural_net_training_ldevents_lsprep.py --trucks T001 --date 2025-08-01
  
  # Multiple trucks, date range  
  python3 03_neural_net_training_ldevents_lsprep.py --trucks T001,T002 --start-date 2025-08-01 --end-date 2025-08-15
  
  # All trucks for specific date range
  python3 03_neural_net_training_ldevents_lsprep.py --all-trucks --start-date 2025-08-01 --end-date 2025-08-03
        """
    )
    
    # Truck selection (mutually exclusive)
    truck_group = parser.add_mutually_exclusive_group(required=True)
    truck_group.add_argument(
        '--trucks',
        type=str,
        help='Comma-separated list of truck IDs (e.g., T001,T002,T003)'
    )
    truck_group.add_argument(
        '--all-trucks',
        action='store_true',
        help='Process all available trucks'
    )
    
    # Date selection arguments
    parser.add_argument(
        '--date',
        type=str,
        help='Single date to process (YYYY-MM-DD format)'
    )
    parser.add_argument(
        '--start-date',
        type=str,
        help='Start date for range (YYYY-MM-DD format)'
    )
    parser.add_argument(
        '--end-date', 
        type=str,
        help='End date for range (YYYY-MM-DD format)'
    )
    
    parser.add_argument(
        '--output-dir',
        type=str,
        default=DEFAULT_OUTPUT_DIR,
        help=f'Output directory for generated files (default: {DEFAULT_OUTPUT_DIR})'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be processed without actually generating files'
    )
    
    args = parser.parse_args()
    
    # Validate date arguments
    if args.date and (args.start_date or args.end_date):
        parser.error("Cannot specify both --date and --start-date/--end-date")
    
    if (args.start_date and not args.end_date) or (args.end_date and not args.start_date):
        parser.error("Both --start-date and --end-date are required when using date range")
    
    if not args.date and not args.start_date:
        parser.error("Must specify either --date or --start-date/--end-date")
    
    return args

def main():
    """Main execution function."""
    args = parse_arguments()
    
    # Create output directory
    output_dir = create_output_directory(args.output_dir)
    setup_logging(output_dir)
    logging.info("Script started successfully")
    
    try:
        # Connect to database
        logging.info("Connecting to database...")
        conn = get_db_connection()
        
        # Verify source table exists
        total_rows = verify_source_table_exists(conn)
        
        # Determine truck IDs to process
        if args.all_trucks:
            truck_ids = get_available_trucks(conn)
        else:
            truck_ids = parse_truck_ids(args.trucks)
            truck_ids = validate_trucks(conn, truck_ids)
        
        # Determine dates to process
        if args.date:
            try:
                single_date = datetime.strptime(args.date, '%Y-%m-%d')
                dates = [single_date]
            except ValueError:
                raise ValueError(f"Invalid date format: {args.date}. Use YYYY-MM-DD format.")
        else:
            try:
                start_date = datetime.strptime(args.start_date, '%Y-%m-%d')
                end_date = datetime.strptime(args.end_date, '%Y-%m-%d')
                validate_date_range(start_date, end_date)
                dates = generate_date_range(start_date, end_date)
            except ValueError as e:
                raise ValueError(f"Invalid date format: {e}. Use YYYY-MM-DD format.")
        
        # Calculate total combinations
        total_combinations = len(truck_ids) * len(dates)
        logging.info(f"Planning to process {total_combinations} truck-date combinations:")
        logging.info(f"Trucks ({len(truck_ids)}): {', '.join(truck_ids)}")
        logging.info(f"Dates ({len(dates)}): {dates[0].strftime('%Y-%m-%d')} to {dates[-1].strftime('%Y-%m-%d')}")
        
        if args.dry_run:
            logging.info("DRY RUN - No files will be generated")
            for truck_id in truck_ids:
                for date in dates:
                    logging.info(f"Would process: {truck_id} on {date.strftime('%Y-%m-%d')}")
            return
        
        # Process all truck-date combinations
        logging.info("Starting data extraction and file generation...")
        results = []
        
        for i, truck_id in enumerate(truck_ids, 1):
            logging.info(f"Processing truck {i}/{len(truck_ids)}: {truck_id}")
            
            for j, date in enumerate(dates, 1):
                logging.info(f"  Processing date {j}/{len(dates)}: {date.strftime('%Y-%m-%d')}")
                
                try:
                    result = process_truck_date_combination(conn, truck_id, date, output_dir)
                    results.append(result)
                    
                    if result['status'] == 'success':
                        logging.info(f"    SUCCESS: {result['record_count']} records -> CSV: {result['csv_file']}")
                        logging.info(f"                                                   -> JSON: {result['task_file']}")
                    else:
                        logging.warning(f"    {result['status'].upper()}: {result}")
                        
                except Exception as e:
                    logging.error(f"    FAILED: {e}")
                    results.append({
                        'truck_id': truck_id,
                        'date': date.strftime('%Y-%m-%d'),
                        'csv_file': None,
                        'json_file': None,
                        'record_count': 0,
                        'status': 'error',
                        'error': str(e)
                    })
        
        # Generate summary report
        generate_summary_report(results, output_dir)
        
        conn.close()
        logging.info("Database connection closed.")
        logging.info("=== SCRIPT COMPLETED SUCCESSFULLY ===")
        
    except Exception as e:
        logging.error(f"Script execution failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()