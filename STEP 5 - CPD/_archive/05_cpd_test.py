#!/usr/bin/env python3
"""
CPD Orchestrator Test - Focused execution for validation
TICKET-116: Testing parallel CPD with smaller dataset
"""

import logging
import psycopg2
from datetime import datetime
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'datamine_v2_db',
    'user': 'ahs_user',
    'password': 'ahs_password'
}

def test_cpd_worker():
    """Test CPD worker with a single small chunk"""
    logger.info("Testing CPD worker with focused dataset...")
    
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # Get a small test chunk - single device, single date
    test_query = """
    SELECT DISTINCT device_id, DATE(timestamp) as processing_date
    FROM "04_primary_feature_table" 
    WHERE load_weight_rate_of_change IS NOT NULL
    AND device_id = 'lake-605-8-0896'
    AND DATE(timestamp) = '2025-08-08'
    LIMIT 1;
    """
    
    cursor.execute(test_query)
    test_chunk = cursor.fetchall()
    logger.info(f"Test chunk: {test_chunk}")
    
    if not test_chunk:
        logger.error("No test data found")
        return
    
    # Load worker SQL
    worker_script_path = Path(__file__).parent / "05_cpd_worker.sql"
    with open(worker_script_path, 'r') as f:
        worker_sql = f.read()
    
    # Execute worker with test chunk
    try:
        cursor.execute(worker_sql, {'chunk_list': tuple(test_chunk)})
        result = cursor.fetchone()
        conn.commit()
        
        if result:
            events_inserted, devices_processed, earliest, latest = result
            logger.info(f"✓ Test successful: {events_inserted} events, {devices_processed} devices")
            logger.info(f"  Time range: {earliest} to {latest}")
        else:
            logger.info("✓ Test completed - No events detected")
            
    except Exception as e:
        logger.error(f"Test failed: {e}")
    finally:
        cursor.close()
        conn.close()

def check_results():
    """Check current results in candidate events table"""
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            COUNT(*) as total_events,
            COUNT(DISTINCT device_id) as devices,
            MIN(timestamp_start) as earliest,
            MAX(timestamp_start) as latest,
            AVG(cpd_confidence_score) as avg_confidence
        FROM "05_candidate_events"
        WHERE created_at >= CURRENT_TIMESTAMP - INTERVAL '10 minutes'
    """)
    
    result = cursor.fetchone()
    if result and result[0] > 0:
        total, devices, earliest, latest, confidence = result
        logger.info(f"📊 Recent results: {total} events from {devices} devices")
        logger.info(f"   Time range: {earliest} to {latest}")
        logger.info(f"   Avg confidence: {confidence:.3f}")
    else:
        logger.info("No recent results found")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    logger.info("=" * 60)
    logger.info("CPD Pipeline Test")
    logger.info("=" * 60)
    
    # Check existing results
    check_results()
    
    # Run focused test
    test_cpd_worker()
    
    # Check results after test
    logger.info("\nAfter test execution:")
    check_results()
    
    logger.info("✓ Test complete")