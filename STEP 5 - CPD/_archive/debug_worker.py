#!/usr/bin/env python3
"""
Debug script for TICKET-142 to isolate and test the 'process_single_partition'
worker function from the main orchestrator.

This script runs the worker function on a single, hardcoded partition in a
synchronous, non-parallel manner. This allows for direct observation of
any exceptions or silent failures that are hidden by the ProcessPoolExecutor.
"""

import logging
from pathlib import Path
from datetime import datetime

# Important: Import the target function AFTER setting up logging
# to ensure the worker uses this script's log configuration.
from cpd_orchestrator_polars import process_single_partition

# Configure logging for the debug run
log_file = Path(__file__).parent / f'debug_worker_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(processName)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def main():
    """
    Main entry point for the debug script.
    """
    logger.info("=" * 80)
    logger.info("DEBUGGING WORKER FUNCTION: process_single_partition")
    logger.info("=" * 80)
    
    # --- CONFIGURATION ---
    # Select a single, representative partition to test.
    # Using the first partition discovered in the main orchestrator's log.
    test_partition = "lake-605-10-0050_2025-07-30"
    
    logger.info(f"Target Partition: {test_partition}")
    logger.info("Executing function directly (no parallelism)...")
    
    try:
        # Call the function directly
        result = process_single_partition(test_partition)
        
        logger.info("\n" + "=" * 80)
        logger.info("EXECUTION COMPLETED")
        logger.info("=" * 80)
        logger.info(f"Result: {result}")
        
    except Exception as e:
        logger.error("!!! EXECUTION FAILED !!!", exc_info=True)
        logger.error(f"Unhandled exception: {e}")

if __name__ == "__main__":
    main()
