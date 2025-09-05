#!/usr/bin/env python3
"""
Massively Parallel Change Point Detection (CPD) Pipeline Orchestrator
TICKET-116: Final Parallelization of CPD Pipeline  
TICKET-130: ProcessPoolExecutor refactor for true parallelism
DataMine V2 - Production CPD Architecture

This script orchestrates parallel CPD processing by dividing the workload into chunks 
(by device_id, date) and processing them concurrently using the UDF-based worker SQL
across multiple database connections with ProcessPoolExecutor.

ProcessPoolExecutor refactor bypasses Python's GIL to achieve true parallelism,
with each worker process creating its own database connection for optimal performance
on multi-core hardware.

Author: Claude Code Development Team  
Date: 2025-09-03
Updated: 2025-09-04 - Refactored to use ProcessPoolExecutor (TICKET-130)
"""

import logging
import psycopg2
import psycopg2.extras
from datetime import datetime
import os
import concurrent.futures
import math
from pathlib import Path

# Configure logging
log_file = Path(__file__).parent / 'cpd_orchestrator.log'
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

def _process_chunk_standalone(chunk_id, chunk_data, worker_sql):
    """Standalone worker function to process a single chunk of device-date combinations.
    
    This function is designed to work with ProcessPoolExecutor, creating its own
    database connection since processes cannot share connection pools.
    """
    import os
    import logging
    
    # Set up logging for this process
    logger = logging.getLogger(__name__)
    
    process_id = os.getpid()
    logger.info(f"Worker {chunk_id:3d} (Process {process_id}) starting with {len(chunk_data)} device-dates")
    
    conn = None
    try:
        # Create database connection for this worker process
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Execute worker SQL with chunk parameter binding - format for PostgreSQL array
        cursor.execute(worker_sql, {'chunk_list': list(chunk_data)})
        result = cursor.fetchone()
        
        conn.commit()
        
        # Log results
        if result:
            events_inserted, devices_processed, earliest, latest = result
            logger.info(f"Worker {chunk_id:3d} completed: {events_inserted} events, {devices_processed} devices")
        else:
            logger.info(f"Worker {chunk_id:3d} completed: No events detected")
        
        return {
            'chunk_id': chunk_id,
            'events_inserted': result[0] if result else 0,
            'devices_processed': result[1] if result else 0,
            'success': True
        }
        
    except Exception as e:
        logger.error(f"Worker {chunk_id:3d} failed: {str(e)}")
        return {
            'chunk_id': chunk_id,
            'events_inserted': 0,
            'devices_processed': 0,
            'success': False,
            'error': str(e)
        }
    finally:
        # Always close connection, even on error
        if conn:
            cursor.close()
            conn.close()

class MassivelyParallelCPDOrchestrator:
    def __init__(self, num_workers=80, chunk_size=8):
        self.num_workers = max(1, num_workers)
        self.chunk_size = chunk_size
        self.worker_sql = self._load_worker_sql()
        
        # ProcessPoolExecutor: Each process creates its own database connection
        # No shared connection pool needed (processes can't share connections)
        logger.info(f"Initialized CPD Orchestrator with {self.num_workers} workers, chunk size {self.chunk_size}")
        logger.info("Using ProcessPoolExecutor - each worker creates its own database connection")

    def _load_worker_sql(self):
        """Load the parameterized worker SQL script."""
        worker_script_path = Path(__file__).parent / "05_cpd_worker.sql"
        
        try:
            with open(worker_script_path, 'r') as f:
                sql_content = f.read()
            logger.info(f"✓ Loaded worker SQL script: {worker_script_path}")
            return sql_content
        except FileNotFoundError:
            logger.error(f"Worker SQL script not found: {worker_script_path}")
            raise

    def _get_database_connection(self):
        """Create a new database connection for orchestrator operations."""
        return psycopg2.connect(**DB_CONFIG)

    def _get_workload(self):
        """Get all unique device_date values to process."""
        conn = self._get_database_connection()
        cursor = conn.cursor()
        
        query = """
        SELECT DISTINCT device_date
        FROM "04_primary_feature_table" 
        WHERE device_date IS NOT NULL
        ORDER BY device_date;
        """
        
        try:
            cursor.execute(query)
            workload = [row[0] for row in cursor.fetchall()]  # Extract strings from tuples
            logger.info(f"✓ Retrieved {len(workload)} device-date combinations to process")
            return workload
        finally:
            cursor.close()
            conn.close()

    def _chunk_workload(self, workload):
        """Divide workload into chunks for parallel processing."""
        chunks = []
        for i in range(0, len(workload), self.chunk_size):
            chunk = workload[i:i + self.chunk_size]
            chunks.append(chunk)
        
        logger.info(f"✓ Divided workload into {len(chunks)} chunks of ~{self.chunk_size} items each")
        return chunks

    def _process_chunk(self, chunk_id, chunk_data):
        """Worker function to process a single chunk of device-date combinations."""
        import os
        process_id = os.getpid()
        logger.info(f"Worker {chunk_id:3d} (Process {process_id}) starting with {len(chunk_data)} device-dates")
        
        conn = None
        try:
            # Create database connection for this worker process
            conn = psycopg2.connect(**DB_CONFIG)
            cursor = conn.cursor()
            
            # Execute worker SQL with chunk parameter binding - format for PostgreSQL array
            cursor.execute(self.worker_sql, {'chunk_list': list(chunk_data)})
            result = cursor.fetchone()
            
            conn.commit()
            
            # Log results
            if result:
                events_inserted, devices_processed, earliest, latest = result
                logger.info(f"Worker {chunk_id:3d} completed: {events_inserted} events, {devices_processed} devices")
            else:
                logger.info(f"Worker {chunk_id:3d} completed: No events detected")
            
            return {
                'chunk_id': chunk_id,
                'events_inserted': result[0] if result else 0,
                'devices_processed': result[1] if result else 0,
                'success': True
            }
            
        except Exception as e:
            logger.error(f"Worker {chunk_id:3d} failed: {str(e)}")
            return {
                'chunk_id': chunk_id,
                'events_inserted': 0,
                'devices_processed': 0,
                'success': False,
                'error': str(e)
            }
        finally:
            # Always close connection, even on error
            if conn:
                cursor.close()
                conn.close()

    def _ensure_candidate_events_table(self):
        """Ensure the candidate events table exists before processing."""
        conn = self._get_database_connection()
        cursor = conn.cursor()
        
        table_sql = """
        CREATE TABLE IF NOT EXISTS "05_candidate_events" (
            event_id SERIAL PRIMARY KEY,
            device_id TEXT NOT NULL,
            timestamp_start TIMESTAMPTZ NOT NULL,
            timestamp_end TIMESTAMPTZ,
            cpd_confidence_score FLOAT,
            detection_method TEXT DEFAULT 'PELT',
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            raw_data_points INTEGER,
            change_point_index INTEGER
        );
        
        CREATE INDEX IF NOT EXISTS idx_candidate_events_device_id ON "05_candidate_events"(device_id);
        CREATE INDEX IF NOT EXISTS idx_candidate_events_timestamp ON "05_candidate_events"(timestamp_start);
        CREATE INDEX IF NOT EXISTS idx_candidate_events_device_timestamp ON "05_candidate_events"(device_id, timestamp_start);
        """
        
        try:
            cursor.execute(table_sql)
            conn.commit()
            logger.info("✓ Candidate events table ready")
        finally:
            cursor.close()
            conn.close()

    def execute_parallel_cpd(self):
        """Execute the complete parallel CPD pipeline."""
        start_time = datetime.now()
        logger.info("🚀 Starting Massively Parallel CPD Pipeline")
        
        try:
            # Step 1: Ensure table exists
            self._ensure_candidate_events_table()
            
            # Step 2: Get workload
            logger.info("📋 Getting workload...")
            workload = self._get_workload()
            
            if not workload:
                logger.warning("⚠️ No data to process")
                return
            
            # Step 3: Chunk workload
            logger.info("🔀 Chunking workload...")
            chunks = self._chunk_workload(workload)
            
            # Step 4: Execute in parallel
            logger.info(f"⚡ Processing {len(chunks)} chunks with {self.num_workers} workers...")
            
            results = []
            with concurrent.futures.ProcessPoolExecutor(max_workers=self.num_workers) as executor:
                # Submit all chunks to process pool
                future_to_chunk = {
                    executor.submit(_process_chunk_standalone, i, chunk, self.worker_sql): i 
                    for i, chunk in enumerate(chunks)
                }
                
                # Collect results as they complete
                for future in concurrent.futures.as_completed(future_to_chunk):
                    chunk_id = future_to_chunk[future]
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        logger.error(f"Chunk {chunk_id} generated exception: {e}")
                        results.append({
                            'chunk_id': chunk_id,
                            'events_inserted': 0,
                            'devices_processed': 0,
                            'success': False,
                            'error': str(e)
                        })
            
            # Step 5: Summary
            duration = datetime.now() - start_time
            total_events = sum(r['events_inserted'] for r in results)
            total_devices = sum(r['devices_processed'] for r in results)
            successful_chunks = sum(1 for r in results if r['success'])
            
            logger.info("🎉 CPD Pipeline Execution Complete!")
            logger.info(f"📊 Results Summary:")
            logger.info(f"   • Duration: {duration}")
            logger.info(f"   • Total Events Detected: {total_events:,}")
            logger.info(f"   • Devices Processed: {total_devices:,}")
            logger.info(f"   • Chunks Processed: {successful_chunks}/{len(chunks)}")
            logger.info(f"   • Average Events/Chunk: {total_events/len(chunks):.1f}")
            
            if successful_chunks < len(chunks):
                failed_chunks = len(chunks) - successful_chunks
                logger.warning(f"⚠️ {failed_chunks} chunks failed - check logs for details")
            
            return {
                'success': True,
                'total_events': total_events,
                'total_devices': total_devices,
                'duration': duration,
                'chunks_processed': successful_chunks,
                'chunks_failed': len(chunks) - successful_chunks
            }
            
        except Exception as e:
            logger.error(f"💥 Pipeline failed: {str(e)}")
            return {'success': False, 'error': str(e)}

def main():
    """Main execution function."""
    logger.info("=" * 80)
    logger.info("DataMine V2 - Massively Parallel CPD Pipeline")
    logger.info("TICKET-116: Final Parallelization Implementation")
    logger.info("=" * 80)
    
    # Initialize orchestrator with optimal parameters for bulk UDF architecture (TICKET-132 Part 3)
    orchestrator = MassivelyParallelCPDOrchestrator(num_workers=24, chunk_size=32)
    
    # Execute pipeline
    result = orchestrator.execute_parallel_cpd()
    
    if result['success']:
        logger.info("✅ Pipeline completed successfully!")
    else:
        logger.error("❌ Pipeline failed!")
        exit(1)

if __name__ == "__main__":
    main()
