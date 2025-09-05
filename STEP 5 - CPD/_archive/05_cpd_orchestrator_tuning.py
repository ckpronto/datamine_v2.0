#!/usr/bin/env python3
"""
CPD Parameter Tuning Orchestrator
TICKET-124: Step 2.2 - Tune Worker and Chunk Size Parameters

This script systematically tests different combinations of num_workers and chunk_size
to find the optimal configuration for maximum throughput in the CPD pipeline.

Target: Find optimal values for 100-core PostgreSQL server processing 96 device-date combinations

Author: Claude Code Development Team
Date: 2025-09-04
"""

import logging
import psycopg2
import psycopg2.extras
import psycopg2.pool
from datetime import datetime
import os
import concurrent.futures
import threading
import math
import time
import json
from pathlib import Path

# Configure logging
log_file = Path(__file__).parent / 'cpd_parameter_tuning.log'
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

class CPDParameterTuner:
    def __init__(self):
        self.worker_sql = self._load_worker_sql()
        self.test_workload = None
        self.results = []
        
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

    def _get_test_workload(self, limit=24):
        """Get a subset of device-date combinations for testing."""
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        query = """
        SELECT DISTINCT device_date
        FROM "04_primary_feature_table" 
        WHERE device_date IS NOT NULL
        ORDER BY device_date
        LIMIT %s;
        """
        
        try:
            cursor.execute(query, (limit,))
            workload = [row[0] for row in cursor.fetchall()]
            logger.info(f"✓ Retrieved {len(workload)} device-date combinations for testing")
            return workload
        finally:
            cursor.close()
            conn.close()

    def _clean_test_table(self):
        """Clean the candidate events table before each test."""
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        try:
            # Delete test results to avoid interference
            cursor.execute('DELETE FROM "05_candidate_events";')
            conn.commit()
            logger.info("✓ Cleaned test table")
        finally:
            cursor.close()
            conn.close()

    def _ensure_test_table(self):
        """Ensure the candidate events table exists for testing."""
        conn = psycopg2.connect(**DB_CONFIG)
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
            logger.info("✓ Test table ready")
        finally:
            cursor.close()
            conn.close()

    def _chunk_workload(self, workload, chunk_size):
        """Divide workload into chunks for parallel processing."""
        chunks = []
        for i in range(0, len(workload), chunk_size):
            chunk = workload[i:i + chunk_size]
            chunks.append(chunk)
        return chunks

    def _process_chunk_timed(self, chunk_id, chunk_data, connection_pool):
        """Worker function with timing for parameter tuning."""
        thread_id = threading.current_thread().ident
        start_time = time.time()
        
        conn = None
        try:
            # Get connection from pool
            conn = connection_pool.getconn()
            cursor = conn.cursor()
            
            # Execute worker SQL with chunk parameter binding
            cursor.execute(self.worker_sql, {'chunk_list': list(chunk_data)})
            result = cursor.fetchone()
            
            conn.commit()
            
            end_time = time.time()
            duration = end_time - start_time
            
            # Return detailed timing results
            return {
                'chunk_id': chunk_id,
                'thread_id': thread_id,
                'duration': duration,
                'chunk_size_actual': len(chunk_data),
                'events_inserted': result[0] if result else 0,
                'devices_processed': result[1] if result else 0,
                'success': True
            }
            
        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time
            logger.error(f"Worker {chunk_id:3d} failed: {str(e)}")
            return {
                'chunk_id': chunk_id,
                'thread_id': thread_id,
                'duration': duration,
                'chunk_size_actual': len(chunk_data),
                'events_inserted': 0,
                'devices_processed': 0,
                'success': False,
                'error': str(e)
            }
        finally:
            if conn:
                cursor.close()
                connection_pool.putconn(conn)

    def run_parameter_test(self, num_workers, chunk_size, workload_limit=24):
        """Run a single parameter combination test."""
        logger.info(f"🧪 Testing: num_workers={num_workers}, chunk_size={chunk_size}")
        
        # Clean test environment
        self._clean_test_table()
        
        # Get test workload
        test_workload = self._get_test_workload(workload_limit)
        
        # Create chunks
        chunks = self._chunk_workload(test_workload, chunk_size)
        num_chunks = len(chunks)
        
        logger.info(f"   Created {num_chunks} chunks from {len(test_workload)} device-dates")
        
        # Initialize connection pool
        pool_size = num_workers + 5
        connection_pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=pool_size,
            **DB_CONFIG
        )
        
        start_time = time.time()
        
        try:
            results = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
                # Submit all chunks
                future_to_chunk = {
                    executor.submit(self._process_chunk_timed, i, chunk, connection_pool): i 
                    for i, chunk in enumerate(chunks)
                }
                
                # Collect results
                for future in concurrent.futures.as_completed(future_to_chunk):
                    chunk_id = future_to_chunk[future]
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        logger.error(f"Chunk {chunk_id} generated exception: {e}")
                        results.append({
                            'chunk_id': chunk_id,
                            'duration': 0,
                            'success': False,
                            'error': str(e)
                        })
            
            end_time = time.time()
            total_duration = end_time - start_time
            
            # Calculate metrics
            successful_chunks = sum(1 for r in results if r['success'])
            total_events = sum(r['events_inserted'] for r in results)
            total_devices = sum(r['devices_processed'] for r in results)
            
            # Throughput calculations
            device_dates_per_second = len(test_workload) / total_duration if total_duration > 0 else 0
            chunks_per_second = num_chunks / total_duration if total_duration > 0 else 0
            
            # Timing analysis
            chunk_durations = [r['duration'] for r in results if r['success']]
            avg_chunk_duration = sum(chunk_durations) / len(chunk_durations) if chunk_durations else 0
            max_chunk_duration = max(chunk_durations) if chunk_durations else 0
            min_chunk_duration = min(chunk_durations) if chunk_durations else 0
            
            test_result = {
                'num_workers': num_workers,
                'chunk_size': chunk_size,
                'workload_size': len(test_workload),
                'num_chunks': num_chunks,
                'total_duration': total_duration,
                'successful_chunks': successful_chunks,
                'failed_chunks': num_chunks - successful_chunks,
                'total_events': total_events,
                'total_devices': total_devices,
                'device_dates_per_second': device_dates_per_second,
                'chunks_per_second': chunks_per_second,
                'avg_chunk_duration': avg_chunk_duration,
                'max_chunk_duration': max_chunk_duration,
                'min_chunk_duration': min_chunk_duration,
                'efficiency_score': device_dates_per_second,  # Primary metric
                'parallelization_effectiveness': total_duration / max_chunk_duration if max_chunk_duration > 0 else 0
            }
            
            logger.info(f"   ✓ Throughput: {device_dates_per_second:.2f} device-dates/sec")
            logger.info(f"   ✓ Total Duration: {total_duration:.2f}s")
            logger.info(f"   ✓ Avg Chunk Duration: {avg_chunk_duration:.2f}s")
            logger.info(f"   ✓ Events Detected: {total_events}")
            
            return test_result
            
        finally:
            connection_pool.closeall()

    def run_comprehensive_tuning(self):
        """Run comprehensive parameter tuning across different configurations."""
        logger.info("🚀 Starting Comprehensive CPD Parameter Tuning")
        logger.info("=" * 80)
        
        # Ensure test environment is ready
        self._ensure_test_table()
        
        # Define parameter ranges based on 100-core system
        worker_configs = [
            32,    # Current baseline
            50,    # 0.5x cores 
            100,   # 1.0x cores
            150,   # 1.5x cores (recommended minimum)
            200,   # 2.0x cores (recommended maximum)
        ]
        
        chunk_sizes = [4, 8, 16, 32]  # As specified in requirements
        
        # Test small subset first to establish baseline (24 device-dates ≈ 25% of full workload)
        workload_limit = 24
        
        logger.info(f"Testing {len(worker_configs)} worker configs × {len(chunk_sizes)} chunk sizes")
        logger.info(f"Using {workload_limit} device-date combinations per test")
        logger.info("-" * 80)
        
        results = []
        test_count = 0
        total_tests = len(worker_configs) * len(chunk_sizes)
        
        for num_workers in worker_configs:
            for chunk_size in chunk_sizes:
                test_count += 1
                logger.info(f"Test {test_count}/{total_tests}: workers={num_workers}, chunk_size={chunk_size}")
                
                try:
                    result = self.run_parameter_test(num_workers, chunk_size, workload_limit)
                    results.append(result)
                    
                    # Brief pause between tests to let system stabilize
                    time.sleep(2)
                    
                except Exception as e:
                    logger.error(f"Test failed: {e}")
                    results.append({
                        'num_workers': num_workers,
                        'chunk_size': chunk_size,
                        'error': str(e),
                        'efficiency_score': 0
                    })
        
        # Analyze results
        logger.info("=" * 80)
        logger.info("📊 TUNING RESULTS ANALYSIS")
        logger.info("=" * 80)
        
        # Sort by efficiency (throughput)
        valid_results = [r for r in results if 'efficiency_score' in r and r['efficiency_score'] > 0]
        valid_results.sort(key=lambda x: x['efficiency_score'], reverse=True)
        
        # Top 5 configurations
        logger.info("🏆 TOP 5 CONFIGURATIONS (by device-dates/second):")
        for i, result in enumerate(valid_results[:5]):
            logger.info(f"{i+1:2d}. workers={result['num_workers']:3d}, chunk_size={result['chunk_size']:2d} "
                       f"→ {result['efficiency_score']:.2f} device-dates/sec "
                       f"({result['total_duration']:.1f}s total)")
        
        # Optimal configuration
        if valid_results:
            optimal = valid_results[0]
            logger.info("=" * 80)
            logger.info("🎯 OPTIMAL CONFIGURATION:")
            logger.info(f"   • num_workers: {optimal['num_workers']}")
            logger.info(f"   • chunk_size: {optimal['chunk_size']}")
            logger.info(f"   • Throughput: {optimal['efficiency_score']:.2f} device-dates/second")
            logger.info(f"   • Estimated Full Pipeline Time: {96/optimal['efficiency_score']:.1f} seconds")
            logger.info("=" * 80)
        
        # Save detailed results
        results_file = Path(__file__).parent / f'cpd_tuning_results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        with open(results_file, 'w') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'system_cores': 100,
                'workload_tested': workload_limit,
                'total_workload': 96,
                'results': results,
                'optimal_config': valid_results[0] if valid_results else None
            }, f, indent=2)
        
        logger.info(f"📁 Detailed results saved to: {results_file}")
        
        return valid_results[0] if valid_results else None

def main():
    """Main execution function."""
    logger.info("=" * 80)
    logger.info("DataMine V2 - CPD Parameter Tuning")
    logger.info("TICKET-124: Step 2.2 - Worker and Chunk Size Optimization")
    logger.info("=" * 80)
    
    tuner = CPDParameterTuner()
    optimal_config = tuner.run_comprehensive_tuning()
    
    if optimal_config:
        logger.info("✅ Parameter tuning completed successfully!")
        logger.info(f"💡 Recommendation: Update main() function with:")
        logger.info(f"    num_workers={optimal_config['num_workers']}")
        logger.info(f"    chunk_size={optimal_config['chunk_size']}")
    else:
        logger.error("❌ Parameter tuning failed!")
        exit(1)

if __name__ == "__main__":
    main()
