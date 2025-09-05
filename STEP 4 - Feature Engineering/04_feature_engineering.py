#!/usr/bin/env python3
"""
Massively Parallel Feature Engineering Pipeline Orchestrator
AHS Analytics Engine - DataMine V2

This script orchestrates true parallel feature engineering by dividing the workload
into chunks (by device_date) and processing them concurrently across multiple
database connections using a thread pool.

Author: Claude Code Development Team
Date: 2025-09-03
"""

import logging
from sqlalchemy import create_engine, text
from datetime import datetime
import os
import concurrent.futures
import threading
import math

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/home/ck/DataMine/APPLICATION/V2/STEP 4 - Feature Engineering/feature_engineering.log'),
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

class MassivelyParallelFeatureEngineer:
    def __init__(self, num_workers=32):
        self.engine = self._connect_to_database()
        # Ensure the number of workers is at least 1
        self.num_workers = max(1, num_workers)
        logger.info(f"Initialized with {self.num_workers} parallel workers.")

    def _connect_to_database(self):
        """Create optimized database connection for high-performance operations."""
        connection_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        
        # Create engine with performance optimizations
        engine = create_engine(
            connection_string,
            pool_size=20,           # Support multiple concurrent connections
            max_overflow=30,        # Allow connection overflow
            pool_pre_ping=True,     # Verify connections before use
            pool_recycle=3600,      # Recycle connections hourly
            echo=False              # Disable SQL echoing for performance
        )
        
        logger.info("Connected to database with high-performance configuration")
        return engine

    def _load_sql_script(self, filename):
        """Load SQL script from file."""
        script_path = os.path.join(
            '/home/ck/DataMine/APPLICATION/V2/STEP 4 - Feature Engineering',
            filename
        )
        
        try:
            with open(script_path, 'r') as f:
                sql_content = f.read()
            logger.info(f"✓ Loaded SQL script: {filename}")
            return sql_content
        except FileNotFoundError:
            logger.error(f"SQL script not found: {script_path}")
            raise
        except Exception as e:
            logger.error(f"Error loading SQL script: {e}")
            raise

    def get_device_date_chunks(self):
        """Fetches all unique device_date pairs and divides them into chunks for workers."""
        logger.info("Fetching all device-date chunks to be processed...")
        query = text('SELECT DISTINCT device_date FROM "02_raw_telemetry_transformed" ORDER BY device_date')
        
        with self.engine.connect() as conn:
            results = conn.execute(query).fetchall()
            all_chunks = [row[0] for row in results]
        
        if not all_chunks:
            raise ValueError("No device-date chunks found in the source table.")

        # Divide the list of chunks into a number of sub-lists equal to the number of workers
        chunk_size = math.ceil(len(all_chunks) / self.num_workers)
        worker_chunks = [all_chunks[i:i + chunk_size] for i in range(0, len(all_chunks), chunk_size)]
        
        logger.info(f"✓ Divided {len(all_chunks)} total chunks into {len(worker_chunks)} batches for parallel processing.")
        return worker_chunks

    @staticmethod
    def parallel_worker_process_chunk(chunk_list):
        """
        This is the core worker function. It runs in a separate thread,
        connects to the DB, and processes its assigned list of device_date chunks.
        Now inserts directly into the shared staging table.
        """
        worker_id = threading.get_ident()
        logger.info(f"[Worker-{worker_id}] Started, assigned {len(chunk_list)} chunks.")

        # Load the SQL script for the worker
        script_path = os.path.join(
            '/home/ck/DataMine/APPLICATION/V2/STEP 4 - Feature Engineering',
            'worker_script.sql'
        )
        try:
            with open(script_path, 'r') as f:
                worker_sql = f.read()
        except Exception as e:
            logger.error(f"[Worker-{worker_id}] Failed to load worker_script.sql: {e}")
            raise

        # Use a new connection from the pool for each worker
        engine = create_engine(f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
        
        with engine.connect() as conn:
            try:
                # Execute the parameterized query for the assigned chunks
                conn.execute(text(worker_sql), {'chunk_list': tuple(chunk_list)})
                conn.commit()
                logger.info(f"✓ [Worker-{worker_id}] Successfully processed {len(chunk_list)} chunks and inserted into staging table.")
                return True
            except Exception as e:
                logger.error(f"❌ [Worker-{worker_id}] Failed during SQL execution: {e}")
                conn.rollback()
                raise

    def _create_staging_table(self):
        """Create the shared staging table that all workers will insert into."""
        logger.info("Creating shared staging table...")
        
        create_staging_sql = """
        DROP TABLE IF EXISTS feature_engineering_staging;
        
        CREATE TABLE feature_engineering_staging (
            timestamp TIMESTAMPTZ NOT NULL,
            ingested_at TIMESTAMPTZ,
            raw_event_hash_id TEXT,
            device_id TEXT,
            device_date TEXT,
            system_engaged BOOLEAN,
            parking_brake_applied BOOLEAN,
            current_position GEOGRAPHY,
            current_speed DOUBLE PRECISION,
            load_weight DOUBLE PRECISION,
            state TEXT,
            software_state TEXT,
            prndl TEXT,
            extras JSONB,
            location_type TEXT,
            is_stationary BOOLEAN,
            altitude DOUBLE PRECISION,
            altitude_rate_of_change DOUBLE PRECISION,
            speed_rolling_avg_5s DOUBLE PRECISION,
            load_weight_smoothed DOUBLE PRECISION,
            load_weight_rate_of_change DOUBLE PRECISION,
            has_reliable_payload BOOLEAN,
            time_in_stationary_state DOUBLE PRECISION
        );
        """
        
        with self.engine.connect() as conn:
            conn.execute(text(create_staging_sql))
            conn.commit()
        
        logger.info("✓ Staging table created successfully.")

    def _cleanup_staging_table(self):
        """Drop the staging table after final assembly."""
        logger.info("Cleaning up staging table...")
        
        with self.engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS feature_engineering_staging;"))
            conn.commit()
        
        logger.info("✓ Staging table cleaned up successfully.")

    def run_pipeline(self):
        """Executes the entire parallel pipeline with staging table architecture."""
        logger.info("=" * 70)
        logger.info("STARTING MASSIVELY PARALLEL FEATURE ENGINEERING")
        logger.info("=" * 70)
        start_time = datetime.now()

        try:
            # STEP 1: Create the shared staging table
            self._create_staging_table()
            
            # STEP 2: Get chunks
            device_date_chunks = self.get_device_date_chunks()
            
            # STEP 3: Process chunks in parallel
            logger.info("🚀 Launching parallel workers...")
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.num_workers) as executor:
                # map() is used to run the worker function over the list of chunks
                future_results = list(executor.map(self.parallel_worker_process_chunk, device_date_chunks))
                
                # Verify all workers completed successfully
                if not all(future_results):
                    raise RuntimeError("One or more workers failed to complete successfully.")

            logger.info("✓ All parallel workers completed successfully.")

            # STEP 4: Assemble the final table
            logger.info("🤝 Assembling final table from staging data...")
            assembly_sql = self._load_sql_script('final_assembly.sql')

            with self.engine.connect() as conn:
                conn.execute(text(assembly_sql))
                conn.commit()
            
            logger.info("✓ Final feature table assembled successfully.")

            # STEP 5: Cleanup staging table
            self._cleanup_staging_table()

            duration = (datetime.now() - start_time).total_seconds()
            logger.info("=" * 70)
            logger.info(f"🎉 PIPELINE COMPLETED SUCCESSFULLY IN {duration:.1f} SECONDS")
            logger.info("=" * 70)
            
        except Exception as e:
            logger.error(f"❌ Pipeline failed: {e}")
            # Ensure cleanup even if pipeline fails
            try:
                self._cleanup_staging_table()
            except:
                pass  # Staging table may not exist
            raise

def main():
    try:
        # You can adjust the number of workers here
        engineer = MassivelyParallelFeatureEngineer(num_workers=32)
        engineer.run_pipeline()
    except Exception as e:
        logger.error(f"❌ Pipeline execution failed: {e}")
        exit(1)

if __name__ == "__main__":
    main()