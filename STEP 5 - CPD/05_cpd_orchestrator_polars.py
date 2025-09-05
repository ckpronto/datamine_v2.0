#!/usr/bin/env python3
"""
TICKET-139 Part 2: Massively Parallel CPD Orchestrator - Production Scale

This script implements a production-scale, massively parallel Change Point Detection
orchestrator that processes the entire telemetry dataset (96 partitions, 8.8M+ records)
simultaneously across all available CPU cores.

Key Performance Features:
- Parquet-based processing with zero database I/O during computation
- ProcessPoolExecutor utilizing all CPU cores (100 available)
- Per-partition workers with isolated Polars processing
- Validated 5-second downsampling solving O(n²) complexity bottleneck
- Sub-hour processing target for complete dataset
- Comprehensive error handling and progress tracking

Architecture: Parquet Partitions -> Parallel Workers -> PELT Algorithm -> Aggregated Results

Author: Claude Code Development Team - Lead Data Engineer (Opus)
Date: 2025-09-04
Tickets: TICKET-139 (Part 2 - Massively Parallel Orchestrator)
"""

import logging
import polars as pl
import ruptures as rpt
import numpy as np
import pandas as pd
import time
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
import psutil
import json
from typing import List, Tuple, Dict, Any, Optional
import os
import sys
import shutil

def get_logger(name: str, log_file: Path):
    """Get a logger configured for file and stream output."""
    logger = logging.getLogger(name)
    if not logger.handlers:  # Avoid adding handlers multiple times
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(processName)s - %(levelname)s - %(message)s')
        
        # File handler
        fh = logging.FileHandler(log_file)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        
        # Stream handler
        sh = logging.StreamHandler()
        sh.setFormatter(formatter)
        logger.addHandler(sh)
        
    return logger

# Production configuration
PARQUET_BASE_PATH = Path(__file__).parent / "features.parquet"
OUTPUT_FILE = Path(__file__).parent / "05_candidate_events_final.csv"
DETECTION_METHOD = "polars_parallel_v2"

# Performance parameters (validated from single-test)
DOWNSAMPLING_INTERVAL = "5s"  # 5-second rolling mean
PELT_PENALTY = 0.05          # Optimal penalty achieving 100% recall
PELT_MIN_SIZE = 10           # Minimum segment length
PELT_JUMP = 1               # Full resolution analysis
BATCH_SIZE = 1000           # For result aggregation

def discover_partition_workload(logger: logging.Logger) -> List[str]:
    """
    Discover all available device_date partitions in the Parquet directory.
    
    Returns:
        List[str]: List of device_date partition names
    """
    logger.info("=" * 80)
    logger.info("WORKLOAD DISCOVERY PHASE")
    logger.info("=" * 80)
    logger.info(f"Scanning Parquet directory: {PARQUET_BASE_PATH}")
    
    try:
        if not PARQUET_BASE_PATH.exists():
            raise FileNotFoundError(f"Parquet directory not found: {PARQUET_BASE_PATH}")
        
        # Discover all partition directories
        partitions = []
        for partition_dir in PARQUET_BASE_PATH.iterdir():
            if partition_dir.is_dir() and partition_dir.name.startswith("device_date="):
                device_date = partition_dir.name.replace("device_date=", "")
                partitions.append(device_date)
        
        partitions.sort()  # Consistent ordering for logging
        
        logger.info(f"Discovered {len(partitions)} device_date partitions")
        logger.info(f"Partition range: {partitions[0]} to {partitions[-1]}")
        logger.info(f"Sample partitions: {partitions[:5]}")
        
        if len(partitions) == 0:
            raise ValueError("No valid partitions found in Parquet directory")
        
        # Log workload estimation
        logger.info("\n--- Workload Estimation ---")
        logger.info(f"Total partitions: {len(partitions)}")
        logger.info(f"Estimated per-partition time: 2-10 seconds (based on single-test)")
        logger.info(f"Sequential processing time: {len(partitions) * 5 / 60:.1f} minutes")
        logger.info(f"Parallel processing target: <60 minutes")
        
        return partitions
        
    except Exception as e:
        logger.error(f"Workload discovery failed: {e}")
        raise

def analyze_system_resources(logger):
    """
    Analyze available system resources for optimal parallel configuration.
    
    Returns:
        Dict[str, Any]: System resource analysis
    """
    
    try:
        # CPU analysis
        cpu_count = mp.cpu_count()
        cpu_logical = psutil.cpu_count(logical=True)
        cpu_physical = psutil.cpu_count(logical=False)
        
        # Memory analysis
        memory = psutil.virtual_memory()
        memory_gb = memory.total / (1024**3)
        memory_available_gb = memory.available / (1024**3)
        
        # Estimate optimal worker count (leave some cores for system)
        # Optimized for 100-core system: use ~90% of available cores for maximum throughput
        optimal_workers = min(cpu_count - 8, 90)  # Leave 8 cores for system stability
        
        resource_analysis = {
            'cpu_logical_cores': cpu_logical,
            'cpu_physical_cores': cpu_physical,
            'memory_total_gb': round(memory_gb, 1),
            'memory_available_gb': round(memory_available_gb, 1),
            'optimal_worker_count': optimal_workers,
            'memory_per_worker_gb': round(memory_available_gb / optimal_workers, 1)
        }
        
        logger.info(f"CPU cores (logical): {cpu_logical}")
        logger.info(f"CPU cores (physical): {cpu_physical}")
        logger.info(f"Total memory: {memory_gb:.1f} GB")
        logger.info(f"Available memory: {memory_available_gb:.1f} GB")
        logger.info(f"Optimal worker count: {optimal_workers}")
        logger.info(f"Memory per worker: {memory_available_gb / optimal_workers:.1f} GB")
        
        return resource_analysis
        
    except Exception as e:
        logger.error(f"System resource analysis failed: {e}")
        raise

def process_single_partition(device_date: str, log_file: Path) -> Tuple[str, float, Dict[str, Any]]:
    """
    Process a single device_date partition using Polars and ruptures PELT.
    
    This function is designed to run in a separate process for parallel execution.
    TICKET-141: Now saves results to Parquet file instead of returning in memory.
    TICKET-142: Uses polars-lts-cpu for broad CPU compatibility, removing the need
                for POLARS_SKIP_CPU_CHECK.
    
    Args:
        device_date: Device-date string (e.g., 'lake-605-10-0050_2025-07-30')
        log_file: Path to the log file for worker-safe logging.
        
    Returns:
        Tuple: (device_date, processing_time, metrics)
    """
    start_time = time.time()
    logger = get_logger(__name__, log_file)
    
    try:
        # Build partition path
        partition_path = PARQUET_BASE_PATH / f"device_date={device_date}"
        
        if not partition_path.exists():
            raise FileNotFoundError(f"Partition path not found: {partition_path}")
        
        # Load Parquet data directly with Polars - optimized for performance
        # TICKET-141: Include multi-signal columns for fleet coverage
        df = (
            pl.scan_parquet(partition_path / "*.parquet")
            .select([
                "timestamp", 
                "load_weight_rate_of_change", 
                "speed_rolling_avg_5s",
                "altitude_rate_of_change", 
                "has_reliable_payload",
                "raw_event_hash_id"
            ])
            .collect(streaming=True)
        )
        
        # TICKET-141: Check has_reliable_payload flag for multi-signal logic  
        # TICKET-146: Implement robust boolean parsing (Root Cause Fix)
        has_reliable_payload_raw = df['has_reliable_payload'][0] if len(df) > 0 else "f"
        has_reliable_payload = str(has_reliable_payload_raw).lower() in ['true', 't', '1']
        
        # Validate minimum data
        if len(df) < 20:
            logger.warning(f"Insufficient data points ({len(df)}) for {device_date}")
            return device_date, time.time() - start_time, {
                'original_records': len(df),
                'clean_records': len(df),
                'downsampled_records': 0,
                'change_points': 0,
                'status': 'insufficient_data'
            }
        
        # Initialize combined change points list
        all_change_point_indices = []
        
        if has_reliable_payload:
            # Path 1: Reliable payload data - SIGNAL-SPECIFIC PREPROCESSING
            # TICKET-144: Move payload-specific data cleaning into this block
            df_clean = df.filter(pl.col("load_weight_rate_of_change").is_not_null())
            
            if len(df_clean) < 20:
                logger.warning(f"Insufficient load weight data ({len(df_clean)}) for {device_date}")
                return device_date, time.time() - start_time, {
                    'original_records': len(df),
                    'clean_records': len(df_clean),
                    'downsampled_records': 0,
                    'change_points': 0,
                    'status': 'insufficient_payload_data'
                }
            
            # Convert timestamp to datetime for payload processing
            df_timestamped = df_clean.with_columns(
                pl.col("timestamp").str.to_datetime(strict=False, time_zone="UTC")
            ).filter(pl.col("timestamp").is_not_null()).sort("timestamp")
            
            # TICKET-146: Final validation checks before downsampling
            if len(df_timestamped) > 0:
                timestamps = df_timestamped['timestamp'].to_numpy()
                signal_values = df_timestamped['load_weight_rate_of_change'].to_numpy()
                
                # Check timestamp sorting
                is_sorted = np.all(timestamps[:-1] <= timestamps[1:])
                if not is_sorted:
                    logger.error(f"Timestamps not sorted in {device_date}")
                    return device_date, time.time() - start_time, {
                        'original_records': len(df), 'clean_records': len(df_clean),
                        'downsampled_records': 0, 'change_points': 0,
                        'status': 'timestamp_error'
                    }
                
                # Check signal finite values
                if not np.all(np.isfinite(signal_values)):
                    logger.warning(f"Non-finite signal values in {device_date}")
                    # Continue processing - ruptures can handle some non-finite values
            
            # Payload-specific downsampling and CPD
            df_downsampled = (
                df_timestamped
                .group_by_dynamic(
                    "timestamp", 
                    every=DOWNSAMPLING_INTERVAL, 
                    period=DOWNSAMPLING_INTERVAL,
                    closed="left"
                )
                .agg([
                    pl.col("load_weight_rate_of_change").mean().alias("signal_values"),
                    pl.col("raw_event_hash_id").first().alias("raw_event_hash_id")
                ])
                .drop_nulls()
            )
            
            if len(df_downsampled) >= 10:
                signal_values = df_downsampled['signal_values'].to_numpy()
                signal_array = signal_values.reshape(-1, 1)
                
                algo = rpt.Pelt(model="l2", min_size=PELT_MIN_SIZE, jump=PELT_JUMP)
                algo.fit(signal_array)
                change_point_indices = algo.predict(pen=PELT_PENALTY)
                
                if len(change_point_indices) > 0 and change_point_indices[-1] == len(signal_values):
                    change_point_indices = change_point_indices[:-1]
                    
                all_change_point_indices.extend(change_point_indices)
        
        else:
            # Path 2: Unreliable payload data - KINEMATIC-SPECIFIC PREPROCESSING
            # TICKET-144: Move kinematic-specific data cleaning into this block
            df_clean = df.filter(
                pl.col("speed_rolling_avg_5s").is_not_null() &
                pl.col("altitude_rate_of_change").is_not_null()
            )
            
            if len(df_clean) < 20:
                logger.warning(f"Insufficient kinematic data ({len(df_clean)}) for {device_date}")
                return device_date, time.time() - start_time, {
                    'original_records': len(df),
                    'clean_records': len(df_clean),
                    'downsampled_records': 0,
                    'change_points': 0,
                    'status': 'insufficient_kinematic_data'
                }
            
            # Convert timestamp to datetime for kinematic processing
            df_timestamped = df_clean.with_columns(
                pl.col("timestamp").str.to_datetime(strict=False, time_zone="UTC")
            ).filter(pl.col("timestamp").is_not_null()).sort("timestamp")
            
            # TICKET-146: Add final validation checks from debug script
            timestamps = df_timestamped['timestamp'].to_numpy()
            speed_values_check = df_timestamped['speed_rolling_avg_5s'].to_numpy()
            altitude_values_check = df_timestamped['altitude_rate_of_change'].to_numpy()
            
            # Validate timestamp sorting
            is_sorted = np.all(timestamps[:-1] <= timestamps[1:])
            if not is_sorted:
                logger.error(f"TICKET-146: Timestamps not sorted for {device_date}")
                return device_date, time.time() - start_time, {
                    'original_records': len(df),
                    'clean_records': len(df_clean),
                    'downsampled_records': 0,
                    'change_points': 0,
                    'status': 'timestamp_sort_error'
                }
            
            # Validate signal finite values
            speed_is_finite = np.all(np.isfinite(speed_values_check))
            altitude_is_finite = np.all(np.isfinite(altitude_values_check))
            
            if not speed_is_finite or not altitude_is_finite:
                logger.error(f"TICKET-146: Non-finite kinematic values for {device_date} - Speed: {speed_is_finite}, Altitude: {altitude_is_finite}")
                return device_date, time.time() - start_time, {
                    'original_records': len(df),
                    'clean_records': len(df_clean),
                    'downsampled_records': 0,
                    'change_points': 0,
                    'status': 'non_finite_signals'
                }
            
            # Create single downsampled DataFrame with both signals
            df_downsampled = (
                df_timestamped
                .group_by_dynamic(
                    "timestamp", 
                    every=DOWNSAMPLING_INTERVAL, 
                    period=DOWNSAMPLING_INTERVAL,
                    closed="left"
                )
                .agg([
                    pl.col("speed_rolling_avg_5s").mean(),
                    pl.col("altitude_rate_of_change").mean(),
                    pl.col("raw_event_hash_id").first()
                ])
                .drop_nulls()
            )
            
            # First CPD Pass: Speed (for stop/start detection)
            speed_signal = df_downsampled.filter(pl.col("speed_rolling_avg_5s").is_not_null())
            if len(speed_signal) >= 10:
                speed_values = speed_signal['speed_rolling_avg_5s'].to_numpy()
                speed_array = speed_values.reshape(-1, 1)
                
                algo_speed = rpt.Pelt(model="l2", min_size=PELT_MIN_SIZE, jump=PELT_JUMP)
                algo_speed.fit(speed_array)
                speed_change_points = algo_speed.predict(pen=PELT_PENALTY)
                
                if len(speed_change_points) > 0 and speed_change_points[-1] == len(speed_values):
                    speed_change_points = speed_change_points[:-1]
                    
                all_change_point_indices.extend(speed_change_points)
            
            # Second CPD Pass: Altitude (for dump detection)  
            altitude_signal = df_downsampled.filter(pl.col("altitude_rate_of_change").is_not_null())
            if len(altitude_signal) >= 10:
                altitude_values = altitude_signal['altitude_rate_of_change'].to_numpy()
                altitude_array = altitude_values.reshape(-1, 1)
                
                algo_altitude = rpt.Pelt(model="l2", min_size=PELT_MIN_SIZE, jump=PELT_JUMP)
                algo_altitude.fit(altitude_array)
                altitude_change_points = algo_altitude.predict(pen=PELT_PENALTY)
                
                if len(altitude_change_points) > 0 and altitude_change_points[-1] == len(altitude_values):
                    altitude_change_points = altitude_change_points[:-1]
                    
                all_change_point_indices.extend(altitude_change_points)
        
        # Validate final downsampled data
        if len(df_downsampled) < 10:
            logger.warning(f"Insufficient downsampled data ({len(df_downsampled)}) for {device_date}")
            return device_date, time.time() - start_time, {
                'original_records': len(df),
                'clean_records': len(df),
                'downsampled_records': len(df_downsampled),
                'change_points': 0,
                'status': 'insufficient_downsampled_data'
            }
        
        # Prepare arrays for timestamp/hash extraction
        timestamp_strings = df_downsampled['timestamp'].dt.strftime("%Y-%m-%d %H:%M:%S").to_numpy()
        hash_ids = df_downsampled['raw_event_hash_id'].to_numpy()
        
        # Remove duplicates and sort change point indices
        all_change_point_indices = sorted(list(set(all_change_point_indices)))
        
        # Convert indices to timestamps and create Polars DataFrame
        change_points_data = []
        device_id = device_date.rsplit('_', 1)[0]  # Extract device_id from device_date
        
        for idx in all_change_point_indices:
            if 0 <= idx < len(timestamp_strings):
                timestamp_str = timestamp_strings[idx]
                timestamp_dt = pd.to_datetime(timestamp_str)
                hash_id = hash_ids[idx]
                
                # Build row data for Polars DataFrame
                change_points_data.append({
                    'device_id': device_id,
                    'timestamp_start': timestamp_dt.isoformat(),
                    'raw_event_hash_id': hash_id
                })
        
        # Create Polars DataFrame from change points
        if change_points_data:
            change_points_df = pl.DataFrame(change_points_data)
            # TICKET-141: Simplified schema - only essential columns for ML stage
            
            # TICKET-141: Save to temp_results directory as Parquet file
            temp_results_dir = Path(__file__).parent / "temp_results"
            temp_results_dir.mkdir(exist_ok=True)
            result_file = temp_results_dir / f"results_{device_date}.parquet"
            change_points_df.write_parquet(result_file)
            
            change_points_count = len(change_points_data)
        else:
            change_points_count = 0
        
        processing_time = time.time() - start_time
        
        # Build metrics for monitoring
        metrics = {
            'original_records': len(df),
            'clean_records': len(df),
            'downsampled_records': len(df_downsampled),
            'change_points': change_points_count,
            'processing_time_seconds': round(processing_time, 3),
            'data_reduction_ratio': round(len(df_downsampled) / len(df), 3),
            'status': 'success'
        }
        
        return device_date, processing_time, metrics
        
    except Exception as e:
        processing_time = time.time() - start_time
        logger.error(f"Processing failed for {device_date}: {e}")
        
        # Return error metrics
        error_metrics = {
            'original_records': 0,
            'clean_records': 0,
            'downsampled_records': 0,
            'change_points': 0,
            'processing_time_seconds': round(processing_time, 3),
            'error_message': str(e),
            'status': 'error'
        }
        
        return device_date, processing_time, error_metrics

def execute_parallel_cpd(partitions: List[str], max_workers: int, log_file: Path) -> Dict[str, Any]:
    """
    Execute Change Point Detection across all partitions in parallel.
    
    TICKET-141: Modified to use file-based results instead of memory accumulation.
    
    Args:
        partitions: List of device_date strings to process
        max_workers: Maximum number of parallel worker processes
        log_file: Path to the log file for worker-safe logging.
        
    Returns:
        Dict[str, Any]: Performance metrics only (results saved to temp files)
    """
    logger = get_logger(__name__, log_file)
    logger.info("\n" + "=" * 80)
    logger.info("PARALLEL CPD EXECUTION PHASE")
    logger.info("=" * 80)
    logger.info(f"Partitions to process: {len(partitions)}")
    logger.info(f"Worker processes: {max_workers}")
    logger.info(f"Target architecture: ProcessPoolExecutor with isolated Polars processing")
    
    total_start_time = time.time()
    partition_metrics = {}
    completed_count = 0
    failed_count = 0
    total_change_points_count = 0
    
    try:
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            # Submit all jobs
            logger.info(f"Submitting {len(partitions)} parallel jobs...")
            future_to_partition = {
                executor.submit(process_single_partition, device_date, log_file): device_date
                for device_date in partitions
            }

            
            # Process results as they complete
            logger.info("Processing results as they complete...")
            
            for future in as_completed(future_to_partition):
                device_date = future_to_partition[future]
                
                try:
                    # Get result from completed worker (no change_points in memory)
                    device_date_result, processing_time, metrics = future.result()
                    
                    # Store metrics and accumulate counts
                    partition_metrics[device_date] = metrics
                    total_change_points_count += metrics.get('change_points', 0)
                    
                    completed_count += 1
                    
                    # Log progress every 20 completed partitions for reduced overhead
                    if completed_count % 20 == 0 or completed_count == len(partitions):
                        progress_pct = (completed_count / len(partitions)) * 100
                        elapsed_time = time.time() - total_start_time
                        avg_time_per_partition = elapsed_time / completed_count
                        est_remaining = (len(partitions) - completed_count) * avg_time_per_partition
                        
                        logger.info(f"Progress: {completed_count}/{len(partitions)} ({progress_pct:.1f}%) | "
                                  f"Elapsed: {elapsed_time/60:.1f}m | "
                                  f"ETA: {est_remaining/60:.1f}m | "
                                  f"Latest: {device_date} -> {metrics.get('change_points', 0)} events")
                    
                except Exception as e:
                    failed_count += 1
                    logger.error(f"Worker failed for {device_date}: {e}")
                    
                    # Store error metrics
                    partition_metrics[device_date] = {
                        'status': 'worker_error',
                        'error_message': str(e),
                        'change_points': 0
                    }
        
        total_processing_time = time.time() - total_start_time
        
        # TICKET-141: Results are now in temp files, no DataFrame construction here
        successful_partitions = completed_count - failed_count
        
        performance_metrics = {
            'total_partitions_processed': len(partitions),
            'successful_partitions': successful_partitions,
            'failed_partitions': failed_count,
            'total_change_points_detected': total_change_points_count,
            'total_processing_time_seconds': round(total_processing_time, 2),
            'total_processing_time_minutes': round(total_processing_time / 60, 2),
            'average_time_per_partition': round(total_processing_time / len(partitions), 3),
            'change_points_per_partition_avg': round(total_change_points_count / max(successful_partitions, 1), 1),
            'max_workers_used': max_workers,
            'parallel_efficiency': round(successful_partitions / len(partitions), 3),
            'detection_method': DETECTION_METHOD,
            'pelt_parameters': {
                'penalty': PELT_PENALTY,
                'min_size': PELT_MIN_SIZE,
                'jump': PELT_JUMP,
                'downsampling_interval': DOWNSAMPLING_INTERVAL
            }
        }
        
        logger.info("\n" + "=" * 80)
        logger.info("PARALLEL PROCESSING COMPLETED")
        logger.info("=" * 80)
        logger.info(f"Total time: {total_processing_time/60:.2f} minutes")
        logger.info(f"Successful partitions: {successful_partitions}/{len(partitions)}")
        logger.info(f"Total change points: {total_change_points_count}")
        logger.info(f"Average events per partition: {total_change_points_count / max(successful_partitions, 1):.1f}")
        logger.info(f"Processing rate: {len(partitions) / (total_processing_time/60):.1f} partitions/minute")
        
        return performance_metrics
        
    except Exception as e:
        logger.error(f"Parallel execution failed: {e}")
        raise

def export_final_results(results_df: pl.DataFrame, performance_metrics: Dict[str, Any], logger: logging.Logger) -> Path:
    """
    Export final aggregated results to CSV for database loading.
    
    Args:
        results_df: Polars DataFrame with all change point results
        performance_metrics: Performance metrics from parallel processing
        
    Returns:
        Path: Path to exported CSV file
    """
    logger.info("\n--- Final Results Export ---")
    
    try:
        # Export main results to CSV
        logger.info(f"Exporting {len(results_df)} change points to {OUTPUT_FILE}")
        
        # Ensure output directory exists
        OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
        
        # Export with proper CSV formatting for database import
        results_df.write_csv(OUTPUT_FILE, separator=',', include_header=True)
        
        # Export performance metrics as JSON
        metrics_file = OUTPUT_FILE.with_suffix('.metrics.json')
        with open(metrics_file, 'w') as f:
            json.dump(performance_metrics, f, indent=2)
        
        # Create summary statistics
        if len(results_df) > 0:
            device_counts = results_df.group_by('device_id').count().sort('count', descending=True)
            logger.info(f"Results by device:")
            for row in device_counts.rows():
                logger.info(f"  {row[0]}: {row[1]} events")
        
        file_size_mb = OUTPUT_FILE.stat().st_size / (1024 * 1024)
        logger.info(f"Exported file size: {file_size_mb:.2f} MB")
        logger.info(f"Main results: {OUTPUT_FILE}")
        logger.info(f"Metrics file: {metrics_file}")
        
        return OUTPUT_FILE
        
    except Exception as e:
        logger.error(f"Results export failed: {e}")
        raise

def validate_parquet_environment(logger):
    """
    Validate that the Parquet data environment is ready for processing.
    """
    logger.info("--- Environment Validation ---")
    
    try:
        # Check Parquet base directory exists
        if not PARQUET_BASE_PATH.exists():
            raise FileNotFoundError(f"Parquet base directory not found: {PARQUET_BASE_PATH}")
        
        # Test load a small partition to validate schema
        test_partitions = list(PARQUET_BASE_PATH.glob("device_date=*/"))
        if not test_partitions:
            raise ValueError("No partition directories found in Parquet base path")
        
        test_partition = test_partitions[0]
        test_df = pl.scan_parquet(test_partition / "*.parquet").limit(5).collect()
        
        required_columns = ['timestamp', 'load_weight_rate_of_change', 'raw_event_hash_id']
        missing_columns = [col for col in required_columns if col not in test_df.columns]
        if missing_columns:
            raise ValueError(f"Required columns missing from Parquet schema: {missing_columns}")
        
        logger.info("Environment validation passed")
        logger.info(f"Schema validated with {len(test_df.columns)} columns")
        logger.info(f"Test partition: {test_partition.name}")
        
    except Exception as e:
        logger.error(f"Environment validation failed: {e}")
        raise

def main():
    """
    Main orchestrator entry point for massively parallel CPD processing.
    """
    # Centralized log file definition
    log_file = Path(__file__).parent / f'cpd_orchestrator_polars_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    logger = get_logger(__name__, log_file)

    logger.info("=" * 80)
    logger.info("MASSIVELY PARALLEL CPD ORCHESTRATOR - PRODUCTION SCALE")
    logger.info("=" * 80)
    logger.info(f"Target: Process entire telemetry dataset with sub-hour performance")
    logger.info(f"Architecture: Parquet -> Parallel Workers -> PELT -> Aggregated Results")
    logger.info(f"Detection method: {DETECTION_METHOD}")
    logger.info(f"Start time: {datetime.now()}")
    
    overall_start_time = time.time()
    
    try:
        # Phase 1: Environment validation
        validate_parquet_environment(logger)
        
        # TICKET-141: Phase 1.5 - Create and clean temp_results directory
        temp_results_dir = Path(__file__).parent / "temp_results"
        if temp_results_dir.exists():
            shutil.rmtree(temp_results_dir)
            logger.info(f"Cleaned existing temp_results directory")
        temp_results_dir.mkdir(exist_ok=True)
        logger.info(f"Created temp_results directory: {temp_results_dir}")
        
        # Phase 2: System resource analysis
        resource_analysis = analyze_system_resources(logger)
        max_workers = resource_analysis['optimal_worker_count']
        
        # Phase 3: Workload discovery
        partitions = discover_partition_workload(logger)
        
        # Phase 4: Parallel CPD execution (results saved to temp files)
        performance_metrics = execute_parallel_cpd(partitions, max_workers, log_file)
        
        # Phase 5: Memory-efficient final aggregation from temp files
        logger.info("\n" + "=" * 80)

        logger.info("FINAL AGGREGATION PHASE - MEMORY-EFFICIENT")
        logger.info("=" * 80)
        logger.info(f"Scanning temp_results directory for Parquet files...")
        
        # Use lazy scanning and collect for memory-efficient aggregation
        temp_parquet_files = list(temp_results_dir.glob("*.parquet"))
        logger.info(f"Found {len(temp_parquet_files)} result files to aggregate")
        
        if temp_parquet_files:
            # Lazy scan all Parquet files and collect into final DataFrame
            logger.info("Performing memory-efficient aggregation with pl.scan_parquet...")
            results_df = pl.scan_parquet(temp_results_dir / "*.parquet").collect()
            logger.info(f"Successfully aggregated {len(results_df)} total change points")
        else:
            # Create empty DataFrame if no results
            logger.warning("No result files found - creating empty DataFrame")
            results_df = pl.DataFrame({
                'device_id': [],
                'timestamp_start': [],
                'raw_event_hash_id': [],
                'timestamp_end': [],
                'cpd_confidence_score': [],
                'detection_method': [],
                'created_at': []
            }, schema={
                'device_id': pl.Utf8,
                'timestamp_start': pl.Utf8,
                'raw_event_hash_id': pl.Utf8,
                'timestamp_end': pl.Utf8,
                'cpd_confidence_score': pl.Float64,
                'detection_method': pl.Utf8,
                'created_at': pl.Utf8
            })
        
        # Phase 6: Export final results
        output_file = export_final_results(results_df, performance_metrics, logger)
        
        # Phase 7: Clean up temp directory after successful export
        shutil.rmtree(temp_results_dir)
        logger.info(f"Cleaned up temp_results directory after successful export")
        
        # Final summary
        total_time = time.time() - overall_start_time
        
        logger.info("\n" + "=" * 80)
        logger.info("ORCHESTRATOR EXECUTION COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        logger.info(f"Total execution time: {total_time/60:.2f} minutes")
        logger.info(f"Partitions processed: {len(partitions)}")
        logger.info(f"Change points detected: {len(results_df)}")
        logger.info(f"Output file: {output_file}")
        logger.info(f"Success rate: {performance_metrics['parallel_efficiency']*100:.1f}%")
        logger.info(f"Processing rate: {len(partitions)/(total_time/60):.1f} partitions/minute")
        
        # Performance target assessment
        if total_time < 3600:  # Sub-hour target
            logger.info("✓ SUB-HOUR PERFORMANCE TARGET ACHIEVED")
        else:
            logger.warning("⚠ Sub-hour target missed - consider optimization")
        
        print(f"\n=== PRODUCTION ORCHESTRATOR RESULTS ===")
        print(f"Total Time: {total_time/60:.2f} minutes")
        print(f"Partitions: {len(partitions)}")
        print(f"Change Points: {len(results_df)}")
        print(f"Output File: {output_file}")
        print(f"Success Rate: {performance_metrics['parallel_efficiency']*100:.1f}%")
        
        return {
            'success': True,
            'total_time_minutes': round(total_time/60, 2),
            'partitions_processed': len(partitions),
            'change_points_detected': len(results_df),
            'output_file': str(output_file),
            'performance_metrics': performance_metrics
        }
        
    except Exception as e:
        total_time = time.time() - overall_start_time
        logger.error(f"Orchestrator execution failed after {total_time/60:.2f} minutes: {e}")
        
        print(f"\n=== ORCHESTRATOR EXECUTION FAILED ===")
        print(f"Error: {e}")
        print(f"Time elapsed: {total_time/60:.2f} minutes")
        
        # Clean up temp directory even on failure
        temp_results_dir = Path(__file__).parent / "temp_results"
        if temp_results_dir.exists():
            shutil.rmtree(temp_results_dir)
            logger.info(f"Cleaned up temp_results directory after failure")
        
        return {
            'success': False,
            'error': str(e),
            'time_elapsed_minutes': round(total_time/60, 2)
        }

if __name__ == "__main__":
    # TICKET-142: Set start method to 'spawn' to avoid fork-related deadlocks
    # with multi-threaded libraries like numpy/polars.
    mp.set_start_method("spawn", force=True)
    main()
