#!/usr/bin/env python3
"""
Change Point Detection Parameter Tuning V2 (Corrected)
TICKET-122: Re-architect Parameter Tuning for Performance and Two-Phase Validation

Optimized high-performance CPD parameter tuning with:
- Bulk data loading (eliminates N+1 query problem)
- Parallel penalty testing using ProcessPoolExecutor (for CPU-bound tasks)
- Two-phase validation (coarse tuning on large dataset, fine-grained on high-precision dataset)

Author: Claude Code Development Team (Restored by Lead Architect)
Date: 2025-09-04
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import ruptures as rpt
import psycopg2
from datetime import datetime, timedelta
import json
import warnings
from concurrent.futures import ProcessPoolExecutor
import multiprocessing as mp
import time
from functools import partial

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore')

# --- Database Configuration ---
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'user': 'ahs_user',
    'password': 'ahs_password',
    'database': 'datamine_v2_db'
}

# --- Data Fetching ---

def fetch_ground_truth_events(source='loaddump'):
    """
    Fetches ground truth event windows from the specified source table.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    
    table_map = {
        'loaddump': '"02.1_labeled_training_data_loaddump"',
        'hand_labeled': '"00_ground_truth_605-10-0050_8-1_hand_labeled"'
    }
    
    if source not in table_map:
        raise ValueError(f"Invalid source '{source}'. Must be 'loaddump' or 'hand_labeled'.")
        
    table_name = table_map[source]
    label_column = 'ml_event_label' if source == 'loaddump' else 'actual_event_label'

    query = f"""
    WITH events AS (
        SELECT 
            device_id,
            MIN(timestamp) as start_time,
            MAX(timestamp) as end_time
        FROM {table_name}
        WHERE {label_column} IN ('load_event', 'dump_event')
        GROUP BY device_id, raw_event_hash_id
    )
    SELECT * FROM events ORDER BY start_time;
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df.to_dict('records')

def fetch_bulk_feature_data(event_windows):
    """
    TICKET-120: Fetches all required time-series data in a single bulk query.
    """
    if not event_windows:
        return pd.DataFrame()

    conn = psycopg2.connect(**DB_CONFIG)
    
    union_queries = []
    for event in event_windows:
        # Add a 60-second buffer before and after each event to ensure we capture the change points
        start = event['start_time'] - timedelta(seconds=60)
        end = event['end_time'] + timedelta(seconds=60)
        union_queries.append(
            f"""(SELECT device_id, timestamp, load_weight_rate_of_change 
               FROM "04_primary_feature_table" 
               WHERE device_id = '{event['device_id']}' 
               AND timestamp BETWEEN '{start.isoformat()}' AND '{end.isoformat()}')"""
        )
        
    full_query = "\nUNION ALL\n".join(union_queries)
    
    df = pd.read_sql_query(full_query, conn)
    conn.close()
    df.sort_values(by=['device_id', 'timestamp'], inplace=True)
    return df

# --- Core CPD and Evaluation Logic ---

def apply_pelt_cpd(signal_data, penalty):
    """
    Applies the PELT algorithm to a signal with a given penalty.
    This is the target function for our parallel processing.
    """
    signal = signal_data.reshape(-1, 1)
    if len(signal) < 10:
        return []
    try:
        algo = rpt.Pelt(model="l2", min_size=10).fit(signal)
        result = algo.predict(pen=penalty)
        return result
    except Exception:
        return []

def calculate_recall(detected_windows, ground_truth_windows):
    """
    Calculates recall: % of ground truth events successfully detected.
    """
    if not ground_truth_windows:
        return 1.0, 0, 0

    detected_count = 0
    for gt_event in ground_truth_windows:
        gt_start, gt_end = gt_event['start_time'], gt_event['end_time']
        for detected_start, detected_end in detected_windows:
            # Check for any overlap
            if max(gt_start, detected_start) < min(gt_end, detected_end):
                detected_count += 1
                break # Move to the next ground truth event
                
    total_events = len(ground_truth_windows)
    recall = detected_count / total_events if total_events > 0 else 1.0
    return recall, detected_count, total_events

def run_single_penalty_test(penalty, feature_df, event_windows):
    """
    A wrapper function for a single penalty test run.
    """
    signal_data = feature_df['load_weight_rate_of_change'].fillna(0).to_numpy()
    change_points = apply_pelt_cpd(signal_data, penalty)
    
    detected_windows = []
    for start_idx, end_idx in zip([0] + change_points[:-1], change_points):
        start_time = feature_df['timestamp'].iloc[start_idx]
        end_time = feature_df['timestamp'].iloc[end_idx - 1]
        detected_windows.append((start_time, end_time))

    recall, detected_events, total_events = calculate_recall(detected_windows, event_windows)
    
    return {
        'penalty': penalty,
        'num_change_points': len(change_points),
        'recall': recall,
        'detected_events': detected_events,
        'total_events': total_events
    }

# --- Main Orchestrator ---

def run_tuning_phase(source, penalty_range):
    """
    Orchestrates a full tuning phase (coarse or fine-grained).
    """
    logger.info(f"--- Starting Tuning Phase: Source='{source}' ---")
    
    # 1. Fetch ground truth data
    event_windows = fetch_ground_truth_events(source=source)
    logger.info(f"Found {len(event_windows)} ground truth events.")

    # 2. Fetch feature data in bulk
    feature_df = fetch_bulk_feature_data(event_windows)
    logger.info(f"Loaded {len(feature_df)} corresponding feature records.")
    
    if feature_df.empty:
        logger.warning("No feature data found for the given events. Skipping phase.")
        return []

    # 3. TICKET-121 & 123: Run tuning in parallel using ProcessPoolExecutor
    logger.info(f"Testing {len(penalty_range)} penalties using up to {mp.cpu_count()} CPU cores...")
    results = []
    
    with ProcessPoolExecutor(max_workers=mp.cpu_count()) as executor:
        # Use partial to pre-fill the function with data, leaving only penalty to be mapped
        func = partial(run_single_penalty_test, feature_df=feature_df, event_windows=event_windows)
        results = list(executor.map(func, penalty_range))

    logger.info("Parallel tuning complete.")
    return sorted(results, key=lambda x: (-x['recall'], x['num_change_points']))


def main():
    """
    Main execution block for the two-phase tuning process.
    """
    start_time = time.time()
    logger.info("=" * 80)
    logger.info("TICKET-122: Starting Two-Phase CPD Parameter Tuning")
    logger.info("=" * 80)

    # Phase 1: Coarse tuning on the large dataset
    coarse_penalty_range = np.arange(0.05, 2.01, 0.05).tolist()
    coarse_results = run_tuning_phase(source='loaddump', penalty_range=coarse_penalty_range)
    
    if not coarse_results:
        logger.error("Coarse tuning failed to produce results. Aborting.")
        return

    # Identify top candidates from Phase 1
    top_recall = coarse_results[0]['recall']
    top_candidates = [r['penalty'] for r in coarse_results if r['recall'] >= top_recall][:5]
    logger.info(f"Phase 1 complete. Top 5 candidates for fine-tuning: {top_candidates}")

    # Phase 2: Fine-grained tuning on the high-precision dataset
    fine_results = run_tuning_phase(source='hand_labeled', penalty_range=top_candidates)

    if not fine_results:
        logger.error("Fine-grained tuning failed to produce results. Using coarse results.")
        final_optimal = coarse_results[0]
    else:
        final_optimal = fine_results[0]
        logger.info("Phase 2 complete.")
        
    end_time = time.time()

    # --- Final Report ---
    logger.info("=" * 80)
    logger.info("Tuning Complete. Final Optimal Parameter:")
    logger.info(f"  - Optimal Penalty: {final_optimal['penalty']}")
    logger.info(f"  - Final Recall: {final_optimal['recall']:.2%}")
    logger.info(f"  - Detected Events: {final_optimal['detected_events']} / {final_optimal['total_events']}")
    logger.info(f"  - Total Runtime: {end_time - start_time:.2f} seconds")
    logger.info("=" * 80)


if __name__ == "__main__":
    # Configure logging for the script
    import logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    main()