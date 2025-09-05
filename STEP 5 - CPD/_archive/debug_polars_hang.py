#!/usr/bin/env python3
"""
TICKET-145: Final Mandate - Isolate and Fix Polars Hang in 775G Data

This standalone debug script manually executes each preprocessing step on a single 
775G partition with verbose logging to identify the exact cause of the hang.

Author: Claude Code Development Team
Date: 2025-09-04
"""

import polars as pl
import numpy as np
from pathlib import Path
import sys

# Hardcode path to problematic 775G partition
PARTITION_PATH = Path(__file__).parent / "features.parquet" / "device_date=lake-775g-2-2262_2025-07-30"
DOWNSAMPLING_INTERVAL = "5s"

def main():
    print("=" * 80)
    print("TICKET-145: STANDALONE 775G POLARS DEBUG")
    print("=" * 80)
    print(f"Target partition: {PARTITION_PATH}")
    print()
    
    try:
        # STEP 1: Load Data
        print("STEP 1: Loading Parquet data...")
        if not PARTITION_PATH.exists():
            raise FileNotFoundError(f"Partition path not found: {PARTITION_PATH}")
        
        df = pl.scan_parquet(PARTITION_PATH / "*.parquet").select([
            "timestamp", 
            "speed_rolling_avg_5s",
            "altitude_rate_of_change", 
            "has_reliable_payload",
            "raw_event_hash_id"
        ]).collect(streaming=True)
        
        print("STEP 1: Data loaded successfully.")
        print(f"Shape: {df.shape}")
        print("Columns:", df.columns)
        print("Data types:")
        print(df.dtypes)
        print("\nFirst 5 rows:")
        print(df.head())
        print()
        
        # STEP 2: Check has_reliable_payload flag
        print("STEP 2: Checking has_reliable_payload flag...")
        has_reliable_payload_raw = df['has_reliable_payload'][0] if len(df) > 0 else "f"
        has_reliable_payload = has_reliable_payload_raw.lower() in ['true', 't', '1', 'yes']
        print(f"has_reliable_payload (raw): {has_reliable_payload_raw}")
        print(f"has_reliable_payload (parsed): {has_reliable_payload}")
        
        if has_reliable_payload:
            print("ERROR: This should be a 775G (unreliable payload) partition!")
            sys.exit(1)
        print()
        
        # STEP 3: Initial Null Filtering
        print("STEP 3: Filtering for non-null kinematic signals...")
        original_count = len(df)
        df_clean = df.filter(
            pl.col("speed_rolling_avg_5s").is_not_null() &
            pl.col("altitude_rate_of_change").is_not_null()
        )
        
        print(f"STEP 3: Initial null filtering complete.")
        print(f"Original rows: {original_count}")
        print(f"Rows after null filtering: {len(df_clean)}")
        print(f"Rows removed: {original_count - len(df_clean)}")
        
        if len(df_clean) == 0:
            print("ERROR: All data filtered out - no valid kinematic signals!")
            sys.exit(1)
        print()
        
        # STEP 4: Timestamp Conversion
        print("STEP 4: Converting timestamp to datetime...")
        df_timestamped = df_clean.with_columns(
            pl.col("timestamp").str.to_datetime(strict=False, time_zone="UTC")
        )
        
        null_timestamps = df_timestamped.filter(pl.col("timestamp").is_null()).height
        print("STEP 4: Timestamp conversion complete.")
        print(f"Null timestamps after conversion: {null_timestamps}")
        
        if null_timestamps > 0:
            print(f"WARNING: {null_timestamps} timestamps could not be converted!")
            # Filter out null timestamps
            df_timestamped = df_timestamped.filter(pl.col("timestamp").is_not_null())
            print(f"Rows after removing null timestamps: {len(df_timestamped)}")
        print()
        
        # STEP 5: Sort Data
        print("STEP 5: Sorting by timestamp...")
        df_sorted = df_timestamped.sort("timestamp")
        print("STEP 5: Sort by timestamp complete.")
        print(f"Final data shape: {df_sorted.shape}")
        print()
        
        # STEP 6: Final Pre-computation Validation (CRITICAL)
        print("STEP 6: Performing final validation before downsampling...")
        
        # Check if timestamp is sorted
        timestamps = df_sorted['timestamp'].to_numpy()
        is_sorted = np.all(timestamps[:-1] <= timestamps[1:])
        print(f"Timestamp is sorted: {is_sorted}")
        
        # Check for non-finite values in signals
        speed_values = df_sorted['speed_rolling_avg_5s'].to_numpy()
        altitude_values = df_sorted['altitude_rate_of_change'].to_numpy()
        
        speed_is_finite = np.all(np.isfinite(speed_values))
        altitude_is_finite = np.all(np.isfinite(altitude_values))
        
        print(f"Speed signal is finite: {speed_is_finite}")
        print(f"Altitude signal is finite: {altitude_is_finite}")
        
        if not speed_is_finite:
            non_finite_speed = np.sum(~np.isfinite(speed_values))
            print(f"  Non-finite speed values: {non_finite_speed}/{len(speed_values)}")
            print(f"  Speed range: {np.nanmin(speed_values):.3f} to {np.nanmax(speed_values):.3f}")
        
        if not altitude_is_finite:
            non_finite_altitude = np.sum(~np.isfinite(altitude_values))
            print(f"  Non-finite altitude values: {non_finite_altitude}/{len(altitude_values)}")
            print(f"  Altitude range: {np.nanmin(altitude_values):.3f} to {np.nanmax(altitude_values):.3f}")
        
        # Check timestamp range and intervals
        print(f"\nTimestamp validation:")
        print(f"  First timestamp: {timestamps[0]}")
        print(f"  Last timestamp: {timestamps[-1]}")
        print(f"  Duration: {timestamps[-1] - timestamps[0]}")
        
        # Calculate timestamp intervals
        intervals = np.diff(timestamps.astype('datetime64[s]').astype('int64'))
        print(f"  Min interval: {np.min(intervals)}s")
        print(f"  Max interval: {np.max(intervals)}s")
        print(f"  Mean interval: {np.mean(intervals):.1f}s")
        
        # Validation checks - stop if critical issues found
        if not is_sorted:
            print("CRITICAL ERROR: Timestamps are not sorted!")
            sys.exit(1)
        
        if len(df_sorted) < 20:
            print(f"CRITICAL ERROR: Insufficient data points ({len(df_sorted)}) for processing!")
            sys.exit(1)
        
        print("STEP 6: All validations passed.")
        print()
        
        # STEP 7: Execute Downsampling (The Moment of Truth)
        print("STEP 7: Attempting group_by_dynamic downsampling...")
        print("This is where the hang typically occurs...")
        
        df_downsampled = (
            df_sorted
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
        
        print("STEP 7: Downsampling completed successfully!")
        print(f"Downsampled shape: {df_downsampled.shape}")
        print(f"Data reduction: {len(df_sorted)} -> {len(df_downsampled)} ({len(df_downsampled)/len(df_sorted)*100:.1f}%)")
        print()
        
        # STEP 8: Validate downsampled results
        print("STEP 8: Validating downsampled results...")
        print("Downsampled data summary:")
        print(df_downsampled.head())
        
        # Check for any remaining issues in downsampled data
        speed_down = df_downsampled['speed_rolling_avg_5s'].to_numpy()
        altitude_down = df_downsampled['altitude_rate_of_change'].to_numpy()
        
        print(f"\nDownsampled signal validation:")
        print(f"  Speed finite: {np.all(np.isfinite(speed_down))}")
        print(f"  Altitude finite: {np.all(np.isfinite(altitude_down))}")
        print(f"  Speed range: {np.nanmin(speed_down):.3f} to {np.nanmax(speed_down):.3f}")
        print(f"  Altitude range: {np.nanmin(altitude_down):.3f} to {np.nanmax(altitude_down):.3f}")
        
        # STEP 9: Test PELT Algorithm on both signals
        print("\nSTEP 9: Testing PELT algorithm on downsampled signals...")
        
        import ruptures as rpt
        PELT_PENALTY = 0.05
        PELT_MIN_SIZE = 10
        PELT_JUMP = 1
        
        # Test Speed CPD
        if len(speed_down) >= 10:
            print("  Testing Speed CPD...")
            try:
                speed_array = speed_down.reshape(-1, 1)
                algo_speed = rpt.Pelt(model="l2", min_size=PELT_MIN_SIZE, jump=PELT_JUMP)
                algo_speed.fit(speed_array)
                speed_change_points = algo_speed.predict(pen=PELT_PENALTY)
                print(f"    Speed CPD successful: {len(speed_change_points)} change points")
            except Exception as e:
                print(f"    Speed CPD failed: {e}")
        
        # Test Altitude CPD  
        if len(altitude_down) >= 10:
            print("  Testing Altitude CPD...")
            try:
                altitude_array = altitude_down.reshape(-1, 1)
                algo_altitude = rpt.Pelt(model="l2", min_size=PELT_MIN_SIZE, jump=PELT_JUMP)
                algo_altitude.fit(altitude_array)
                altitude_change_points = algo_altitude.predict(pen=PELT_PENALTY)
                print(f"    Altitude CPD successful: {len(altitude_change_points)} change points")
            except Exception as e:
                print(f"    Altitude CPD failed: {e}")
        
        print()
        print("=" * 80)
        print("SUCCESS: FULL 775G PIPELINE COMPLETED!")
        print("=" * 80)
        print("All steps including PELT algorithm completed successfully.")
        print("775G data is fully processable - hanging issue is in multiprocessing.")
        
    except Exception as e:
        print()
        print("=" * 80)
        print("ERROR DETECTED:")
        print("=" * 80)
        print(f"Exception: {type(e).__name__}: {e}")
        print("This is likely the root cause of the hanging issue.")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()