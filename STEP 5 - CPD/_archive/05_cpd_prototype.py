#!/usr/bin/env python3
"""
Change Point Detection Prototype - Step 3.2
TICKET-113: CPD Pipeline Design V2

Prototype implementation using PELT algorithm on single device-date
to detect change points in load_weight_rate_of_change signal.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import ruptures as rpt
import psycopg2
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Database connection parameters
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'user': 'ahs_user',
    'password': 'ahs_password',
    'database': 'datamine_v2_db'
}

def fetch_data(device_id, device_date, sample_size=5000):
    """Fetch data for single device-date from 04_primary_feature_table"""
    conn = psycopg2.connect(**DB_CONFIG)
    
    # Limit to a sample for prototyping
    query = """
    SELECT 
        timestamp,
        load_weight_rate_of_change,
        load_weight,
        is_stationary,
        device_id,
        device_date
    FROM "04_primary_feature_table" 
    WHERE device_id = %s AND device_date = %s
    ORDER BY timestamp
    LIMIT %s;
    """
    
    print(f"Fetching first {sample_size} records...")
    df = pd.read_sql_query(query, conn, params=(device_id, device_date, sample_size))
    conn.close()
    
    return df

def preprocess_signal(df):
    """Preprocess the load_weight_rate_of_change signal"""
    # Handle NaN values by forward fill then backward fill
    df['load_weight_rate_of_change_clean'] = df['load_weight_rate_of_change'].fillna(method='ffill').fillna(method='bfill')
    
    # If still NaN, fill with 0
    df['load_weight_rate_of_change_clean'] = df['load_weight_rate_of_change_clean'].fillna(0)
    
    return df

def apply_pelt_cpd(signal, pen=10):
    """Apply PELT algorithm for change point detection"""
    # Convert to numpy array
    signal_array = np.array(signal).reshape(-1, 1)
    
    # Initialize PELT algorithm with L2 norm (for continuous signals)
    algo = rpt.Pelt(model="l2", min_size=10, jump=1)
    
    # Fit the model
    algo.fit(signal_array)
    
    # Predict change points
    change_points = algo.predict(pen=pen)
    
    # Remove the last point (it's always the end of signal)
    if len(change_points) > 0 and change_points[-1] == len(signal):
        change_points = change_points[:-1]
    
    return change_points

def visualize_results(df, change_points, pen_value):
    """Create visualization of change points overlaid on signal"""
    plt.figure(figsize=(15, 8))
    
    # Plot the signal
    plt.subplot(2, 1, 1)
    plt.plot(df.index, df['load_weight_rate_of_change_clean'], 'b-', alpha=0.7, label='Load Weight Rate of Change')
    
    # Add change points as vertical lines
    for cp in change_points:
        plt.axvline(x=cp, color='red', linestyle='--', alpha=0.8)
    
    plt.title(f'Change Point Detection V2 - Device: {df["device_id"].iloc[0]} | Date: {df["device_date"].iloc[0]} | Pen: {pen_value}')
    plt.ylabel('Load Weight Rate of Change')
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    # Plot load weight for context
    plt.subplot(2, 1, 2)
    plt.plot(df.index, df['load_weight'], 'g-', alpha=0.7, label='Load Weight')
    
    # Add change points as vertical lines
    for cp in change_points:
        plt.axvline(x=cp, color='red', linestyle='--', alpha=0.8)
    
    plt.xlabel('Sample Index')
    plt.ylabel('Load Weight')
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    # Save the plot
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"cpd_prototype_v2_{df['device_id'].iloc[0]}_pen{pen_value}_{timestamp}.png"
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    
    return filename

def extract_change_point_timestamps(df, change_points):
    """Convert change point indices to actual timestamps"""
    timestamps = []
    
    for cp_idx in change_points:
        if cp_idx < len(df):
            timestamp = df.iloc[cp_idx]['timestamp']
            timestamps.append(timestamp)
    
    return timestamps

def main():
    print("CPD Prototype V2 - Step 3.2")
    print("=" * 40)
    
    # Parameters
    device_id = 'lake-605-8-0896'
    device_date = 'lake-605-8-0896_2025-08-08'
    pen_value = 10  # Default penalty value
    
    print(f"Device: {device_id}")
    print(f"Date: {device_date}")
    print(f"Penalty: {pen_value}")
    print()
    
    # Fetch data (sample for prototyping)
    print("Fetching data from database...")
    df = fetch_data(device_id, device_date, sample_size=5000)
    print(f"Retrieved {len(df)} records")
    
    # Preprocess signal
    print("Preprocessing signal...")
    df = preprocess_signal(df)
    
    # Check for valid signal
    signal = df['load_weight_rate_of_change_clean']
    print(f"Signal stats - Mean: {signal.mean():.4f}, Std: {signal.std():.4f}, Min: {signal.min():.4f}, Max: {signal.max():.4f}")
    
    # Apply PELT change point detection
    print("Applying PELT change point detection...")
    change_points = apply_pelt_cpd(signal, pen=pen_value)
    
    print(f"Detected {len(change_points)} change points")
    
    # Extract timestamps for change points
    change_point_timestamps = extract_change_point_timestamps(df, change_points)
    
    print("\nDetected Change Points:")
    print("-" * 20)
    for i, (idx, timestamp) in enumerate(zip(change_points, change_point_timestamps)):
        print(f"{i+1:2d}. Index: {idx:6d} | Timestamp: {timestamp}")
    
    # Create visualization
    print("\nCreating visualization...")
    plot_filename = visualize_results(df, change_points, pen_value)
    print(f"Visualization saved as: {plot_filename}")
    
    # Summary statistics
    print(f"\nSummary:")
    print(f"- Total samples: {len(df)}")
    print(f"- Change points detected: {len(change_points)}")
    print(f"- Detection rate: {len(change_points)/len(df)*100:.4f}% of samples")
    
    return {
        'device_id': device_id,
        'device_date': device_date,
        'total_samples': len(df),
        'change_points': change_points,
        'change_point_timestamps': change_point_timestamps,
        'pen_value': pen_value,
        'plot_filename': plot_filename
    }

if __name__ == "__main__":
    results = main()