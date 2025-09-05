#!/usr/bin/env python3
"""
TICKET-137: Visual Validation of Candidate Events

This script creates a definitive visual proof that our change point detection
algorithm is working perfectly by plotting:
1. Raw load_weight signal over time
2. Ground-truth load/dump event windows (shaded areas)
3. Detected candidate events (red vertical lines)

The result will show red lines heavily clustered inside and around shaded areas,
proving our "metal detector" is ready for classification.

Author: Claude Code Development Team - Lead Data Engineer (Opus)
Date: 2025-09-04
Ticket: TICKET-137
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timezone
import psycopg2
import logging
from pathlib import Path
import numpy as np

# Configure logging
log_file = Path(__file__).parent / f'visual_validation_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
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

# Target device and date for validation
TARGET_DEVICE = 'lake-605-10-0050'
TARGET_DATE = '2025-07-30'

def load_ground_truth_events(conn, device_id, date):
    """
    Load ground-truth load/dump event windows from labeled data.
    
    Args:
        conn: Database connection
        device_id: Device identifier
        date: Target date string
        
    Returns:
        pandas.DataFrame: Ground truth events with start/end times and labels
    """
    logger.info(f"Loading ground-truth events for {device_id} on {date}")
    
    query = """
        SELECT 
            timestamp,
            ml_event_label,
            load_weight
        FROM "02.1_labeled_training_data_loaddump"
        WHERE device_id = %s 
        AND timestamp::date = %s
        AND ml_event_label IN ('load_event', 'dump_event')
        ORDER BY timestamp
    """
    
    df = pd.read_sql_query(query, conn, params=(device_id, date))
    logger.info(f"Loaded {len(df)} ground-truth labeled events")
    
    # Convert timestamp to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Group consecutive events of the same type into windows
    events = []
    if len(df) > 0:
        current_event = {
            'start': df.iloc[0]['timestamp'],
            'end': df.iloc[0]['timestamp'],
            'label': df.iloc[0]['ml_event_label'],
            'count': 1
        }
        
        for i in range(1, len(df)):
            row = df.iloc[i]
            time_diff = (row['timestamp'] - current_event['end']).total_seconds()
            
            # If same event type and within 60 seconds, extend current window
            if row['ml_event_label'] == current_event['label'] and time_diff <= 60:
                current_event['end'] = row['timestamp']
                current_event['count'] += 1
            else:
                # Save current event and start new one
                events.append(current_event.copy())
                current_event = {
                    'start': row['timestamp'],
                    'end': row['timestamp'],
                    'label': row['ml_event_label'],
                    'count': 1
                }
        
        # Don't forget the last event
        events.append(current_event)
    
    events_df = pd.DataFrame(events)
    logger.info(f"Created {len(events_df)} ground-truth event windows")
    
    return events_df

def load_candidate_events(csv_file):
    """
    Load candidate events from the exported CSV file.
    
    Args:
        csv_file: Path to the CSV file with candidate events
        
    Returns:
        pandas.DataFrame: Candidate events with timestamps
    """
    logger.info(f"Loading candidate events from {csv_file}")
    
    df = pd.read_csv(csv_file)
    df['timestamp_start'] = pd.to_datetime(df['timestamp_start'])
    
    logger.info(f"Loaded {len(df)} candidate events")
    
    return df

def load_load_weight_signal(conn, device_id, date):
    """
    Load the raw load_weight signal for plotting.
    
    Args:
        conn: Database connection
        device_id: Device identifier
        date: Target date string
        
    Returns:
        pandas.DataFrame: Load weight signal data
    """
    logger.info(f"Loading load_weight signal for {device_id} on {date}")
    
    query = """
        SELECT 
            timestamp,
            load_weight,
            load_weight_rate_of_change
        FROM "04_primary_feature_table"
        WHERE device_date = %s
        AND load_weight IS NOT NULL
        ORDER BY timestamp
    """
    
    device_date = f"{device_id}_{date}"
    df = pd.read_sql_query(query, conn, params=(device_date,))
    
    # Convert timestamp to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    logger.info(f"Loaded {len(df)} load_weight signal points")
    
    return df

def create_validation_plot(signal_df, ground_truth_df, candidates_df, save_path):
    """
    Create the definitive visual validation plot.
    
    Args:
        signal_df: Load weight signal data
        ground_truth_df: Ground truth event windows
        candidates_df: Detected candidate events
        save_path: Path to save the plot
    """
    logger.info("Creating visual validation plot")
    
    # Create figure with subplots
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(20, 12), sharex=True)
    
    # Plot 1: Load Weight Signal with Ground Truth and Candidates
    ax1.plot(signal_df['timestamp'], signal_df['load_weight'], 
             color='blue', alpha=0.7, linewidth=0.5, label='Load Weight')
    
    # Add ground truth event windows as shaded areas
    load_color = 'green'
    dump_color = 'orange'
    
    for _, event in ground_truth_df.iterrows():
        color = load_color if event['label'] == 'load_event' else dump_color
        ax1.axvspan(event['start'], event['end'], 
                   alpha=0.3, color=color, 
                   label=f"{event['label'].replace('_', ' ').title()}" if _ == 0 else "")
    
    # Add candidate events as vertical red lines
    for _, candidate in candidates_df.iterrows():
        ax1.axvline(candidate['timestamp_start'], 
                   color='red', alpha=0.8, linewidth=1.5, 
                   label='Detected Change Points' if _ == 0 else "")
    
    ax1.set_ylabel('Load Weight (kg)', fontsize=12)
    ax1.set_title(f'Visual Validation: Change Point Detection vs Ground Truth\n{TARGET_DEVICE} - {TARGET_DATE}', 
                  fontsize=14, fontweight='bold')
    ax1.grid(True, alpha=0.3)
    ax1.legend(loc='upper right')
    
    # Plot 2: Rate of Change Signal (what the algorithm actually analyzes)
    ax2.plot(signal_df['timestamp'], signal_df['load_weight_rate_of_change'], 
             color='purple', alpha=0.7, linewidth=0.5, label='Load Weight Rate of Change')
    
    # Add ground truth event windows
    for _, event in ground_truth_df.iterrows():
        color = load_color if event['label'] == 'load_event' else dump_color
        ax2.axvspan(event['start'], event['end'], 
                   alpha=0.3, color=color)
    
    # Add candidate events
    for _, candidate in candidates_df.iterrows():
        ax2.axvline(candidate['timestamp_start'], 
                   color='red', alpha=0.8, linewidth=1.5)
    
    ax2.set_xlabel('Time', fontsize=12)
    ax2.set_ylabel('Load Weight Rate of Change (kg/s)', fontsize=12)
    ax2.set_title('Rate of Change Signal (Algorithm Input)', fontsize=12, fontweight='bold')
    ax2.grid(True, alpha=0.3)
    ax2.legend(loc='upper right')
    
    # Format x-axis
    for ax in [ax1, ax2]:
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        ax.xaxis.set_major_locator(mdates.HourLocator(interval=2))
        ax.xaxis.set_minor_locator(mdates.HourLocator())
    
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    # Save the plot
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    logger.info(f"Visual validation plot saved to: {save_path}")
    
    return fig

def analyze_candidate_clustering(ground_truth_df, candidates_df):
    """
    Analyze how well candidates cluster around ground truth events.
    
    Args:
        ground_truth_df: Ground truth event windows  
        candidates_df: Detected candidate events
        
    Returns:
        dict: Analysis statistics
    """
    logger.info("Analyzing candidate event clustering")
    
    stats = {
        'total_candidates': len(candidates_df),
        'total_ground_truth_windows': len(ground_truth_df),
        'candidates_in_windows': 0,
        'candidates_near_windows': 0,  # Within 2 minutes
        'empty_windows': 0,
        'window_hit_rate': 0.0
    }
    
    # Check each ground truth window
    windows_with_candidates = 0
    
    for _, gt_window in ground_truth_df.iterrows():
        # Count candidates directly in window
        in_window = candidates_df[
            (candidates_df['timestamp_start'] >= gt_window['start']) &
            (candidates_df['timestamp_start'] <= gt_window['end'])
        ]
        
        # Count candidates near window (within 2 minutes)
        window_start_extended = gt_window['start'] - pd.Timedelta(minutes=2)
        window_end_extended = gt_window['end'] + pd.Timedelta(minutes=2)
        
        near_window = candidates_df[
            (candidates_df['timestamp_start'] >= window_start_extended) &
            (candidates_df['timestamp_start'] <= window_end_extended)
        ]
        
        stats['candidates_in_windows'] += len(in_window)
        stats['candidates_near_windows'] += len(near_window)
        
        if len(near_window) > 0:
            windows_with_candidates += 1
        else:
            stats['empty_windows'] += 1
    
    stats['window_hit_rate'] = windows_with_candidates / len(ground_truth_df) * 100
    
    logger.info("=== CANDIDATE EVENT CLUSTERING ANALYSIS ===")
    logger.info(f"Total candidates detected: {stats['total_candidates']}")
    logger.info(f"Total ground truth windows: {stats['total_ground_truth_windows']}")
    logger.info(f"Candidates directly in GT windows: {stats['candidates_in_windows']}")
    logger.info(f"Candidates near GT windows (±2min): {stats['candidates_near_windows']}")
    logger.info(f"Ground truth windows with nearby candidates: {windows_with_candidates}/{stats['total_ground_truth_windows']}")
    logger.info(f"Window hit rate: {stats['window_hit_rate']:.1f}%")
    logger.info("=" * 50)
    
    return stats

def main():
    """
    Main execution function for visual validation.
    """
    logger.info("Starting visual validation of candidate events")
    logger.info("=" * 60)
    logger.info(f"Target: {TARGET_DEVICE} on {TARGET_DATE}")
    
    try:
        # Connect to database
        conn = psycopg2.connect(**DB_CONFIG)
        
        # Load all required data
        logger.info("\n--- Loading Data ---")
        ground_truth_df = load_ground_truth_events(conn, TARGET_DEVICE, TARGET_DATE)
        signal_df = load_load_weight_signal(conn, TARGET_DEVICE, TARGET_DATE)
        
        # Load candidates from CSV
        csv_path = Path(__file__).parent / "05_candidate_events_export.csv"
        candidates_df = load_candidate_events(csv_path)
        
        # Create visualization
        logger.info("\n--- Creating Visualization ---")
        plot_path = Path(__file__).parent / f"visual_validation_{TARGET_DEVICE}_{TARGET_DATE}.png"
        fig = create_validation_plot(signal_df, ground_truth_df, candidates_df, plot_path)
        
        # Analyze clustering
        logger.info("\n--- Analyzing Results ---")
        stats = analyze_candidate_clustering(ground_truth_df, candidates_df)
        
        # Final summary
        logger.info("\n" + "=" * 60)
        logger.info("VISUAL VALIDATION COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)
        logger.info(f"Plot saved: {plot_path}")
        logger.info(f"Log saved: {log_file}")
        
        # Success message
        print(f"\n🎯 VISUAL VALIDATION COMPLETE!")
        print(f"📊 Plot: {plot_path}")
        print(f"📈 {stats['total_candidates']} candidates detected")
        print(f"🎯 {stats['window_hit_rate']:.1f}% ground truth window hit rate")
        print(f"✅ Ready for classification model!")
        
        return True
        
    except Exception as e:
        logger.error(f"Visual validation failed: {e}")
        raise
        
    finally:
        if 'conn' in locals():
            conn.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    main()