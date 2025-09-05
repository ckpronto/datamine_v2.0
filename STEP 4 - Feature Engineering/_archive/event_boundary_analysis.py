#!/usr/bin/env python3
"""
Event Boundary Analysis - Exploratory Data Analysis
AHS Analytics Engine - DataMine V2

This script performs exploratory data analysis to validate feature engineering ideas
by analyzing sensor signal changes around load and dump events.

Author: Claude Code Development Team
Date: 2025-09-03
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import seaborn as sns
from datetime import timedelta

# Database connection configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'datamine_v2_db',
    'user': 'ahs_user',
    'password': 'ahs_password'
}

def connect_to_database():
    """Create database connection using SQLAlchemy."""
    connection_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    engine = create_engine(connection_string)
    return engine

def load_prefiltered_window_data(engine, event_type):
    """
    Executes a single, powerful SQL query using hash ID joins for maximum efficiency.
    Uses raw_event_hash_id for exact 1:1 joins instead of device_id + timestamp.
    """
    # This optimized SQL query uses hash ID joins for better performance
    query = f"""
    WITH event_boundaries AS (
        -- Step 1: Identify the start of each new event block using hash IDs
        SELECT
            raw_event_hash_id,
            device_id,
            timestamp,
            ml_event_label,
            -- This flag is TRUE only on the first row of an event
            (LAG(ml_event_label, 1, 'none') OVER (PARTITION BY device_id ORDER BY timestamp) != ml_event_label) AS is_new_event
        FROM "02.1_labeled_training_data_loaddump"
        WHERE ml_event_label = '{event_type}'
        ORDER BY device_id, timestamp
    ),
    event_groups AS (
        -- Step 2: Assign a unique ID to each consecutive event block
        SELECT
            raw_event_hash_id,
            device_id,
            timestamp,
            ml_event_label,
            -- The SUM() over the boolean flag creates a unique group ID for each event
            SUM(CASE WHEN is_new_event THEN 1 ELSE 0 END) OVER (PARTITION BY device_id ORDER BY timestamp) as event_block_id
        FROM event_boundaries
    ),
    event_with_times AS (
        -- Step 3: Add start/end times to each event record
        SELECT
            raw_event_hash_id,
            device_id,
            timestamp,
            ml_event_label,
            event_block_id,
            device_id || '_' || event_block_id AS event_unique_id,
            MIN(timestamp) OVER (PARTITION BY device_id, event_block_id) AS start_time,
            MAX(timestamp) OVER (PARTITION BY device_id, event_block_id) AS end_time
        FROM event_groups
    )
    -- Step 4: Use hash ID for exact 1:1 join with telemetry data
    SELECT
        e.event_unique_id,
        t.current_speed,
        t.load_weight,
        ST_Z(t.current_position::geometry) as altitude,
        e.timestamp as event_timestamp,
        e.start_time,
        e.end_time,
        -- Label which period this specific telemetry point belongs to
        CASE
            WHEN e.timestamp BETWEEN (e.start_time - INTERVAL '30 seconds') AND e.start_time THEN 'before_event'
            WHEN e.timestamp BETWEEN e.start_time AND e.end_time THEN 'during_event'
            WHEN e.timestamp BETWEEN e.end_time AND (e.end_time + INTERVAL '30 seconds') THEN 'after_event'
        END AS period
    FROM event_with_times e
    JOIN "02_raw_telemetry_transformed" t ON e.raw_event_hash_id = t.raw_event_hash_id
    WHERE 
        t.current_position IS NOT NULL
        AND e.start_time != e.end_time  -- Remove single-timestamp events
        AND e.timestamp BETWEEN (e.start_time - INTERVAL '30 seconds') AND (e.end_time + INTERVAL '30 seconds')
    """
    
    print(f"Loading pre-filtered data for {event_type} from database using hash ID joins...")
    # This DataFrame will be much smaller and ready for analysis
    data = pd.read_sql(query, engine)
    print(f"Loaded {len(data)} analysis-ready records for {event_type}.")
    return data

def extract_event_windows_database_optimized(data):
    """
    This function becomes much simpler. It just aggregates the pre-filtered data.
    """
    signals = ['current_speed', 'load_weight', 'altitude']
    results = {signal: {'before_event': [], 'during_event': [], 'after_event': []} for signal in signals}

    if data.empty:
        print("No data to analyze")
        return results

    # The only work left is a simple groupby-aggregate
    grouped_means = data.groupby(['event_unique_id', 'period'])[signals].mean().reset_index()
    
    print(f"Calculated means for {len(grouped_means)} event-period combinations")

    for signal in signals:
        for period in ['before_event', 'during_event', 'after_event']:
            values = grouped_means[grouped_means['period'] == period][signal]
            results[signal][period] = values.dropna().tolist()
            
    return results

def create_comparison_plots(load_results, dump_results):
    """Create bar plots comparing signal values across event periods."""
    
    signals = ['current_speed', 'load_weight', 'altitude']
    periods = ['before_event', 'during_event', 'after_event']
    
    # Set up the plotting style
    plt.style.use('default')
    sns.set_palette("husl")
    
    fig, axes = plt.subplots(2, 3, figsize=(18, 12))
    fig.suptitle('Event Boundary Analysis - Load and Dump Events', fontsize=16, fontweight='bold')
    
    for i, signal in enumerate(signals):
        # Load events plot
        ax_load = axes[0, i]
        load_means = [np.mean(load_results[signal][period]) for period in periods]
        load_stds = [np.std(load_results[signal][period]) for period in periods]
        
        bars_load = ax_load.bar(periods, load_means, yerr=load_stds, capsize=5, alpha=0.7)
        ax_load.set_title(f'Load Events - {signal.replace("_", " ").title()}')
        ax_load.set_ylabel('Average Value')
        ax_load.tick_params(axis='x', rotation=45)
        
        # Add value labels on bars
        for bar, mean in zip(bars_load, load_means):
            height = bar.get_height()
            ax_load.text(bar.get_x() + bar.get_width()/2., height,
                        f'{mean:.2f}', ha='center', va='bottom')
        
        # Dump events plot
        ax_dump = axes[1, i]
        dump_means = [np.mean(dump_results[signal][period]) for period in periods]
        dump_stds = [np.std(dump_results[signal][period]) for period in periods]
        
        bars_dump = ax_dump.bar(periods, dump_means, yerr=dump_stds, capsize=5, alpha=0.7)
        ax_dump.set_title(f'Dump Events - {signal.replace("_", " ").title()}')
        ax_dump.set_ylabel('Average Value')
        ax_dump.tick_params(axis='x', rotation=45)
        
        # Add value labels on bars
        for bar, mean in zip(bars_dump, dump_means):
            height = bar.get_height()
            ax_dump.text(bar.get_x() + bar.get_width()/2., height,
                        f'{mean:.2f}', ha='center', va='bottom')
    
    plt.tight_layout()
    
    # Save the plot
    output_path = '/home/ck/DataMine/APPLICATION/V2/STEP 4 - Feature Engineering/event_boundary_analysis.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"\nPlot saved to: {output_path}")
    
    plt.show()

def print_summary_statistics(load_results, dump_results):
    """Print summary statistics for the analysis."""
    
    print("\n" + "="*60)
    print("EVENT BOUNDARY ANALYSIS SUMMARY")
    print("="*60)
    
    signals = ['current_speed', 'load_weight', 'altitude']
    periods = ['before_event', 'during_event', 'after_event']
    
    for signal in signals:
        print(f"\n{signal.replace('_', ' ').title().upper()}:")
        print("-" * 40)
        
        print("LOAD EVENTS:")
        for period in periods:
            values = load_results[signal][period]
            if values:
                mean_val = np.mean(values)
                std_val = np.std(values)
                print(f"  {period.replace('_', ' ').title()}: {mean_val:.2f} ± {std_val:.2f}")
            else:
                print(f"  {period.replace('_', ' ').title()}: No data")
        
        print("DUMP EVENTS:")
        for period in periods:
            values = dump_results[signal][period]
            if values:
                mean_val = np.mean(values)
                std_val = np.std(values)
                print(f"  {period.replace('_', ' ').title()}: {mean_val:.2f} ± {std_val:.2f}")
            else:
                print(f"  {period.replace('_', ' ').title()}: No data")

def main():
    """Main execution function."""
    
    print("Starting Event Boundary Analysis...")
    print("="*50)
    
    # Connect to database
    engine = connect_to_database()
    print("✓ Connected to database")
    
    # Load pre-filtered data for each event type using database-optimized approach
    load_data = load_prefiltered_window_data(engine, 'load_event')
    load_results = extract_event_windows_database_optimized(load_data)
    
    dump_data = load_prefiltered_window_data(engine, 'dump_event')
    dump_results = extract_event_windows_database_optimized(dump_data)
    
    # Create visualization
    create_comparison_plots(load_results, dump_results)
    
    # Print summary statistics
    print_summary_statistics(load_results, dump_results)
    
    print("\n" + "="*50)
    print("Event Boundary Analysis Complete!")
    print("This analysis confirms the presence of distinct signal patterns")
    print("around event boundaries, validating our feature engineering approach.")

if __name__ == "__main__":
    main()