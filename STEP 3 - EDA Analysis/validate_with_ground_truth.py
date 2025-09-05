# GROUND TRUTH VALIDATION SCRIPT

import pandas as pd
from sqlalchemy import create_engine, text
import os
import warnings

# --- SETUP ---
warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
OUTPUT_DIR = "/home/ck/DataMine/APPLICATION/V2/STEP 3 - EDA Analysis/advanced_eda_output"
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)
print("--- Libraries imported and setup complete ---")

# --- DATABASE CONNECTION ---
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'datamine_v2_db',
    'user': 'ahs_user',
    'password': 'ahs_password'
}
try:
    engine = create_engine(f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    with engine.connect() as connection:
        print("--- Database connection successful ---")
except Exception as e:
    print(f"--- Database connection failed: {e} ---")
    exit()

# --- 1. LOAD DATA ---
try:
    # Load existing labeled training data
    query_training = text("""
    SELECT device_id, timestamp, ml_event_label
    FROM \"02.1_labeled_training_data_loaddump\"
    WHERE ml_event_label IS NOT NULL;
    """)
    training_df = pd.read_sql_query(query_training, engine)
    training_df['timestamp'] = pd.to_datetime(training_df['timestamp'])
    print(f"--- Loaded {len(training_df):,} records from '02.1_labeled_training_data_loaddump' ---")

    # Load high-quality ground truth data
    query_ground_truth = text("""
    SELECT device_id, timestamp, actual_event_label
    FROM \"00_ground_truth_605-10-0050_8-1_hand_labeled\"
    WHERE actual_event_label IS NOT NULL;
    """)
    ground_truth_df = pd.read_sql_query(query_ground_truth, engine)
    ground_truth_df['timestamp'] = pd.to_datetime(ground_truth_df['timestamp'])
    print(f"--- Loaded {len(ground_truth_df):,} records from '00_ground_truth_605-10-0050_8-1_hand_labeled' ---")

except Exception as e:
    print(f"--- ERROR at 1. Load Data: {e} ---")
    exit()

# --- 2. MERGE & COMPARE ---
try:
    # Merge the two dataframes on device_id and timestamp
    comparison_df = pd.merge(
        training_df,
        ground_truth_df,
        on=['device_id', 'timestamp'],
        how='inner' # Use inner join to only compare timestamps present in both tables
    )
    print(f"--- Merged dataframes, resulting in {len(comparison_df):,} common records ---")

    # Identify discrepancies
    comparison_df['label_matches'] = comparison_df['ml_event_label'] == comparison_df['actual_event_label']
    discrepancies_df = comparison_df[comparison_df['label_matches'] == False]
    print(f"--- Found {len(discrepancies_df):,} label discrepancies ---")

except Exception as e:
    print(f"--- ERROR at 2. Merge & Compare: {e} ---")
    exit()

# --- 3. GENERATE DISCREPANCY REPORT ---
try:
    if not discrepancies_df.empty:
        # Create a summary report
        discrepancy_summary = discrepancies_df.groupby(['ml_event_label', 'actual_event_label']).size().reset_index(name='count')
        discrepancy_summary = discrepancy_summary.sort_values(by='count', ascending=False)

        # Save the detailed discrepancies and the summary to CSV
        discrepancies_filepath = os.path.join(OUTPUT_DIR, 'label_discrepancies_detailed.csv')
        summary_filepath = os.path.join(OUTPUT_DIR, 'label_discrepancy_summary.csv')

        discrepancies_df.to_csv(discrepancies_filepath, index=False)
        discrepancy_summary.to_csv(summary_filepath, index=False)

        print("\n--- DISCREPANCY REPORT ---")
        print(discrepancy_summary)
        print(f"\n--- Detailed discrepancy report saved to: {discrepancies_filepath} ---")
        print(f"--- Summary report saved to: {summary_filepath} ---")
    else:
        print("\n--- NO DISCREPANCIES FOUND ---")
        # Create an empty file to indicate the check was run
        open(os.path.join(OUTPUT_DIR, 'no_discrepancies_found.txt'), 'a').close()


except Exception as e:
    print(f"--- ERROR at 3. Generate Report: {e} ---")
    exit()

print("\n--- VALIDATION SCRIPT COMPLETED SUCCESSFULLY ---")
