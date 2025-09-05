# ADVANCED EDA VERIFICATION SCRIPT

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine, text
from shapely.geometry import Point, Polygon
import warnings
import os

# --- SETUP ---
warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', None)
plt.style.use('seaborn-v0_8-whitegrid')
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

# --- 1. LOAD LABELED DATA ---
try:
    query = text("""
    SELECT 
        timestamp, device_id, ml_event_label, current_speed, prndl, parking_brake_applied,
        ST_X(current_position::geometry) as longitude,
        ST_Y(current_position::geometry) as latitude
    FROM \"02.1_labeled_training_data_loaddump\" 
    WHERE ml_event_label IS NOT NULL AND current_position IS NOT NULL;
    """)
    labeled_df = pd.read_sql_query(query, engine)
    print(f"--- 1. Loaded {len(labeled_df):,} labeled records ---")
except Exception as e:
    print(f"--- ERROR at 1. Load Data: {e} ---")
    exit()

# --- 2. GEOSPATIAL ANALYSIS ---
try:
    ZONES = {
        'Crusher': Polygon([(-97.8302154, 33.2580123), (-97.8301054, 33.2578261), (-97.829931, 33.2579001), (-97.8300786, 33.2580796), (-97.830218, 33.258019), (-97.8302154, 33.2580123)]),
        'Stockpile 1': Polygon([(-97.8301483, 33.258324), (-97.8299498, 33.257954), (-97.8294402, 33.2581267), (-97.8291729, 33.2590222), (-97.8293744, 33.2596088), (-97.8297365, 33.2593307), (-97.8301529, 33.2583203), (-97.8301483, 33.258324)]),
        'Stockpile 2': Polygon([(-97.8300501, 33.260527), (-97.8297765, 33.2599618), (-97.8294948, 33.2600986), (-97.8292749, 33.2605584), (-97.8297255, 33.2607356), (-97.8300501, 33.2605247), (-97.8300501, 33.260527)]),
        'Stockpile 3': Polygon([(-97.8277426, 33.2419613), (-97.8276353, 33.2411806), (-97.8269916, 33.2404538), (-97.8264122, 33.2410281), (-97.8263157, 33.2415171), (-97.8266322, 33.2417863), (-97.8277372, 33.2419613), (-97.8277426, 33.2419613)]),
        'Pit 1': Polygon([(-97.8406601, 33.2732661), (-97.8406708, 33.270898), (-97.8354781, 33.2709428), (-97.8358321, 33.2734634), (-97.8406601, 33.2732661)]),
        'Pit 2': Polygon([(-97.8365509, 33.2763763), (-97.8364115, 33.2737571), (-97.8321092, 33.2738737), (-97.8322487, 33.2759726), (-97.8365509, 33.2763763)]),
        'Pit 3': Polygon([(-97.8350793, 33.2736128), (-97.8343498, 33.2700068), (-97.8326761, 33.2700516), (-97.8323435, 33.2736845), (-97.8350793, 33.2736128)])
    }
    def get_location_type(lon, lat):
        point = Point(lon, lat)
        for zone_name, polygon in ZONES.items():
            if polygon.contains(point):
                return zone_name
        return 'Haul Road / Other'
    labeled_df['location_type'] = labeled_df.apply(lambda row: get_location_type(row['longitude'], row['latitude']), axis=1)
    location_counts = labeled_df['location_type'].value_counts()
    location_counts.to_csv(os.path.join(OUTPUT_DIR, 'location_type_counts.csv'))
    print("--- 2. Geospatial analysis complete ---")
    print(location_counts)
except Exception as e:
    print(f"--- ERROR at 2. Geospatial Analysis: {e} ---")
    exit()

# --- 3. MULTI-SENSOR SIGNATURE ANALYSIS ---
try:
    event_df = labeled_df[labeled_df['ml_event_label'].isin(['load_event', 'dump_event'])].copy()
    
    speed_profile = event_df.groupby('ml_event_label')['current_speed'].describe()
    speed_profile.to_csv(os.path.join(OUTPUT_DIR, 'speed_profile.csv'))
    print("\n--- Speed Profile ---")
    print(speed_profile)

    prndl_profile = event_df.groupby(['ml_event_label', 'prndl']).size().unstack(fill_value=0)
    prndl_profile.to_csv(os.path.join(OUTPUT_DIR, 'prndl_profile.csv'))
    print("\n--- PRNDL Profile ---")
    print(prndl_profile)

    brake_profile = event_df.groupby(['ml_event_label', 'parking_brake_applied']).size().unstack(fill_value=0)
    brake_profile.to_csv(os.path.join(OUTPUT_DIR, 'brake_profile.csv'))
    print("\n--- Parking Brake Profile ---")
    print(brake_profile)

    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    sns.boxplot(data=event_df, x='ml_event_label', y='current_speed', ax=axes[0])
    axes[0].set_title('Speed Distribution During Events')
    prndl_profile.plot(kind='bar', stacked=True, ax=axes[1])
    axes[1].set_title('PRNDL State During Events')
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, 'sensor_profiles.png'))
    plt.close()
    print("\n--- 3. Multi-sensor analysis complete, profiles saved ---")
except Exception as e:
    print(f"--- ERROR at 3. Multi-Sensor Analysis: {e} ---")
    exit()

# --- 4. COMBINED GEOSPATIAL & EVENT ANALYSIS ---
try:
    location_event_profile = event_df.groupby(['location_type', 'ml_event_label']).size().unstack(fill_value=0)
    location_event_profile.to_csv(os.path.join(OUTPUT_DIR, 'location_event_profile.csv'))
    print("\n--- Event Counts by Geographic Zone ---")
    print(location_event_profile)

    location_event_profile.plot(kind='bar', stacked=True, figsize=(12, 7))
    plt.title('Primary Event Type by Operational Zone')
    plt.ylabel('Event Count')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, 'location_event_profile.png'))
    plt.close()
    print("\n--- 4. Combined analysis complete, plot saved ---")
except Exception as e:
    print(f"--- ERROR at 4. Combined Analysis: {e} ---")
    exit()

print("\n--- ADVANCED EDA SCRIPT COMPLETED SUCCESSFULLY ---")
