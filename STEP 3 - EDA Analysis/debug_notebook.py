# Auto-generated script from 01_EDA_Report_DB.ipynb to debug execution.

# Cell 2: Import required libraries
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import psycopg2
from sqlalchemy import create_engine
import warnings

warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 100)
plt.style.use('default')
sns.set_palette("husl")
plt.rcParams['figure.figsize'] = (12, 8)
print("Cell 2: Libraries imported successfully!")

# Cell 4: Database Connection & Initial Data Overview
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'datamine_v2_db',
    'user': 'ahs_user',
    'password': 'ahs_password'
}
try:
    engine = create_engine(f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    print("Cell 4: Database connection established!")
except Exception as e:
    print(f"Cell 4: ERROR - {e}")
    exit()

# Cell 5: Get dataset overview
try:
    overview_query = """
    SELECT 
        device_id,
        CASE WHEN device_id LIKE '%775g%' THEN '775G (Broken Load Sensors)' 
             ELSE '605 (Working Load Sensors)' END as truck_type,
        COUNT(*) as total_records,
        MIN(timestamp) as earliest_record,
        MAX(timestamp) as latest_record,
        COUNT(DISTINCT DATE(timestamp)) as operational_days
    FROM "02_raw_telemetry_transformed" 
    GROUP BY device_id, truck_type
    ORDER BY truck_type, total_records DESC;
    """
    overview_df = pd.read_sql_query(overview_query, engine)
    print("Cell 5: Dataset overview query successful.")
    print(overview_df)
except Exception as e:
    print(f"Cell 5: ERROR - {e}")
    exit()

# Cell 7: Table Schema and Data Types Analysis
try:
    schema_query = """
    SELECT 
        column_name,
        data_type,
        is_nullable,
        column_default
    FROM information_schema.columns 
    WHERE table_name = '02_raw_telemetry_transformed' 
        AND table_schema = 'public'
    ORDER BY ordinal_position;
    """
    schema_df = pd.read_sql_query(schema_query, engine)
    print("Cell 7: Schema query successful.")
    print(schema_df)
except Exception as e:
    print(f"Cell 7: ERROR - {e}")
    exit()

# Cell 9: Data Quality Assessment
try:
    quality_query = """
    SELECT 
        COUNT(*) as total_records,
        COUNT(device_id) as device_id_count,
        COUNT(timestamp) as timestamp_count,
        COUNT(system_engaged) as system_engaged_count,
        COUNT(parking_brake_applied) as parking_brake_count,
        COUNT(current_position) as position_count,
        COUNT(current_speed) as speed_count,
        COUNT(load_weight) as load_weight_count,
        COUNT(state) as state_count,
        COUNT(software_state) as software_state_count,
        COUNT(prndl) as prndl_count,
        COUNT(extras) as extras_count
    FROM "02_raw_telemetry_transformed";
    """
    quality_df = pd.read_sql_query(quality_query, engine)
    print("Cell 9: Data quality query successful.")
    print(quality_df)
except Exception as e:
    print(f"Cell 9: ERROR - {e}")
    exit()

# Cell 10: Data quality by truck type
try:
    quality_by_type_query = """
    SELECT 
        CASE WHEN device_id LIKE '%775g%' THEN '775G (Broken Load Sensors)' 
             ELSE '605 (Working Load Sensors)' END as truck_type,
        COUNT(*) as total_records,
        COUNT(load_weight) as load_weight_count,
        ROUND(COUNT(load_weight)::numeric / COUNT(*)::numeric * 100, 2) as load_weight_completeness,
        COUNT(CASE WHEN load_weight > 0 THEN 1 END) as positive_load_weight,
        ROUND(AVG(load_weight), 2) as avg_load_weight,
        ROUND(STDDEV(load_weight), 2) as stddev_load_weight,
        MIN(load_weight) as min_load_weight,
        MAX(load_weight) as max_load_weight
    FROM "02_raw_telemetry_transformed" 
    GROUP BY truck_type
    ORDER BY truck_type;
    """
    quality_by_type_df = pd.read_sql_query(quality_by_type_query, engine)
    print("Cell 10: Quality by truck type query successful.")
    print(quality_by_type_df)
except Exception as e:
    print(f"Cell 10: ERROR - {e}")
    exit()

# Cell 12: State Distribution Analysis
try:
    state_analysis_query = """
    SELECT 
        state,
        COUNT(*) as total_count,
        ROUND(COUNT(*)::numeric / (SELECT COUNT(*) FROM "02_raw_telemetry_transformed")::numeric * 100, 2) as percentage,
        COUNT(CASE WHEN device_id LIKE '%605%' THEN 1 END) as count_605,
        COUNT(CASE WHEN device_id LIKE '%775g%' THEN 1 END) as count_775g
    FROM "02_raw_telemetry_transformed" 
    GROUP BY state 
    ORDER BY total_count DESC;
    """
    state_df = pd.read_sql_query(state_analysis_query, engine)
    print("Cell 12: State distribution query successful.")
    print(state_df)
except Exception as e:
    print(f"Cell 12: ERROR - {e}")
    exit()

# Cell 14: Load Weight Analysis
try:
    load_weight_query = """
    SELECT 
        device_id,
        CASE WHEN device_id LIKE '%775g%' THEN '775G' ELSE '605' END as truck_series,
        COUNT(*) as total_records,
        COUNT(load_weight) as load_weight_records,
        COUNT(CASE WHEN load_weight IS NOT NULL AND load_weight > 0 THEN 1 END) as positive_weight_records,
        ROUND(AVG(load_weight), 2) as avg_load_weight,
        ROUND(STDDEV(load_weight), 2) as stddev_load_weight,
        MIN(load_weight) as min_load_weight,
        MAX(load_weight) as max_load_weight,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY load_weight) as q25_load_weight,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY load_weight) as median_load_weight,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY load_weight) as q75_load_weight,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY load_weight) as q95_load_weight
    FROM "02_raw_telemetry_transformed" 
    GROUP BY device_id, truck_series
    ORDER BY truck_series, device_id;
    """
    load_weight_df = pd.read_sql_query(load_weight_query, engine)
    print("Cell 14: Load weight analysis query successful.")
    print(load_weight_df)
except Exception as e:
    print(f"Cell 14: ERROR - {e}")
    exit()

print("Script finished successfully.")
