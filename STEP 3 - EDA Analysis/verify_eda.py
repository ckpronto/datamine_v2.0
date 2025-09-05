
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine, text
import warnings

# Setup
warnings.filterwarnings('ignore')
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 100)
print("---> Libraries imported ---")

# Database Connection
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
        print("---> Database connection successful ---")
except Exception as e:
    print(f"---> Database connection failed: {e} ---")
    exit()

# Run Queries
try:
    overview_query = text("SELECT device_id, CASE WHEN device_id LIKE '%775g%' THEN '775G' ELSE '605' END as truck_type, COUNT(*) as record_count FROM \"02_raw_telemetry_transformed\" GROUP BY device_id, truck_type ORDER BY truck_type, record_count DESC;")
    pd.read_sql_query(overview_query, engine)
    print("---> Query 1 (Overview) successful ---")

    quality_by_type_query = text("SELECT CASE WHEN device_id LIKE '%775g%' THEN '775G' ELSE '605' END as truck_type, COUNT(load_weight) as load_weight_count, ROUND((COUNT(load_weight) * 100.0 / COUNT(*))::numeric, 2) as completeness FROM \"02_raw_telemetry_transformed\" GROUP BY truck_type;")
    pd.read_sql_query(quality_by_type_query, engine)
    print("---> Query 2 (Quality by Type) successful ---")

    print("\n---> VERIFICATION COMPLETE: All Python code and SQL queries are valid. ---")

except Exception as e:
    print(f"---> An error occurred during query execution: {e} ---")
