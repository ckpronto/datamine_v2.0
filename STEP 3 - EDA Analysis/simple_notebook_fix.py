"""
Simple fix for the most critical notebook formatting issues
"""

import json

notebook_path = "/home/ck/DataMine/APPLICATION/V2/STEP 3 - EDA Analysis/01_EDA_Report_DB.ipynb"

# Read the notebook
with open(notebook_path, 'r') as f:
    notebook = json.load(f)

# Manually fix the most problematic cells that we can see from the preview
cell_fixes = {
    6: '''# Get table schema information
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

schema_df = pd.read_sql_query(text(schema_query), engine)
print("Table Schema:")
display(schema_df)

# Get enum values
enum_queries = {
    'state': "SELECT unnest(enum_range(NULL::telemetry_state_enum)) as enum_value;",
    'software_state': "SELECT unnest(enum_range(NULL::software_state_enum)) as enum_value;",
    'prndl': "SELECT unnest(enum_range(NULL::prndl_enum)) as enum_value;"
}

print("\\n=== ENUM VALUES ===")
for enum_name, query in enum_queries.items():
    try:
        enum_df = pd.read_sql_query(text(query), engine)
        print(f"\\n{enum_name.upper()} enum values:")
        print(enum_df['enum_value'].tolist())
    except Exception as e:
        print(f"Error getting {enum_name} enum values: {e}")''',
    
    8: '''# Data quality analysis - null values, completeness by column
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

quality_df = pd.read_sql_query(text(quality_query), engine)
total_records = quality_df['total_records'].iloc[0]

print("=== DATA COMPLETENESS ANALYSIS ===")
for col in quality_df.columns:
    if col != 'total_records':
        count = quality_df[col].iloc[0]
        completeness = (count / total_records) * 100
        missing = total_records - count
        print(f"{col:25}: {count:>10,} records ({completeness:>6.2f}% complete, {missing:>8,} missing)")

print(f"\\nTotal Records: {total_records:,}")'''
}

# Apply fixes
for cell_id, fixed_content in cell_fixes.items():
    if cell_id < len(notebook['cells']):
        notebook['cells'][cell_id]['source'] = fixed_content.split('\n')
        print(f"Fixed cell {cell_id}")

# Save the updated notebook  
with open(notebook_path, 'w') as f:
    json.dump(notebook, f, indent=2)

print("Critical formatting issues fixed!")
print("The notebook should now be more readable and functional.")