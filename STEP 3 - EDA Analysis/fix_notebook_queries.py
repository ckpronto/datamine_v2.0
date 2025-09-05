"""
Quick fix script to update all pd.read_sql_query calls in the notebook
to use SQLAlchemy 2.0 compatible text() function
"""

import json
import re

# Read the notebook
notebook_path = "/home/ck/DataMine/APPLICATION/V2/STEP 3 - EDA Analysis/01_EDA_Report_DB.ipynb"

with open(notebook_path, 'r') as f:
    notebook = json.load(f)

# Pattern to find pd.read_sql_query calls without text()
pattern = r'pd\.read_sql_query\(([^,]+),\s*engine\)'
replacement = r'pd.read_sql_query(text(\1), engine)'

# Update cells
updated_count = 0
for cell in notebook['cells']:
    if cell['cell_type'] == 'code':
        source = ''.join(cell['source'])
        if 'pd.read_sql_query' in source and 'text(' not in source:
            # Fix the source
            updated_source = re.sub(pattern, replacement, source)
            if updated_source != source:
                cell['source'] = updated_source.split('\n')
                updated_count += 1
                print(f"Updated cell with pd.read_sql_query")

print(f"Total cells updated: {updated_count}")

# Save the updated notebook
with open(notebook_path, 'w') as f:
    json.dump(notebook, f, indent=2)

print("Notebook updated successfully!")
print("You can now run the notebook without SQLAlchemy compatibility issues.")