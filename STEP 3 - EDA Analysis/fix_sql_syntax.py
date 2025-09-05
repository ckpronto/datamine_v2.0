"""
Fix SQL syntax errors in the notebook - PostgreSQL uses single quotes for strings
"""

import json
import re

notebook_path = "/home/ck/DataMine/APPLICATION/V2/STEP 3 - EDA Analysis/01_EDA_Report_DB.ipynb"

with open(notebook_path, 'r') as f:
    notebook = json.load(f)

# Fix SQL syntax in all cells
fixed_count = 0
for i, cell in enumerate(notebook['cells']):
    if cell['cell_type'] == 'code':
        source = ''.join(cell['source'])
        original_source = source
        
        # Fix double quotes in SQL strings - convert to single quotes
        # But be careful not to break Python strings
        if 'SELECT' in source or 'FROM' in source:
            # Replace double quotes with single quotes in SQL LIKE patterns
            source = re.sub(r'LIKE\s*"([^"]*)"', r"LIKE '\1'", source)
            # Replace double quotes around string literals in SQL
            source = re.sub(r'THEN\s*"([^"]*)"', r"THEN '\1'", source)
            source = re.sub(r'ELSE\s*"([^"]*)"', r"ELSE '\1'", source)
            # Fix other common SQL string patterns
            source = re.sub(r"=\s*\"([^\"]*?)\"", r"= '\1'", source)
            source = re.sub(r"<>\s*\"([^\"]*?)\"", r"<> '\1'", source)
            
            if source != original_source:
                cell['source'] = source.split('\n')
                fixed_count += 1
                print(f"Fixed SQL syntax in cell {i}")

print(f"Total cells with SQL fixes: {fixed_count}")

# Save the updated notebook
with open(notebook_path, 'w') as f:
    json.dump(notebook, f, indent=2)

print("SQL syntax fixed!")
print("PostgreSQL queries now use proper single quotes for strings.")