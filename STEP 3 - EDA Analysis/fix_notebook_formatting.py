"""
Fix formatting issues in the notebook where line breaks were lost
"""

import json
import re

notebook_path = "/home/ck/DataMine/APPLICATION/V2/STEP 3 - EDA Analysis/01_EDA_Report_DB.ipynb"

with open(notebook_path, 'r') as f:
    notebook = json.load(f)

# Fix cells with formatting issues
fixed_count = 0
for i, cell in enumerate(notebook['cells']):
    if cell['cell_type'] == 'code':
        source_text = ''.join(cell['source'])
        original_length = len(source_text)
        
        # Check if this cell has lost line breaks (very long single lines)
        lines = source_text.split('\n')
        needs_fixing = False
        
        for line in lines:
            # If any line is extremely long and contains SQL keywords, it needs fixing
            if len(line) > 200 and ('SELECT' in line or 'FROM' in line):
                needs_fixing = True
                break
        
        if needs_fixing:
            print(f"Fixing formatting in cell {i}")
            
            # Fix common SQL formatting issues
            fixed_source = source_text
            
            # Add line breaks after SQL keywords
            fixed_source = re.sub(r'SELECT\s+', 'SELECT \n    ', fixed_source)
            fixed_source = re.sub(r'FROM\s+"', 'FROM "', fixed_source)
            fixed_source = re.sub(r'WHERE\s+', '\nWHERE ', fixed_source)
            fixed_source = re.sub(r'GROUP BY\s+', '\nGROUP BY ', fixed_source)
            fixed_source = re.sub(r'ORDER BY\s+', '\nORDER BY ', fixed_source)
            fixed_source = re.sub(r'UNION ALL\s*SELECT', '\n\nUNION ALL\n\nSELECT', fixed_source)
            
            # Fix Python code formatting
            fixed_source = re.sub(r'"""([^"]+)"""([a-zA-Z_])', r'"""\1"""\n\n\2', fixed_source)
            fixed_source = re.sub(r'engine\)([a-zA-Z_])', r'engine)\n\2', fixed_source)
            fixed_source = re.sub(r'print\(([^)]+)\)([a-zA-Z_])', r'print(\1)\n\2', fixed_source)
            
            # Split into proper lines
            cell['source'] = fixed_source.split('\n')
            fixed_count += 1

print(f"Fixed formatting in {fixed_count} cells")

# Save the updated notebook
with open(notebook_path, 'w') as f:
    json.dump(notebook, f, indent=2)

print("Notebook formatting restored!")