"""
Fix script to address variable dependency issues in the EDA notebook
"""

import json

notebook_path = "/home/ck/DataMine/APPLICATION/V2/STEP 3 - EDA Analysis/01_EDA_Report_DB.ipynb"

with open(notebook_path, 'r') as f:
    notebook = json.load(f)

# Find and fix specific problematic cells
for i, cell in enumerate(notebook['cells']):
    if cell['cell_type'] == 'code':
        source_text = ''.join(cell['source'])
        
        # Fix cell that uses individual_enums without proper initialization
        if 'individual_enums.items()' in source_text and 'individual_enums = {}' not in source_text:
            print(f"Fixing individual_enums dependency in cell {i}")
            # Add safety check
            new_source = """# Visualize ENUM distributions (with safety check)
fig, axes = plt.subplots(3, 1, figsize=(12, 15))

# Safety check for individual_enums
if 'individual_enums' in locals():
    for i, (enum_name, enum_data) in enumerate(individual_enums.items()):
        enum_data.plot(x=enum_name, y='percentage', kind='bar', ax=axes[i])
        axes[i].set_title(f'{enum_name.upper()} Distribution (% of Total Records)')
        axes[i].set_xlabel(enum_name.replace('_', ' ').title())
        axes[i].set_ylabel('Percentage (%)')
        axes[i].tick_params(axis='x', rotation=45)
        axes[i].grid(True, alpha=0.3)
else:
    print("Individual enum data not available. Please run the previous ENUM analysis cell first.")
    
plt.tight_layout()
plt.show()"""
            cell['source'] = new_source.split('\n')
            
        # Fix cell that uses daily_summary without proper initialization  
        elif 'daily_summary.plot' in source_text and 'daily_summary =' not in source_text:
            print(f"Fixing daily_summary dependency in cell {i}")
            new_source = """# Visualize temporal patterns (with safety check)
fig, axes = plt.subplots(2, 2, figsize=(15, 12))

# Safety check for required variables
if 'daily_summary' in locals() and 'hourly_summary' in locals():
    # Daily records over time
    daily_summary.plot(y='record_count', ax=axes[0,0], kind='line')
    axes[0,0].set_title('Daily Record Count Over Time')
    axes[0,0].set_xlabel('Date')
    axes[0,0].set_ylabel('Records per Day')
    axes[0,0].tick_params(axis='x', rotation=45)

    # Hourly activity pattern
    hourly_summary.plot(y='record_count', ax=axes[0,1], kind='bar')
    axes[0,1].set_title('Average Hourly Activity Pattern')
    axes[0,1].set_xlabel('Hour of Day')
    axes[0,1].set_ylabel('Average Records per Hour')

    # Daily average speed
    daily_summary.plot(y='avg_speed', ax=axes[1,0], kind='line', color='orange')
    axes[1,0].set_title('Daily Average Speed')
    axes[1,0].set_xlabel('Date')
    axes[1,0].set_ylabel('Average Speed (m/s)')
    axes[1,0].tick_params(axis='x', rotation=45)

    # Daily active devices
    daily_summary.plot(y='active_devices', ax=axes[1,1], kind='line', color='green')
    axes[1,1].set_title('Daily Active Devices')
    axes[1,1].set_xlabel('Date')
    axes[1,1].set_ylabel('Number of Active Devices')
    axes[1,1].tick_params(axis='x', rotation=45)
else:
    print("Temporal analysis data not available. Please run the previous temporal patterns cell first.")

plt.tight_layout()
plt.show()"""
            cell['source'] = new_source.split('\n')
            
        # Fix summary cell that uses undefined variables
        elif 'quality_issues_df[quality_issues_df' in source_text:
            print(f"Fixing quality_issues_df dependency in cell {i}")
            # Add safety checks to the summary cell
            original_source = ''.join(cell['source'])
            
            # Insert safety checks for key variables
            safety_checks = '''# Safety checks for required variables
required_vars = ['overview_df', 'quality_issues_df', 'load_weight_df', 'state_df', 'speed_df']
missing_vars = [var for var in required_vars if var not in locals()]

if missing_vars:
    print(f"Missing required variables: {missing_vars}")
    print("Please run the previous analysis cells to generate the required data.")
else:
    # Original summary code follows
'''
            
            new_source = safety_checks + '    ' + original_source.replace('\n', '\n    ')
            cell['source'] = new_source.split('\n')

# Save the updated notebook
with open(notebook_path, 'w') as f:
    json.dump(notebook, f, indent=2)

print("Notebook dependencies fixed!")
print("Added safety checks to prevent NameError issues.")
print("The notebook will now handle missing variables gracefully.")