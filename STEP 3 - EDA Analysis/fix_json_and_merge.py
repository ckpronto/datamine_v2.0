#!/usr/bin/env python3
"""
Fix JSON formatting issues in the WORKING notebook and make it the main one
"""

import json
import re

# Read the WORKING notebook as text to fix JSON issues
with open('01_EDA_Report_DB_WORKING.ipynb', 'r') as f:
    content = f.read()

print("Original content length:", len(content))

# Find and show the problematic area around line 474
lines = content.split('\n')
print("\nLines around the error (470-480):")
for i in range(470, min(480, len(lines))):
    if i < len(lines):
        print(f"{i:3d}: {repr(lines[i])}")

# Try to fix common JSON issues
print("\nAttempting to fix JSON issues...")

# Fix unescaped quotes in JSON strings
# This is a bit tricky - we need to be careful not to break valid JSON
try:
    # First, let's try to identify what's wrong by looking at the specific error location
    char_pos = 20220
    problem_area = content[char_pos-100:char_pos+100]
    print(f"\nProblem area around character {char_pos}:")
    print(repr(problem_area))
    
    # Try a simple fix - replace the problematic character if it's an unescaped quote
    fixed_content = content[:char_pos-1] + '"' + content[char_pos:]
    
    # Test if this fixes the JSON
    test_json = json.loads(fixed_content)
    print("JSON fix successful!")
    
    # Save the corrected version
    with open('01_EDA_Report_DB.ipynb', 'w') as f:
        json.dump(test_json, f, indent=2)
    
    print("Successfully fixed and saved the notebook as 01_EDA_Report_DB.ipynb")
    print("This version contains all the important fixes from the WORKING version")
    
except Exception as e:
    print(f"Fix attempt failed: {e}")
    print("Will try a different approach...")
    
    # Alternative: try to manually reconstruct the JSON
    try:
        # Extract the main structure
        notebook_start = content.find('{')
        notebook_data = content[notebook_start:]
        
        # Try to parse what we can
        print("Attempting alternative fix...")
        
        # For now, let's use the basic notebook and manually add the key fixes
        basic_notebook = {
            "cells": [],
            "metadata": {
                "kernelspec": {
                    "display_name": "Python 3",
                    "language": "python", 
                    "name": "python3"
                }
            },
            "nbformat": 4,
            "nbformat_minor": 4
        }
        
        print("Created basic notebook structure - you'll need to manually copy the cell content")
        
    except Exception as e2:
        print(f"Alternative fix also failed: {e2}")
        print("The WORKING notebook needs manual JSON repair")