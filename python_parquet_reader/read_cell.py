#!/usr/bin/env python3

import pandas as pd

# Read parquet file
df = pd.read_parquet('input.parquet')

# Pick the first row and the column with nested data
nested_cell = df.at[0, 'e']  # change 'column_data' to your actual nested column name

# It should be a dict or array-like object representing nested cells
#print(nested_cell)

# Example to iterate nested structure
for qualifier_entry in nested_cell['column']:
    qualifier = qualifier_entry['name']
    for cell in qualifier_entry['cell']:
        timestamp = cell['timestamp']
        raw_value = cell['value']

        # Handle raw_value bytes decoding here
        print(f"Qualifier: {qualifier}, Timestamp: {timestamp}, Raw Value: {raw_value}")

