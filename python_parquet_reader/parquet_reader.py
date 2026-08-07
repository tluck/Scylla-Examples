#!/usr/bin/env python3

import pandas as pd
import pyarrow.parquet as pq

# Read parquet file into a pandas DataFrame
df = pd.read_parquet('input.parquet')

# Show all columns without truncation
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)  # No limit on column width

column_names = list(df.columns)
print("File has", len(df), "rows and", len(column_names), "columns:")
print("The columns are:")
print(column_names)

# Iterate over all rows by index
n=0
for row in range(len(df)):
    row_key = df.at[row, column_names[0]]  # Assuming the first column is the row key
    
    for column in column_names[1:]:  # Skip the first column if it's a key
        nested_cell = df.at[row, column]
        if nested_cell is None:
            continue
        
        # Iterate nested structure
        for qualifier_entry in nested_cell['column']:
            qualifier = qualifier_entry['name']
            for cell in qualifier_entry['cell']:
                timestamp = cell['timestamp']
                raw_value = cell['value']
                n += 1
                print(f"row_key: {row_key}, column: {column}, Qualifier: {qualifier}, Timestamp: {timestamp}, Raw Value: {raw_value.hex()}")

print("Total cells processed:", n)

#print(df[column].iloc[[0]])
#print(df.iloc[[0]])

df.to_csv('output_file.csv', index=False)
# Display the DataFrame
#print(df)

## Read only the schema (does not load data)
