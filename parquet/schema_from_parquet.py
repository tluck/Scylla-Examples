#!/usr/bin/env python3
import pyarrow.parquet as pq

def arrow_to_cql(pa_type):
    pa_type_str = str(pa_type)
    if pa_type_str in ['int8', 'int16', 'int32']:
        return 'int'
    elif pa_type_str == 'int64':
        return 'bigint'
    elif pa_type_str == 'float32':
        return 'float'
    elif pa_type_str == 'float64':
        return 'double'
    elif pa_type_str == 'bool':
        return 'boolean'
    elif pa_type_str in ['string', 'utf8']:
        return 'text'
    elif pa_type_str == 'binary':
        return 'blob'
    elif 'timestamp' in pa_type_str:
        return 'timestamp'
    elif pa_type_str in ['date32', 'date64', 'date']:
        return 'date'
    else:
        return 'text'  # default fallback

def generate_scylla_schema(parquet_file_path, table_name, primary_key_cols):
    # Read parquet schema
    schema = pq.read_schema(parquet_file_path)
    
    # Generate columns with mapped CQL types
    columns = []
    for name, pa_type in zip(schema.names, schema.types):
        cql_type = arrow_to_cql(pa_type)
        columns.append(f"\"{name}\" {cql_type}")
    
    # Format primary key, supports composite keys
    if isinstance(primary_key_cols, str):
        primary_key_cols = [primary_key_cols]
    
    #if len(primary_key_cols) == 1:
    #    primary_key = primary_key_cols[0]
    #else:
    primary_key = "(" + ", ".join(primary_key_cols) + ")"
    
    cql = f"CREATE TABLE {table_name} (\n    " + ",\n    ".join(columns) + f",\n    PRIMARY KEY {primary_key}\n);"
    return cql

if __name__ == "__main__":
    parquet_path = "input.parquet"  # Replace with your file path
    table = "parquet"                 # Desired ScyllaDB table name
    
    # Specify your primary key columns here (at least partition key)
    primary_keys = ["rowkey"]         # Replace with your primary key columns
    
    cql_schema = generate_scylla_schema(parquet_path, table, primary_keys)
    print(cql_schema)

