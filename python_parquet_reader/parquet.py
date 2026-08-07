#!/usr/bin/env python3

import pyarrow as pa
import pyarrow.parquet as pq
import datetime

# Define the schema
schema = pa.schema([
    pa.field("user_id", pa.int64(), nullable=False),
    pa.field("name", pa.string(), nullable=False),
    pa.field("email", pa.string(), nullable=True),
    pa.field("age", pa.int32(), nullable=True),
    pa.field("signup_date", pa.date32(), nullable=True),
    pa.field("is_active", pa.bool_(), nullable=False),
    pa.field("tags", pa.list_(pa.string()), nullable=True)
])

# Example data matching the schema
data = [
    {
        "user_id": 3,
        "name": "Jim",
        "email": "alice@example.com",
        "age": 34,
        "signup_date": datetime.date(2024, 6, 17),
        "is_active": True,
        "tags": ["premium", "email_subscriber"]
    },
    {
        "user_id": 4,
        "name": "Tom",
        "email": None,
        "age": None,
        "signup_date": None,
        "is_active": False,
        "tags": []
    }
]

# Convert data to columns for pyarrow (columnar format)
def to_column(key):
    # Handles list type for 'tags'
    if key == "tags":
        return [item["tags"] if "tags" in item else None for item in data]
    return [item[key] for item in data]

table = pa.Table.from_arrays(
    [
        to_column("user_id"),
        to_column("name"),
        to_column("email"),
        to_column("age"),
        [x.toordinal() if x is not None else None for x in to_column("signup_date")],  # date32 expects ordinal
        to_column("is_active"),
        to_column("tags"),
    ],
    schema=schema
)

# Write to Parquet file
pq.write_table(table, 'users.parquet')
print("Parquet file 'users.parquet' created.")

