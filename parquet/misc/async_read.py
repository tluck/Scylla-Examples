#!/usr/bin/env python3
import json
import base64
import struct
import logging
from datetime import datetime
from multiprocessing import Pool, cpu_count
import asyncio

from scyllapy import Scylla, Batch   # pip install scyllapy
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Config
USERNAME = "cassandra"
PASSWORD = "cassandra"
SCYLLA_IP = ["127.0.0.1:9042"]
SCYLLA_HOST =["127.0.0.1"]
KEYSPACE = "moloco"
TABLE = "table_w_zstd"
CQL = f"INSERT INTO {KEYSPACE}.{TABLE} (row_key, family, qualifier, timestamp, timestamp_micros, value_b64) VALUES (?, ?, ?, ?, ?, ?)"

BATCH_SIZE = 2
CHUNK_SIZE = 2

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

def stream_json_records(json_file_path):
    """Memory efficient streaming of records."""
    with open(json_file_path, 'r', encoding='utf-8') as file:
        for line_num, line in enumerate(file, 1):
            line = line.strip()
            if line:
                try:
                    yield json.loads(line)
                except Exception as e:
                    logger.warning(f"Skipping malformed JSON at line {line_num}: {e}")

def process_single_record(record):
    # Simple field extraction for DB write
    row_key = record.get('row_key', 'unknown')
    cells = record.get('cells', [])
    processed = []
    for cell in cells:
        family = cell.get('family', 'unknown')
        qualifier = cell.get('qual', 'unknown')
        timestamp = cell.get('ts_micros', 0)
        timestamp_dt = "2025-09-01T00:00:00Z"
        #timestamp_dt = datetime.fromtimestamp(timestamp / 1e6) if timestamp > 0 else None
        value_b64 = cell.get('value_b64', '')
        print(row_key, family, qualifier, timestamp_dt, timestamp, value_b64)
        processed.append(
            (row_key, family, qualifier, timestamp_dt, timestamp, value_b64)
        )
    return processed

def chunk_generator(json_file_path, chunk_size):
    chunk = []
    for record in stream_json_records(json_file_path):
        chunk.append(record)
        if len(chunk) >= chunk_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk
    print("Chunk processed.")

def process_chunk_for_insert(records_chunk):
    """Process a chunk into DB-ready rows."""
    result = []
    for record in records_chunk:
        result.extend(process_single_record(record))
    return result

async def scylla_etl_async(ip_addrs, keyspace, cql, batch_data, username, password):
    """Insert batch using async ScyllaPy batch API, with user/pass auth."""
    scylla = Scylla(ip_addrs, keyspace=keyspace, username=username, password=password)
    await scylla.startup()
    batch = Batch()
    # All queries identical and batch API in scyllapy supports this pattern
    batch.add_query(cql)
    await scylla.batch(batch, batch_data)
    await scylla.shutdown()

def flush_to_scylla(batch_rows):
    """Sync wrapper to launch async batch insert from multiprocessing."""
    asyncio.run(
        scylla_etl_async(
            SCYLLA_IP, KEYSPACE, CQL, batch_rows, USERNAME, PASSWORD
        )
    )

def main(json_file_path):
    # Multiprocess pool for CPU-bound extraction
    with Pool(cpu_count()) as pool:
        for records_chunk in chunk_generator(json_file_path, CHUNK_SIZE):
            # Preprocess chunk into DB parameter rows
            processed_rows = pool.apply(process_chunk_for_insert, (records_chunk,))
            # Batch and send to Scylla
            for i in range(0, len(processed_rows), BATCH_SIZE):
                batch = processed_rows[i:i + BATCH_SIZE]
                print("Flushing batch to Scylla:", i)
                flush_to_scylla(batch)   # Async insert

    logger.info("ETL processing completed.")

if __name__ == "__main__":
    create_ks = f"""CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
      WITH replication = {{'class' : 'org.apache.cassandra.locator.NetworkTopologyStrategy', 'replication_factor' : 3}}
      AND tablets = {{'enabled': 'true' }};
      """
    create_table = f"""CREATE TABLE IF NOT EXISTS {KEYSPACE}.{TABLE}
      (row_key text, family text, qualifier text, timestamp text, timestamp_micros text, value_b64 text,
      PRIMARY KEY (row_key, qualifier))
      WITH compression = {{ 'sstable_compression': 'ZstdCompressor' }};
      """
    cluster = Cluster(SCYLLA_HOST, auth_provider=PlainTextAuthProvider(username='cassandra', password='cassandra'))
    session = cluster.connect()
    session.execute(create_ks)
    print("Keyspace created.")
    session.execute(f"""DROP TABLE if exists {KEYSPACE}.{TABLE};""")
    session.execute(create_table)
    print("Table created.")
    session.shutdown()
    main("prod_revised.json")

