#!/usr/bin/env python3

import json
import base64
import pandas as pd
import time
import datetime
import random
import argparse
from collections import defaultdict
import struct
from datetime import datetime
import logging
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from cassandra.auth import PlainTextAuthProvider
from cassandra import ConsistencyLevel
from cassandra.concurrent import execute_concurrent_with_args
import gc
import os
from typing import Iterator, List, Dict, Any, Optional

## Script args and Help
parser = argparse.ArgumentParser(add_help=True)
parser.add_argument('-s', action="store", dest="SCYLLA_IP", default="127.0.0.1")
parser.add_argument('-u', action="store", dest="USERNAME", default="cassandra")
parser.add_argument('-p', action="store", dest="PASSWORD", default="cassandra")
parser.add_argument('-x', action="store_true", dest="EXPORT_CSV", help='Export to csv file')
parser.add_argument('-f', '--file', type=str, default="input.json", help='Path to the input JSON file')
parser.add_argument('-M', '--mode', type=int, default=0, help='compression type')
parser.add_argument('-c', '--chunk-size', type=int, default=1000, help='Number of records to process in each chunk')
parser.add_argument('-b', '--batch-size', type=int, default=100, help='Number of records to insert in each batch')
parser.add_argument('-m', '--max-memory', type=int, default=500, help='Maximum memory usage in MB before forcing garbage collection')
parser.add_argument('-i', '--progress-interval', type=int, default=1000, help='Show progress every N records')
opts = parser.parse_args()

SCYLLA_IP = opts.SCYLLA_IP.split(',')
USERNAME = opts.USERNAME
PASSWORD = opts.PASSWORD
FILENAME = opts.file
EXPORT_CSV = opts.EXPORT_CSV
MODE = opts.mode
CHUNK_SIZE = opts.chunk_size
BATCH_SIZE = opts.batch_size
MAX_MEMORY_MB = opts.max_memory
PROGRESS_INTERVAL = opts.progress_interval

## Define KS + Table
session = ""
keyspace = "moloco"
tablets = "true"

# mode=1
tables = ["table_w_zstd", "table_w_lz4c", "table_w_none"]
table = tables[MODE]
cql = f"""INSERT INTO {keyspace}.{table} (row_key, family, qualifier, timestamp, timestamp_micros, value_b64) VALUES (?,?,?,?,?,?) """

compression = ["'sstable_compression': 'ZstdCompressor'",
               "'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'",
               ""]

print(f"ScyllaDB IPs: {SCYLLA_IP}", f"Username: {USERNAME}", f"Password: {PASSWORD}")
print(f"Chunk size: {CHUNK_SIZE}, Batch size: {BATCH_SIZE}")
print(f"Filename: {FILENAME}, Export CSV: {EXPORT_CSV}")
print(f"Compression Mode: {compression[MODE]}")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class StreamingBigtableProcessor:
    """
    Streaming approach: Process large Bigtable JSON files in chunks without loading everything into memory
    """

    def __init__(self, json_file_path: str, chunk_size: int = CHUNK_SIZE, batch_size: int = BATCH_SIZE):
        self.json_file_path = json_file_path
        self.chunk_size = chunk_size
        self.batch_size = batch_size
        self.processed_count = 0
        self.error_count = 0
        self.insert_batch = []

        # Statistics tracking
        self.stats = {
            'total_records': 0,
            'total_cells': 0,
            'family_counts': defaultdict(int),
            'qualifier_counts': defaultdict(int),
            'errors': []
        }

    def get_file_size(self) -> int:
        """Get file size for progress tracking"""
        return os.path.getsize(self.json_file_path)

    def check_memory_usage(self):
        """Check memory usage and force garbage collection if needed"""
        import psutil
        process = psutil.Process(os.getpid())
        memory_mb = process.memory_info().rss / 1024 / 1024

        if memory_mb > MAX_MEMORY_MB:
            logger.info(f"Memory usage ({memory_mb:.1f} MB) exceeds limit, forcing garbage collection")
            gc.collect()

    def stream_json_records(self) -> Iterator[Dict[str, Any]]:
        """Stream JSON records one at a time"""
        try:
            file_size = self.get_file_size()
            bytes_read = 0

            with open(self.json_file_path, 'r', encoding='utf-8') as file:
                for line_num, line in enumerate(file, 1):
                    bytes_read += len(line.encode('utf-8'))
                    line = line.strip()

                    if line:
                        try:
                            record = json.loads(line)
                            record['_line_num'] = line_num
                            record['_progress'] = (bytes_read / file_size) * 100
                            yield record
                        except json.JSONDecodeError as e:
                            error_msg = f"Malformed JSON on line {line_num}: {e}"
                            logger.warning(error_msg)
                            self.stats['errors'].append(error_msg)
                            self.error_count += 1
                            continue

                    # Show progress periodically
                    if line_num % PROGRESS_INTERVAL == 0:
                        progress = (bytes_read / file_size) * 100
                        logger.info(f"Progress: {progress:.1f}% - Processed {line_num} lines")
                        self.check_memory_usage()

        except FileNotFoundError:
            logger.error(f"File not found: {self.json_file_path}")
            raise
        except Exception as e:
            logger.error(f"Error streaming file: {e}")
            raise

    def decode_base64_value(self, value_b64: str) -> Any:
        """Decode base64 encoded values"""
        try:
            decoded_bytes = base64.b64decode(value_b64)

            # Try to decode as different data types
            if len(decoded_bytes) == 8:
                try:
                    value = struct.unpack('>Q', decoded_bytes)[0]
                    return value
                except:
                    pass

            # Try as string
            try:
                return decoded_bytes.decode('utf-8')
            except UnicodeDecodeError:
                return decoded_bytes.hex()

        except Exception as e:
            logger.warning(f"Failed to decode base64 value: {e}")
            return value_b64

    def process_record_cells(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Process cells from a single record and return rows for database insertion"""
        rows = []
        row_key = record.get('row_key', f'unknown_{record.get("_line_num", 0)}')
        cells = record.get('cells', [])

        self.stats['total_cells'] += len(cells)

        for cell in cells:
            family = cell.get('family', 'unknown')
            qualifier = cell.get('qual', 'unknown')
            timestamp = cell.get('ts_micros', 0)
            value_b64 = cell.get('value_b64', '')

            # Update statistics
            self.stats['family_counts'][family] += 1
            self.stats['qualifier_counts'][qualifier] += 1

            # Convert timestamp from microseconds to datetime
            timestamp_dt = datetime.fromtimestamp(timestamp / 1000000) if timestamp > 0 else None

            rows.append({
                'row_key': row_key,
                'family': family,
                'qualifier': qualifier,
                'timestamp': timestamp_dt,
                'timestamp_micros': timestamp,
                'value_b64': value_b64
            })

        return rows

    def execute_batch_insert(self):
        """Execute batch insert to database"""
        if not self.insert_batch:
            return

        try:
            cql_prepared = session.prepare(cql)
            cql_prepared.consistency_level = ConsistencyLevel.TWO

            # Prepare batch data
            batch_data = []
            for row in self.insert_batch:
                batch_data.append((
                    row['row_key'],
                    row['family'], 
                    row['qualifier'],
                    row['timestamp'],
                    row['timestamp_micros'],
                    row['value_b64']
                ))

            # Execute concurrent batch
            execute_concurrent_with_args(
                session, 
                cql_prepared, 
                batch_data,
                concurrency=50,
                raise_on_first_error=False
            )

            logger.debug(f"Inserted batch of {len(self.insert_batch)} rows")
            self.insert_batch.clear()

        except Exception as e:
            logger.error(f"Batch insert failed: {e}")
            # Don't clear the batch on error - could implement retry logic here
            raise

    def stream_process_and_insert(self):
        """Main streaming processing function"""
        logger.info(f"Starting streaming processing of {self.json_file_path}")
        start_time = time.time()

        try:
            for record in self.stream_json_records():
                try:
                    # Process record and get database rows
                    db_rows = self.process_record_cells(record)

                    # Add to batch
                    self.insert_batch.extend(db_rows)

                    # Execute batch when it reaches batch_size
                    if len(self.insert_batch) >= self.batch_size:
                        self.execute_batch_insert()

                    self.processed_count += 1
                    self.stats['total_records'] += 1

                    # Progress reporting
                    if self.processed_count % PROGRESS_INTERVAL == 0:
                        elapsed = time.time() - start_time
                        rate = self.processed_count / elapsed
                        progress = record.get('_progress', 0)
                        logger.info(f"Processed {self.processed_count} records ({rate:.1f} records/sec) - {progress:.1f}% complete")

                except Exception as e:
                    logger.error(f"Error processing record {self.processed_count}: {e}")
                    self.error_count += 1
                    continue

            # Execute final batch
            if self.insert_batch:
                self.execute_batch_insert()

            # Final statistics
            elapsed = time.time() - start_time
            logger.info(f"Streaming processing complete!")
            logger.info(f"Total records processed: {self.processed_count}")
            logger.info(f"Total cells processed: {self.stats['total_cells']}")
            logger.info(f"Total errors: {self.error_count}")
            logger.info(f"Total time: {elapsed:.2f} seconds")
            logger.info(f"Average rate: {self.processed_count / elapsed:.2f} records/sec")

        except KeyboardInterrupt:
            logger.info("Processing interrupted by user")
            # Execute final batch before exiting
            if self.insert_batch:
                self.execute_batch_insert()
            raise
        except Exception as e:
            logger.error(f"Streaming processing failed: {e}")
            raise

    def generate_streaming_analysis_report(self, output_file: str = 'streaming_analysis_report.txt'):
        """Generate analysis report from collected statistics"""
        logger.info("Generating analysis report...")

        try:
            with open(output_file, 'w') as f:
                f.write("STREAMING BIGTABLE DATA ANALYSIS REPORT\n")
                f.write("=" * 60 + "\n\n")

                f.write(f"Processing Summary:\n")
                f.write(f"Total Records Processed: {self.stats['total_records']}\n")
                f.write(f"Total Cells Processed: {self.stats['total_cells']}\n")
                f.write(f"Processing Errors: {self.error_count}\n")

                if self.stats['total_records'] > 0:
                    avg_cells = self.stats['total_cells'] / self.stats['total_records']
                    f.write(f"Average Cells per Record: {avg_cells:.2f}\n\n")

                f.write("COLUMN FAMILY DISTRIBUTION:\n")
                f.write("-" * 40 + "\n")
                sorted_families = sorted(self.stats['family_counts'].items(), 
                                       key=lambda x: x[1], reverse=True)
                for family, count in sorted_families:
                    f.write(f"{family}: {count} cells\n")

                f.write("\nTOP 20 QUALIFIERS:\n")
                f.write("-" * 40 + "\n")
                sorted_qualifiers = sorted(self.stats['qualifier_counts'].items(), 
                                         key=lambda x: x[1], reverse=True)[:20]
                for qual, count in sorted_qualifiers:
                    f.write(f"{qual}: {count} occurrences\n")

                if self.stats['errors']:
                    f.write("\nERRORS ENCOUNTERED:\n")
                    f.write("-" * 40 + "\n")
                    for error in self.stats['errors'][:50]:  # Limit to first 50 errors
                        f.write(f"{error}\n")

            logger.info(f"Analysis report saved to {output_file}")

        except Exception as e:
            logger.error(f"Failed to generate analysis report: {e}")

    def export_sample_to_csv(self, sample_size: int = 10000, output_file: str = 'sample_data.csv'):

        """Export a sample of processed data to CSV for verification"""
        logger.info(f"Exporting sample of {sample_size} records to CSV...")

        sample_rows = []
        count = 0

        try:
            for record in self.stream_json_records():
                if count >= sample_size:
                    break

                db_rows = self.process_record_cells(record)
                sample_rows.extend(db_rows)
                count += 1

            # Convert to DataFrame and save
            df = pd.DataFrame(sample_rows)
            df.to_csv(output_file, index=False)
            logger.info(f"Sample data exported to {output_file} ({len(sample_rows)} rows)")

        except Exception as e:
            logger.error(f"Failed to export sample data: {e}")

def main():
    """Main execution function with streaming processing"""
    json_file = FILENAME # 'prod_revised.json'

    # Initialize streaming processor
    processor = StreamingBigtableProcessor(json_file, CHUNK_SIZE, BATCH_SIZE)

    try:
        logger.info("Starting streaming Bigtable data processing...")

        # Main streaming processing
        processor.stream_process_and_insert()

        # Generate analysis report
        processor.generate_streaming_analysis_report()

        # Export sample data for verification

        if EXPORT_CSV == True:
            output_file = FILENAME.replace('.json', '.csv')
            processor.export_sample_to_csv(sample_size=5000, output_file=output_file)

        print("\n" + "="*60)
        print("STREAMING PROCESSING COMPLETE!")
        print("="*60)
        print(f"Total Records: {processor.stats['total_records']}")
        print(f"Total Cells: {processor.stats['total_cells']}")
        print(f"Processing Errors: {processor.error_count}")

        print(f"\nTop Column Families:")
        sorted_families = sorted(processor.stats['family_counts'].items(), 
                               key=lambda x: x[1], reverse=True)[:5]
        for family, count in sorted_families:
            print(f"  {family}: {count} cells")

        print("\nGenerated Files:")
        print("- streaming_analysis_report.txt: Detailed analysis")
        if EXPORT_CSV == True:
            print(f"- {FILENAME.replace('.json', '.csv')}: Sample extracted data for verification")

    except Exception as e:
        logger.error(f"Streaming processing failed: {e}")
        raise

def getCluster():
    profile = ExecutionProfile(load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc='GCE_US_WEST_1')))

    return Cluster(
        execution_profiles={EXEC_PROFILE_DEFAULT: profile},
        contact_points=[
            "node-0.gce-us-west-1.458b377b0896c66a4356.clusters.scylla.cloud", "node-1.gce-us-west-1.458b377b0896c66a4356.clusters.scylla.cloud", "node-2.gce-us-west-1.458b377b0896c66a4356.clusters.scylla.cloud"
        ],
        port=9042,
        auth_provider = PlainTextAuthProvider(username=USERNAME, password=PASSWORD))

    # cluster = Cluster(
    # SCYLLA_IP,
    # auth_provider=auth_provider,
    # # Optimize connection pool
    # max_connections_per_host=25,
    # # Increase concurrent requests
    # default_retry_policy=RetryPolicy(),
    # # Connection timeouts
    # connect_timeout=30,
    # control_connection_timeout=30)

if __name__ == "__main__":

    print('Connecting to cluster')
    cluster = Cluster(SCYLLA_IP, auth_provider=PlainTextAuthProvider(username=USERNAME, password=PASSWORD))
    #cluster = getCluster()
    session = cluster.connect()
    
    t = tables[MODE]
    c = compression[MODE]

    create_ks = f"""
    CREATE KEYSPACE IF NOT EXISTS {keyspace}
      WITH replication = {{'class' : 'org.apache.cassandra.locator.NetworkTopologyStrategy', 'replication_factor' : 3}}
      AND tablets = {{'enabled': {tablets} }};
      """

    create_table = f"""CREATE TABLE IF NOT EXISTS {keyspace}.{t}
      (row_key text, family text, qualifier text, timestamp date, timestamp_micros timestamp, value_b64 text,
      PRIMARY KEY (row_key, family, timestamp_micros, qualifier))
      WITH compression = {{ {c} }};
      """

    session.execute(create_ks)
    session.execute(f"""DROP TABLE if exists {keyspace}.{t};""")
    session.execute(create_table)

    main()

    session.shutdown()

