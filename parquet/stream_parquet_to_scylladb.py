#!/usr/bin/env python3

import pandas as pd
import pyarrow.parquet as pq
import argparse
import base64
import struct
import logging
import time
import gc
import os
from re import S
from typing import Iterator, List, Dict, Any, Optional
from collections import defaultdict
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from cassandra.auth import PlainTextAuthProvider
from cassandra import ConsistencyLevel
from cassandra.concurrent import execute_concurrent_with_args

## Script args and Help
parser = argparse.ArgumentParser(add_help=True)
parser.add_argument('-s', action="store", dest="SCYLLA_IP", default="127.0.0.1")
parser.add_argument('-u', action="store", dest="USERNAME", default="cassandra")
parser.add_argument('-p', action="store", dest="PASSWORD", default="cassandra")
parser.add_argument('-x', action="store_true", dest="EXPORT_CSV", help='Export to csv file')
parser.add_argument('-k', action="store_true", dest="DROP_KEYSPACE", help='Drop keyspace')
parser.add_argument('-f', '--file', type=str, default="input.parquet", help='Path to the input JSON file')
parser.add_argument('-M', '--mode', type=int, default=0, help='compression type')
parser.add_argument('-b', '--batch-size', type=int, default=4000, help='Number of records to insert in each batch')
parser.add_argument('-m', '--max-memory', type=int, default=1024, help='Maximum memory usage in MB before forcing garbage collection')
parser.add_argument('-i', '--progress-interval', type=int, default=50, help='Show progress every N records')
parser.add_argument('--log', default='INFO', help='Set logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)')
parser.add_argument('--dc', dest='LOCAL_DC', default='GCE_US_WEST_1', help='Local datacenter name for ScyllaDB')

opts = parser.parse_args()

SCYLLA_IP = opts.SCYLLA_IP.split(',')
USERNAME = opts.USERNAME
PASSWORD = opts.PASSWORD
FILENAME = opts.file
EXPORT_CSV = opts.EXPORT_CSV
DROP_KEYSPACE = opts.DROP_KEYSPACE
MODE = opts.mode
# CHUNK_SIZE = opts.chunk_size
BATCH_SIZE = opts.batch_size
MAX_MEMORY_MB = opts.max_memory
PROGRESS_INTERVAL = opts.progress_interval
LOCAL_DC = opts.LOCAL_DC

## Define KS + Table
session = None
tablets = True
modes = ["zdic", "zstd", "lz4c", "none"]
compression = ["'sstable_compression': 'ZstdWithDictsCompressor', 'compression_level': 9",
               "'sstable_compression': 'ZstdCompressor'",
               "'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'",
               ""]
c = compression[MODE]               

table = "table"
keyspace = f"moloco_{modes[MODE]}"
# cql = f"""INSERT INTO {keyspace}.{table} (row_key, family, qualifier, timestamp, raw_value) VALUES (?,?,?,?,?) """

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

logger.info(f"ScyllaDB IPs: {SCYLLA_IP}, Username: {USERNAME}, Password: {PASSWORD}")
logger.info(f"Using batch size: {BATCH_SIZE}")
logger.info(f"Reading from filename: {FILENAME}")
if EXPORT_CSV:
    logger.info(f"Exporting to CSV file: {FILENAME.replace('.parquet', '.csv')}")
logger.info(f"Compression mode: {compression[MODE]}")

class StreamingBigtableProcessor:
    # def __init__(self, parquet_file_path: str, chunk_size: int = CHUNK_SIZE, batch_size: int = BATCH_SIZE):
    def __init__(self, parquet_file_path: str, batch_size: int = BATCH_SIZE):
        self.parquet_file_path = parquet_file_path
        # self.chunk_size = chunk_size
        self.batch_size = batch_size
        self.family_batches = defaultdict(list)
        self.processed_count = 0
        self.error_count = 0
        self.column_names = None  # Will detect in streaming

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
        return os.path.getsize(self.parquet_file_path)

    def check_memory_usage(self):
        """Check memory usage and force garbage collection if needed"""
        import psutil
        process = psutil.Process(os.getpid())
        memory_mb = process.memory_info().rss / 1024 / 1024

        if memory_mb > MAX_MEMORY_MB:
            logger.info(f"Memory usage ({memory_mb:.1f} MB) exceeds limit, forcing garbage collection")
            gc.collect()

    def stream_parquet_records(self) -> Iterator[Dict[str, Any]]:
        try:
            parquet_file = pq.ParquetFile(self.parquet_file_path)
            total_row_groups = parquet_file.num_row_groups
            total_rows = parquet_file.metadata.num_rows
            logger.info(f"Parquet file has {total_rows} rows in {total_row_groups} row groups")

            count = 0
            for rg_idx in range(total_row_groups):
                row_group_table = parquet_file.read_row_group(rg_idx)
                df = row_group_table.to_pandas()

                # Set column_names once on first batch
                if self.column_names is None:
                    self.column_names = list(df.columns)
                    logger.info(f"Detected columns: {self.column_names}")
                    create_tables_by_column_family(self.column_names)

                for idx, row in df.iterrows():
                    record = row.to_dict()
                    record['_progress'] = 100 * count / total_rows if total_rows else None
                    record['_line_num'] = count
                    yield record
                    count += 1

                    if count % PROGRESS_INTERVAL == 0:
                        self.check_memory_usage()

            logger.debug(f"stream_parquet_records: Completed streaming {count} records from Parquet file")
        except FileNotFoundError:
            logger.error(f"File not found: {self.parquet_file_path}")
            raise
        except Exception as e:
            logger.error(f"Error streaming Parquet file: {e}")
            raise

    def decode_base64_value(self, raw_value: str) -> Any:
        """Decode base64 encoded values"""
        try:
            decoded_bytes = base64.b64decode(raw_value)

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
            return raw_value

    def process_record_cells(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        rows = []
        if self.column_names is None:
            raise ValueError("Column names are not initialized")  # Defensive

        row_key = record.get(self.column_names[0], f'unknown_{record.get("_line_num", 0)}')
        logger.debug("Processing row_key: %s", row_key)
        # For each family column
        families_in_row = set()  # Track families found in this row
        for family in self.column_names[1:]:
            nested_cell = record.get(family, None)
            if nested_cell is None:
                continue
            families_in_row.add(family)  # Add family to set
            try:
                for qualifier_entry in nested_cell.get('column', []):
                    qualifier = qualifier_entry.get('name')
                    for cell in qualifier_entry.get('cell', []):
                        timestamp = cell.get('timestamp')
                        raw_value = cell.get('value')

                        # Update statistics
                        self.stats['family_counts'][family] += 1
                        self.stats['qualifier_counts'][qualifier] += 1

                        rows.append({
                            'row_key': row_key,
                            'family': family,
                            'qualifier': qualifier,
                            'timestamp': timestamp,
                            'raw_value': raw_value
                        })
                        self.stats['total_cells'] += 1

            except Exception as e:
                logger.warning(f"Error processing cell for family {family}: {e}")
                self.stats['errors'].append(str(e))
                self.error_count += 1
        # Record how many families this row contains
        num_families = len(families_in_row)
        if 'families_per_row' not in self.stats:
            self.stats['families_per_row'] = defaultdict(int)
        self.stats['families_per_row'][num_families] += 1

        return rows

    def add_to_family_batch(self, rows: List[Dict[str, Any]]):
        """Add rows to each family's batch; insert when batch is full."""
        for row in rows:
            fam = row['family']
            self.family_batches[fam].append(row)
            if len(self.family_batches[fam]) >= self.batch_size:
                self.insert_family_batch(fam)

    def insert_family_batch(self, family: str):
        """Insert and clear the batch for a single family."""
        rows = self.family_batches[family]
        if not rows:
            return
        import re
        def sanitize_table_name(name):
            sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name)
            if not sanitized[0].isalpha():
                sanitized = 'f_' + sanitized
            return sanitized

        try:
            sanitized_family = sanitize_table_name(family)
            table_name = f"{table}_{sanitized_family}"
            cql_family = f"""INSERT INTO {keyspace}.{table_name} (row_key, qualifier, timestamp, raw_value) VALUES (?,?,?,?) """
            cql_prepared = session.prepare(cql_family)
            cql_prepared.consistency_level = ConsistencyLevel.ONE
            batch_data = [(r['row_key'], r['qualifier'], r['timestamp'], r['raw_value']) for r in rows]
            execute_concurrent_with_args(
                session,
                cql_prepared,
                batch_data,
                concurrency=50,
                raise_on_first_error=True
            )
            logger.debug(f"Inserted batch of {len(rows)} rows into table {table_name}")
        except Exception as e:
            logger.error(f"Batch insert failed for family {family}: {e}")
            raise
        self.family_batches[family] = []  # Clear batch

    def flush_all_batches(self):
        """At end, insert all remaining rows (for all families)."""
        for family in list(self.family_batches.keys()):
            self.insert_family_batch(family)

    def stream_process_and_insert(self):
        logger.info(f"Starting streaming processing of {self.parquet_file_path}")
        start_time = time.time()
        try:
            for record in self.stream_parquet_records():
                try:
                    logger.debug(f"Record: {record}")
                    db_rows = self.process_record_cells(record)
                    self.add_to_family_batch(db_rows)
                    self.processed_count += 1
                    self.stats['total_records'] += 1

                    if self.processed_count % PROGRESS_INTERVAL == 0:
                        elapsed = time.time() - start_time
                        rate = self.processed_count / elapsed
                        progress = record.get('_progress', 0)
                        logger.info(f"Processed {self.processed_count} records ({rate:.1f} records/sec) - {progress:.1f}% complete")
                except Exception as e:
                    logger.error(f"Error processing record {self.processed_count}: {e}")
                    self.error_count += 1
                    continue

            self.flush_all_batches()
            elapsed = time.time() - start_time
            logger.info(f"Streaming processing complete!")
            logger.info(f"Total records processed: {self.processed_count}")
            logger.info(f"Total cells processed: {self.stats['total_cells']}")
            logger.info(f"Total errors: {self.error_count}")
            logger.info(f"Total time: {elapsed:.2f} seconds")
            logger.info(f"Average rate: {self.processed_count / elapsed:.2f} records/sec")
        except KeyboardInterrupt:
            logger.info("Processing interrupted by user")
            self.flush_all_batches()
            raise
        except Exception as e:
            logger.error(f"Streaming processing failed: {e}")
            self.flush_all_batches()
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

                if 'families_per_row' in self.stats:
                    f.write("\nFAMILIES PER ROW DISTRIBUTION:\n")
                    f.write("-" * 40 + "\n")
                    sorted_family_counts = sorted(self.stats['families_per_row'].items())
                    for family_count, freq in sorted_family_counts:
                        f.write(f"{family_count} families: {freq} rows\n")

            logger.info(f"Analysis report saved to {output_file}")

        except Exception as e:
            logger.error(f"Failed to generate analysis report: {e}")

    def export_sample_to_csv(self, sample_size: int = 10000, output_file: str = 'sample_data.csv'):
        """Export a sample of processed data to CSV for verification (use Parquet)"""
        logger.info(f"Exporting sample of {sample_size} records to CSV...")
        sample_rows = []
        count = 0
        try:
            for record in self.stream_parquet_records():
                if count >= sample_size:
                    break
                db_rows = self.process_record_cells(record)
                sample_rows.extend(db_rows)
                count += 1
            df = pd.DataFrame(sample_rows)
            df.to_csv(output_file, index=False)
            logger.info(f"Sample data exported to {output_file} ({len(sample_rows)} rows)")
        except Exception as e:
            logger.error(f"Failed to export sample  {e}")


def main():
    """Main execution function with streaming processing"""
    parquet_file = FILENAME # 'input.parquet'

    # Initialize streaming processor
    processor = StreamingBigtableProcessor(parquet_file, batch_size=BATCH_SIZE)
    try:
        logger.info("Starting streaming Bigtable data processing...")

        # Main streaming processing
        processor.stream_process_and_insert()

        # Generate analysis report
        processor.generate_streaming_analysis_report()

        # Export sample data for verification

        if EXPORT_CSV == True:
            base, _ = os.path.splitext(FILENAME)
            output_file = base + '.csv'
            processor.export_sample_to_csv(sample_size=5000, output_file=output_file)

        print("\n\n" + "="*60)
        logger.info("STREAMING PROCESSING COMPLETE!")
        print("="*60)
        logger.info(f"Total Records: {processor.stats['total_records']}")
        logger.info(f"Total Cells: {processor.stats['total_cells']}")
        logger.info(f"Processing Errors: {processor.error_count}")

        logger.info(f"\nCells by Column Families (table):")
        sorted_families = sorted(processor.stats['family_counts'].items(), 
                               key=lambda x: x[1], reverse=True)[:]
        for family, count in sorted_families:
            logger.info(f"  {family}: {count} cells")

        logger.info("\nGenerated Files:")
        logger.info("- streaming_analysis_report.txt: Detailed analysis")
        if EXPORT_CSV == True:
            logger.info(f"- {FILENAME.replace('.parquet', '.csv')}: Sample extracted data for verification")

    except Exception as e:
        logger.error(f"Streaming processing failed: {e}")
        raise

def create_tables_by_column_family(column_names: List[str]):
    """Create ScyllaDB tables based on detected column families"""
    if not column_names or len(column_names) < 2:
        raise ValueError("Column names must include at least one family column")
    import re
    def sanitize_table_name(name):
        # Cassandra table names must start with a letter and contain only alphanumeric and underscores
        sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', name)
        if not sanitized[0].isalpha():
            sanitized = 'f_' + sanitized
        return sanitized

    for family in column_names[1:]:  # Skip first column (row_key)
        sanitized_family = sanitize_table_name(family)
        t = f"table_{sanitized_family}"
        logger.info(f"Creating table {keyspace}.{t} with compression {c}")
        create_table = f"""CREATE TABLE IF NOT EXISTS {keyspace}.{t}
            (row_key text, qualifier text, timestamp timestamp, raw_value blob,
            PRIMARY KEY (row_key, timestamp, qualifier))
            WITH CLUSTERING ORDER BY (timestamp DESC, qualifier ASC)
            AND compression = {{ {c} }};
            """
        try:
            session.execute(create_table)
        except Exception as e:
            logger.error(f"Failed to create table {keyspace}.{t}: {e}")

def get_cluster():
    """Get ScyllaDB cluster connection with optimized settings"""

    if SCYLLA_IP == ['127.0.0.1']:
        return Cluster(SCYLLA_IP, auth_provider=PlainTextAuthProvider(username=USERNAME, password=PASSWORD))
    else:
        profile = ExecutionProfile(load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc=LOCAL_DC)))
        return Cluster(
        contact_points= SCYLLA_IP,
        auth_provider = PlainTextAuthProvider(username=USERNAME, password=PASSWORD),
        execution_profiles={EXEC_PROFILE_DEFAULT: profile},
        connect_timeout=30,
        control_connection_timeout=30)

if __name__ == "__main__":
    logger.info('Connecting to cluster')
    # cluster = Cluster(SCYLLA_IP, auth_provider=PlainTextAuthProvider(username=USERNAME, password=PASSWORD))
    try:
        cluster = get_cluster()
        session = cluster.connect()
    
        if DROP_KEYSPACE == True:
            logger.info(f"Dropping keyspace {keyspace} (if exists)")
            drop_ks = f"DROP KEYSPACE IF EXISTS {keyspace};"
            session.execute(drop_ks)
        tablets_str = 'true' if tablets else 'false'
        create_ks = f"""
            CREATE KEYSPACE IF NOT EXISTS {keyspace}
            WITH replication = {{'class' : 'org.apache.cassandra.locator.NetworkTopologyStrategy', 'replication_factor' : 2}}
            AND tablets = {{'enabled': '{tablets_str}' }};
            """
        session.execute(create_ks)
        logger.info('Keyspace created')
        main()
    except Exception as e:
        logger.error(f"Error during cluster setup or main execution: {e}")
        raise
    finally:
        if 'session' in locals() and session:
            session.shutdown()
        if 'cluster' in locals() and cluster:
            cluster.shutdown()
