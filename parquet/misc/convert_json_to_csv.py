#!/usr/bin/env python3
import json
import base64
import pandas as pd
from collections import defaultdict
import struct
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BigtableDataProcessor:
    """
    Approach 1: Basic data extraction and analysis from Bigtable-like JSON data
    """
    
    def __init__(self, json_file_path):
        self.json_file_path = json_file_path
        self.data = []
        self.processed_records = []
        
    def load_data(self):
        """Load JSON data from file"""
        try:
            with open(self.json_file_path, 'r', encoding='utf-8') as file:
                for line_num, line in enumerate(file, 1):
                    line = line.strip()
                    if line:
                        try:
                            record = json.loads(line)
                            self.data.append(record)
                        except json.JSONDecodeError as e:
                            logger.warning(f"Skipping malformed JSON on line {line_num}: {e}")
            
            logger.info(f"Loaded {len(self.data)} records from {self.json_file_path}")
            
        except FileNotFoundError:
            logger.error(f"File not found: {self.json_file_path}")
            raise
        except Exception as e:
            logger.error(f"Error loading  {e}")
            raise
    
    def decode_base64_value(self, encoded_value):
        """Decode base64 encoded values"""
        try:
            decoded_bytes = base64.b64decode(encoded_value)
            
            # Try to decode as different data types
            if len(decoded_bytes) == 8:
                # Might be a 64-bit integer or timestamp
                try:
                    value = struct.unpack('>Q', decoded_bytes)[0]  # Big-endian unsigned long
                    return value
                except:
                    pass
            
            # Try as string
            try:
                return decoded_bytes.decode('utf-8')
            except UnicodeDecodeError:
                # Return raw bytes if can't decode as string
                return decoded_bytes.hex()
                
        except Exception as e:
            logger.warning(f"Failed to decode base64 value: {e}")
            return encoded_value
    
    def extract_cell_data(self, cells):
        """Extract and organize cell data by family"""
        cell_data = defaultdict(list)
        
        for cell in cells:
            family = cell.get('family', 'unknown')
            qualifier = cell.get('qual', 'unknown')
            timestamp = cell.get('ts_micros', 0)
            encoded_value = cell.get('value_b64', '')
            
            # Decode the base64 value
            decoded_value = self.decode_base64_value(encoded_value)
            
            # Convert timestamp from microseconds to datetime
            timestamp_dt = datetime.fromtimestamp(timestamp / 1000000) if timestamp > 0 else None
            
            cell_info = {
                'qualifier': qualifier,
                'timestamp': timestamp_dt,
                'timestamp_micros': timestamp,
                'raw_value': encoded_value,
                'decoded_value': decoded_value
            }
            
            cell_data[family].append(cell_info)
        
        return dict(cell_data)
    
    def process_records(self):
        """Process all loaded records"""
        logger.info("Processing records...")
        
        for i, record in enumerate(self.data):
            try:
                row_key = record.get('row_key', f'unknown_{i}')
                cells = record.get('cells', [])
                
                processed_record = {
                    'row_key': row_key,
                    'cell_count': len(cells),
                    'families': self.extract_cell_data(cells)
                }
                
                self.processed_records.append(processed_record)
                
            except Exception as e:
                logger.error(f"Error processing record {i}: {e}")
                continue
        
        logger.info(f"Processed {len(self.processed_records)} records")
    
    def analyze_data_structure(self):
        """Analyze the structure of the data"""
        logger.info("Analyzing data structure...")
        
        family_counts = defaultdict(int)
        qualifier_counts = defaultdict(int)
        total_cells = 0
        
        for record in self.processed_records:
            total_cells += record['cell_count']
            
            for family, cells in record['families'].items():
                family_counts[family] += len(cells)
                
                for cell in cells:
                    qualifier_counts[cell['qualifier']] += 1
        
        analysis = {
            'total_records': len(self.processed_records),
            'total_cells': total_cells,
            'avg_cells_per_record': total_cells / len(self.processed_records) if self.processed_records else 0,
            'family_distribution': dict(family_counts),
            'top_qualifiers': dict(sorted(qualifier_counts.items(), key=lambda x: x[1], reverse=True)[:20])
        }
        
        return analysis
    
    def extract_to_dataframe(self, family_name=None):
        """Extract data to pandas DataFrame"""
        rows = []
        
        for record in self.processed_records:
            row_key = record['row_key']
            
            if family_name:
                # Extract specific family data
                if family_name in record['families']:
                    for cell in record['families'][family_name]:
                        rows.append({
                            'row_key': row_key,
                            'family': family_name,
                            'qualifier': cell['qualifier'],
                            'timestamp': cell['timestamp'],
                            'timestamp_micros': cell['timestamp_micros'],
                            'decoded_value': cell['decoded_value']
                        })
            else:
                # Extract all families
                for family, cells in record['families'].items():
                    for cell in cells:
                        rows.append({
                            'row_key': row_key,
                            'family': family,
                            'qualifier': cell['qualifier'],
                            'timestamp': cell['timestamp'],
                            'timestamp_micros': cell['timestamp_micros'],
                            'decoded_value': cell['decoded_value']
                        })
        
        return pd.DataFrame(rows)
    
    def save_analysis_report(self, output_file='analysis_report.txt'):
        """Save analysis report to file"""
        analysis = self.analyze_data_structure()
        
        with open(output_file, 'w') as f:
            f.write("BIGTABLE DATA ANALYSIS REPORT\n")
            f.write("=" * 50 + "\n\n")
            
            f.write(f"Total Records: {analysis['total_records']}\n")
            f.write(f"Total Cells: {analysis['total_cells']}\n")
            f.write(f"Average Cells per Record: {analysis['avg_cells_per_record']:.2f}\n\n")
            
            f.write("FAMILY DISTRIBUTION:\n")
            f.write("-" * 30 + "\n")
            for family, count in sorted(analysis['family_distribution'].items(), key=lambda x: x[1], reverse=True):
                f.write(f"{family}: {count} cells\n")
            
            f.write("\nTOP 20 QUALIFIERS:\n")
            f.write("-" * 30 + "\n")
            for qual, count in list(analysis['top_qualifiers'].items())[:20]:
                f.write(f"{qual}: {count} occurrences\n")
        
        logger.info(f"Analysis report saved to {output_file}")
    
    def export_to_csv(self, family_name=None, output_file='extracted_data.csv'):
        """Export processed data to CSV"""
        df = self.extract_to_dataframe(family_name)
        df.to_csv(output_file, index=False)
        logger.info(f"Data exported to {output_file} ({len(df)} rows)")
        return df

def main():
    """Main execution function"""
    # Initialize processor
    processor = BigtableDataProcessor('prod_revised.json')
    
    try:
        # Step 1: Load data
        processor.load_data()
        
        # Step 2: Process records
        processor.process_records()
        
        # Step 3: Analyze data structure
        analysis = processor.analyze_data_structure()
        print("\nDATA ANALYSIS SUMMARY:")
        print(f"Total Records: {analysis['total_records']}")
        print(f"Total Cells: {analysis['total_cells']}")
        print(f"Average Cells per Record: {analysis['avg_cells_per_record']:.2f}")
        
        print(f"\nTop 5 Column Families:")
        for family, count in list(analysis['family_distribution'].items())[:5]:
            print(f"  {family}: {count} cells")
        
        # Step 4: Save detailed analysis report
        processor.save_analysis_report()
        
        # Step 5: Export specific family data (example: 'pb' family)
        if 'pb' in analysis['family_distribution']:
            pb_df = processor.export_to_csv('pb', 'pb_family_data.csv')
            print(f"\nExtracted 'pb' family  {len(pb_df)} rows")
        
        # Step 6: Export all data
        all_df = processor.export_to_csv(None, 'all_data.csv')
        print(f"Exported all  {len(all_df)} rows")
        
        print("\nProcessing complete! Check the generated files:")
        print("- analysis_report.txt: Detailed analysis")
        print("- pb_family_data.csv: PB family data")
        print("- all_data.csv: All extracted data")
        
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        raise

if __name__ == "__main__":
    main()

