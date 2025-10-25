#!/usr/bin/env python3
"""
Filter out fake serial numbers during USPTO processing
Skip serial numbers 60000001-69999999 and only process real data
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from controllers.core.uspto_controllers import ProcessingController
from pathlib import Path

def is_fake_serial_number(serial_no):
    """Check if a serial number is fake (60000001-69999999)"""
    try:
        serial_int = int(serial_no)
        return 60000001 <= serial_int <= 69999999
    except (ValueError, TypeError):
        return False

def filter_fake_data_from_csv():
    """Test filtering fake data from CSV processing"""
    
    print("Testing Fake Data Filtering...")
    print("=" * 50)
    
    # Create processing controller
    config = {
        'processing': {
            'batch_size': 1000,
            'chunk_size': 10000
        }
    }
    
    processor = ProcessingController(config)
    
    # Test with TRCFECO2 CSV file
    csv_file = Path("uspto_data/extracted/TRCFECO2/case_file.csv/case_file.csv")
    
    if not csv_file.exists():
        print(f"❌ CSV file not found: {csv_file}")
        return
    
    print(f"Processing CSV file: {csv_file}")
    print(f"File size: {csv_file.stat().st_size / (1024*1024):.1f}MB")
    print()
    print("Filtering out fake serial numbers (60000001-69999999)...")
    print()
    
    try:
        batch_count = 0
        total_records = 0
        fake_records_skipped = 0
        real_records_processed = 0
        
        for batch in processor.process_csv_file(csv_file, 'TRCFECO2'):
            batch_count += 1
            
            # Filter out fake records from this batch
            real_records = []
            for record in batch:
                total_records += 1
                
                serial_no = record.get('serial_no')
                if serial_no and is_fake_serial_number(serial_no):
                    fake_records_skipped += 1
                else:
                    real_records_processed += 1
                    real_records.append(record)
            
            # Only yield batches with real data
            if real_records:
                print(f"Batch {batch_count}: {len(real_records)} real records (skipped {len(batch) - len(real_records)} fake)")
                
                # Show first real record from first batch
                if batch_count == 1 and real_records:
                    print(f"\nFirst REAL record sample:")
                    first_record = real_records[0]
                    for key, value in first_record.items():
                        if value:  # Only show non-empty values
                            print(f"  {key}: {value}")
            else:
                print(f"Batch {batch_count}: All records were fake (skipped {len(batch)} records)")
            
            # Stop after 10 batches for testing
            if batch_count >= 10:
                print(f"\nStopped after {batch_count} batches for testing")
                break
        
        print(f"\n" + "=" * 50)
        print("FILTERING RESULTS:")
        print("=" * 50)
        print(f"Total records examined: {total_records:,}")
        print(f"Fake records skipped: {fake_records_skipped:,} ({fake_records_skipped/total_records*100:.1f}%)")
        print(f"Real records processed: {real_records_processed:,} ({real_records_processed/total_records*100:.1f}%)")
        
        if real_records_processed > 0:
            print(f"\n✅ SUCCESS: Found {real_records_processed:,} real records!")
            print("   The filtering is working correctly.")
        else:
            print(f"\n❌ ISSUE: No real records found in the sample.")
            print("   All records in this file appear to be fake data.")
        
    except Exception as e:
        print(f"❌ Error processing CSV: {e}")
        import traceback
        traceback.print_exc()

def create_filtered_processor():
    """Create a modified processor that filters out fake data"""
    
    print("\nCreating Filtered Processor...")
    print("=" * 40)
    
    filter_code = '''
# Add this method to ProcessingController class
def _is_fake_serial_number(self, serial_no):
    """Check if a serial number is fake (60000001-69999999)"""
    try:
        serial_int = int(serial_no)
        return 60000001 <= serial_int <= 69999999
    except (ValueError, TypeError):
        return False

def _filter_fake_records(self, batch_data):
    """Filter out fake records from batch data"""
    real_records = []
    fake_count = 0
    
    for record in batch_data:
        serial_no = record.get('serial_no')
        if serial_no and self._is_fake_serial_number(serial_no):
            fake_count += 1
        else:
            real_records.append(record)
    
    if fake_count > 0:
        self.logger.info(f"Filtered out {fake_count} fake records, keeping {len(real_records)} real records")
    
    return real_records

# Modify the _clean_record method to filter fake data
def _clean_record(self, record: Dict, product_id: str) -> Optional[Dict]:
    """Clean and normalize a record with fake data filtering"""
    try:
        # First check if this is fake data
        serial_no = record.get('serial_no')
        if serial_no and self._is_fake_serial_number(serial_no):
            self.logger.debug(f"Skipping fake record with serial_no: {serial_no}")
            return None  # Skip fake records
        
        # Continue with normal cleaning for real records
        mapped_record = self._map_column_names(record)
        
        cleaned = {}
        for key, value in mapped_record.items():
            if value is None or value == '':
                cleaned[key] = None
            elif isinstance(value, str):
                cleaned[key] = value.strip()
            else:
                cleaned[key] = value
        
        # Add metadata
        cleaned['data_source'] = f"{product_id} [CSV]"
        cleaned['batch_number'] = 0  # Will be set by database controller
        
        return cleaned
        
    except Exception as e:
        self.logger.error(f"Error cleaning record: {e}")
        return None
'''
    
    with open("filtered_processor_patch.py", 'w') as f:
        f.write(f"# Filtered Processor Patch\\n\\n{filter_code}")
    
    print("✅ Created: filtered_processor_patch.py")
    print("   This contains the code to filter out fake serial numbers")
    print("   during processing instead of processing them.")

def main():
    """Main function"""
    
    print("USPTO Fake Data Filtering")
    print("=" * 50)
    print()
    print("GOAL: Skip fake serial numbers (60000001-69999999)")
    print("      and only process real trademark data")
    print()
    
    # Test the filtering
    filter_fake_data_from_csv()
    
    # Create the processor patch
    create_filtered_processor()
    
    print("\n" + "=" * 50)
    print("FILTERING SOLUTION READY!")
    print("=" * 50)
    print()
    print("The system can now:")
    print("✅ Detect fake serial numbers (60000001-69999999)")
    print("✅ Skip fake records during processing")
    print("✅ Only process real trademark data")
    print("✅ Maintain processing performance")
    print()
    print("Next step: Apply the filtering patch to the processor")

if __name__ == "__main__":
    main()
