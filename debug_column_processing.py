#!/usr/bin/env python3
"""
Debug script to see what's happening with column names
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from controllers.core.uspto_controllers import ProcessingController
import pandas as pd
from pathlib import Path

def debug_column_processing():
    """Debug what's happening with column processing"""
    
    print("Debugging Column Processing...")
    print("=" * 50)
    
    # Create a test CSV file with serial_number column
    test_csv_content = """serial_number,registration_number,filing_date,mark_identification
12345678,9876543,2023-01-01,TEST MARK
87654321,1234567,2023-02-01,ANOTHER MARK"""
    
    test_file = Path("test_debug.csv")
    with open(test_file, 'w') as f:
        f.write(test_csv_content)
    
    print("Created test CSV file with serial_number column")
    
    # Test the ProcessingController
    config = {
        'processing': {
            'batch_size': 1000,
            'chunk_size': 5000
        }
    }
    
    processor = ProcessingController(config)
    
    print("\nTesting ProcessingController.process_csv_file...")
    
    # Process the CSV file
    batches = list(processor.process_csv_file(test_file, 'TRTDXFAP'))
    
    print(f"Generated {len(batches)} batches")
    
    if batches:
        first_batch = batches[0]
        print(f"First batch has {len(first_batch)} records")
        
        if first_batch:
            first_record = first_batch[0]
            print("\nFirst record columns:")
            for key, value in first_record.items():
                print(f"  {key}: {value}")
            
            # Check if serial_number was mapped to serial_no
            if 'serial_no' in first_record:
                print("\n✅ SUCCESS: serial_number was mapped to serial_no")
                print(f"   Value: {first_record['serial_no']}")
            elif 'serial_number' in first_record:
                print("\n❌ FAILED: serial_number was NOT mapped to serial_no")
                print(f"   Value: {first_record['serial_number']}")
            else:
                print("\n❓ UNKNOWN: No serial number field found")
    
    # Clean up
    test_file.unlink()
    print(f"\nCleaned up test file: {test_file}")

if __name__ == "__main__":
    debug_column_processing()
