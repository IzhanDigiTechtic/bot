#!/usr/bin/env python3
"""
Test the column mapping fix
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from controllers.core.uspto_controllers import ProcessingController

def test_column_mapping():
    """Test the column mapping functionality"""
    
    print("Testing Column Mapping Fix...")
    print("=" * 40)
    
    # Create a test processing controller
    config = {
        'processing': {
            'batch_size': 1000,
            'chunk_size': 5000
        }
    }
    
    processor = ProcessingController(config)
    
    # Test record with various column names
    test_record = {
        'serial_number': '12345678',
        'registration_number': '9876543',
        'filing_date': '2023-01-01',
        'mark_identification': 'TEST MARK',
        'examiner_attorney_name': 'John Doe',
        'some_other_field': 'test value'
    }
    
    print("Original record:")
    for key, value in test_record.items():
        print(f"  {key}: {value}")
    
    # Test column mapping
    mapped_record = processor._map_column_names(test_record)
    
    print("\nMapped record:")
    for key, value in mapped_record.items():
        print(f"  {key}: {value}")
    
    # Test full cleaning
    cleaned_record = processor._clean_record(test_record, 'TRTDXFAP')
    
    print("\nCleaned record:")
    for key, value in cleaned_record.items():
        print(f"  {key}: {value}")
    
    # Check if serial_number was mapped to serial_no
    if 'serial_no' in cleaned_record:
        print("\n✅ SUCCESS: serial_number was mapped to serial_no")
        print(f"   Value: {cleaned_record['serial_no']}")
    else:
        print("\n❌ FAILED: serial_number was not mapped to serial_no")
    
    print("\n" + "=" * 40)
    print("Column mapping test completed!")

if __name__ == "__main__":
    test_column_mapping()
