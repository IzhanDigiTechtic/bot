#!/usr/bin/env python3
"""
Test both processors to ensure column mapping works
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from controllers.core.file_processors import USPTOFileProcessor
from controllers.core.uspto_controllers import ProcessingController

def test_both_processors():
    """Test both processors for column mapping"""
    
    print("Testing Both Processors for Column Mapping...")
    print("=" * 50)
    
    # Test record
    test_record = {
        'serial_number': '12345678',
        'registration_number': '9876543',
        'filing_date': '2023-01-01',
        'mark_identification': 'TEST MARK'
    }
    
    print("Original record:")
    for key, value in test_record.items():
        print(f"  {key}: {value}")
    
    # Test USPTOFileProcessor
    print("\n" + "="*30)
    print("Testing USPTOFileProcessor:")
    print("="*30)
    
    config = {'processing': {'batch_size': 1000}}
    file_processor = USPTOFileProcessor(config)
    
    mapped_record = file_processor._map_column_names(test_record)
    print("Mapped record:")
    for key, value in mapped_record.items():
        print(f"  {key}: {value}")
    
    cleaned_record = file_processor._clean_record(test_record, 'TRTYRAP')
    print("Cleaned record:")
    for key, value in cleaned_record.items():
        print(f"  {key}: {value}")
    
    # Test ProcessingController
    print("\n" + "="*30)
    print("Testing ProcessingController:")
    print("="*30)
    
    processing_controller = ProcessingController(config)
    
    mapped_record2 = processing_controller._map_column_names(test_record)
    print("Mapped record:")
    for key, value in mapped_record2.items():
        print(f"  {key}: {value}")
    
    cleaned_record2 = processing_controller._clean_record(test_record, 'TRTYRAP')
    print("Cleaned record:")
    for key, value in cleaned_record2.items():
        print(f"  {key}: {value}")
    
    # Check results
    print("\n" + "="*50)
    print("RESULTS:")
    print("="*50)
    
    success1 = 'serial_no' in cleaned_record
    success2 = 'serial_no' in cleaned_record2
    
    print(f"USPTOFileProcessor: {'✅ SUCCESS' if success1 else '❌ FAILED'}")
    print(f"ProcessingController: {'✅ SUCCESS' if success2 else '❌ FAILED'}")
    
    if success1 and success2:
        print("\n🎉 ALL PROCESSORS FIXED!")
        print("Column mapping is working correctly in both processors.")
    else:
        print("\n❌ SOME PROCESSORS STILL HAVE ISSUES")
        print("Need to fix the failing processors.")

if __name__ == "__main__":
    test_both_processors()
