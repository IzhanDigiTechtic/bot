#!/usr/bin/env python3
"""
Test the registration_number column mapping fix
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from controllers.core.uspto_controllers import DatabaseController

def test_registration_number_mapping():
    """Test that registration_number is not being mapped incorrectly"""
    
    print("Testing registration_number Column Mapping Fix...")
    print("=" * 60)
    
    # Create a test database controller
    config = {
        'database': {
            'dbname': 'trademarks',
            'user': 'postgres',
            'password': '1234',
            'host': 'localhost',
            'port': '5432'
        }
    }
    
    db_controller = DatabaseController(config)
    
    # Test batch data with registration_number column
    test_batch_data = [
        {
            'serial_number': '12345678',
            'registration_number': '9876543',  # This should NOT be mapped
            'filing_date': '2023-01-01',
            'mark_identification': 'TEST MARK'
        },
        {
            'serial_number': '87654321',
            'registration_number': '1234567',  # This should NOT be mapped
            'filing_date': '2023-02-01',
            'mark_identification': 'ANOTHER MARK'
        }
    ]
    
    print("Original batch data:")
    for i, record in enumerate(test_batch_data):
        print(f"  Record {i+1}: {record}")
    
    # Test column mapping
    mapped_batch = db_controller._map_insert_columns(test_batch_data)
    
    print("\nMapped batch data:")
    for i, record in enumerate(mapped_batch):
        print(f"  Record {i+1}: {record}")
    
    # Check if registration_number was incorrectly mapped
    success = True
    for record in mapped_batch:
        if 'registration_no' in record:
            print(f"\n❌ FAILED: registration_number was incorrectly mapped to registration_no in record: {record}")
            success = False
        elif 'registration_number' not in record:
            print(f"\n❌ FAILED: registration_number missing in record: {record}")
            success = False
    
    if success:
        print("\n✅ SUCCESS: registration_number columns preserved correctly")
        print("   Database insert will now work with registration_number!")
    
    print("\n" + "=" * 60)
    print("Registration number mapping test completed!")

if __name__ == "__main__":
    test_registration_number_mapping()
