#!/usr/bin/env python3
"""
Test the comprehensive column mapping fix
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from controllers.core.uspto_controllers import DatabaseController

def test_database_column_mapping():
    """Test the database column mapping functionality"""
    
    print("Testing Database Column Mapping Fix...")
    print("=" * 50)
    
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
    
    # Test batch data with serial_number column
    test_batch_data = [
        {
            'serial_number': '12345678',
            'registration_number': '9876543',
            'filing_date': '2023-01-01',
            'mark_identification': 'TEST MARK'
        },
        {
            'serial_number': '87654321',
            'registration_number': '1234567',
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
    
    # Check if serial_number was mapped to serial_no
    success = True
    for record in mapped_batch:
        if 'serial_number' in record:
            print(f"\n❌ FAILED: serial_number still present in record: {record}")
            success = False
        elif 'serial_no' not in record:
            print(f"\n❌ FAILED: serial_no not found in record: {record}")
            success = False
    
    if success:
        print("\n✅ SUCCESS: All serial_number columns mapped to serial_no")
        print("   Database insert will now work correctly!")
    
    print("\n" + "=" * 50)
    print("Database column mapping test completed!")

if __name__ == "__main__":
    test_database_column_mapping()
