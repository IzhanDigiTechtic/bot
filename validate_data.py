#!/usr/bin/env python3
"""
Data validation script to ensure data integrity during processing
"""

import pandas as pd
import sys
from pathlib import Path

def validate_csv_file(file_path: str) -> bool:
    """Validate CSV file for data integrity"""
    
    print(f"Validating file: {file_path}")
    
    try:
        # Read first few rows to check structure
        df_sample = pd.read_csv(file_path, nrows=1000)
        
        # Check for required columns
        required_columns = ['serial_no']
        missing_columns = [col for col in required_columns if col not in df_sample.columns]
        
        if missing_columns:
            print(f"Missing required columns: {missing_columns}")
            return False
        
        # Check serial number patterns
        serial_nos = df_sample['serial_no'].dropna()
        
        if len(serial_nos) == 0:
            print("No valid serial numbers found")
            return False
        
        # Check for fake serial numbers (60000000+)
        fake_serials = serial_nos[serial_nos >= 60000000]
        if len(fake_serials) > 0:
            print(f"Found {len(fake_serials)} potentially fake serial numbers (60000000+)")
            print("Sample fake serials:", fake_serials.head().tolist())
            
            # Check if ALL serials are fake
            if len(fake_serials) == len(serial_nos):
                print("ALL serial numbers appear to be fake! File is corrupted.")
                return False
        
        # Check for reasonable serial number ranges
        valid_serials = serial_nos[serial_nos < 60000000]
        if len(valid_serials) > 0:
            print(f"Found {len(valid_serials)} valid serial numbers")
            print(f"Range: {valid_serials.min()} - {valid_serials.max()}")
        else:
            print("No valid serial numbers found")
            return False
        
        print("File validation passed")
        return True
        
    except Exception as e:
        print(f"Error validating file: {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python validate_data.py <file_path>")
        sys.exit(1)
    
    file_path = sys.argv[1]
    if not Path(file_path).exists():
        print(f"File not found: {file_path}")
        sys.exit(1)
    
    success = validate_csv_file(file_path)
    sys.exit(0 if success else 1)
