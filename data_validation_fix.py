#!/usr/bin/env python3
"""
Data Validation Fix for USPTO Processing
Prevents processing of fake/test data
"""

import psycopg2
import sys
from pathlib import Path

def validate_data_before_processing(file_path):
    """Validate data before processing to prevent fake data"""
    
    print(f"Validating data file: {file_path}")
    
    try:
        # Read first 1000 lines to check for fake data
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            lines = [f.readline().strip() for _ in range(1000)]
        
        # Check for fake serial numbers
        fake_indicators = ['60000001', '60000002', '60000003', '60000004', '60000005']
        fake_count = 0
        
        for line in lines:
            if any(fake in line for fake in fake_indicators):
                fake_count += 1
        
        if fake_count > 50:  # If more than 50 lines contain fake data
            print(f"ERROR: Found {fake_count} lines with fake serial numbers!")
            print("This file contains TEST DATA, not real USPTO data.")
            print("DO NOT PROCESS THIS FILE!")
            return False
        else:
            print(f"SUCCESS: Only {fake_count} lines with fake data found")
            print("This appears to be real USPTO data.")
            return True
            
    except Exception as e:
        print(f"Error validating file: {e}")
        return False

def clean_fake_data_from_database():
    """Remove all fake data from the database"""
    
    print("Cleaning fake data from database...")
    
    db_config = {
        'dbname': 'trademarks',
        'user': 'postgres',
        'password': '1234',
        'host': 'localhost',
        'port': '5432'
    }
    
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # Get all product tables
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name LIKE 'product_%'
        """)
        
        product_tables = cursor.fetchall()
        
        total_deleted = 0
        
        for (table_name,) in product_tables:
            # Delete records with fake serial numbers
            cursor.execute(f"""
                DELETE FROM {table_name} 
                WHERE serial_no >= 60000000 OR serial_no::text LIKE '600000%'
            """)
            
            deleted_count = cursor.rowcount
            total_deleted += deleted_count
            
            if deleted_count > 0:
                print(f"Deleted {deleted_count} fake records from {table_name}")
        
        conn.commit()
        conn.close()
        
        print(f"Total fake records deleted: {total_deleted}")
        
    except Exception as e:
        print(f"Error cleaning database: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
        validate_data_before_processing(file_path)
    else:
        clean_fake_data_from_database()
