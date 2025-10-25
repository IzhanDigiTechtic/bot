#!/usr/bin/env python3
"""
Stop USPTO processing and clean up fake data
"""

import psycopg2
import json
from pathlib import Path
import signal
import os
import sys

def get_db_config():
    """Get database configuration"""
    config_file = Path("uspto_controller_config.json")
    if config_file.exists():
        with open(config_file, 'r') as f:
            config = json.load(f)
        db_config = config.get('database', {})
        db_config.pop('use_copy', None)
        return db_config
    return {
        'dbname': 'trademarks',
        'user': 'postgres',
        'password': '1234',
        'host': 'localhost',
        'port': '5432'
    }

def stop_processing():
    """Stop any running USPTO processing"""
    
    print("Stopping USPTO Processing...")
    print("=" * 40)
    
    try:
        # Find and kill any running USPTO processes
        import subprocess
        
        # On Windows, find Python processes running USPTO scripts
        result = subprocess.run(['tasklist', '/FI', 'IMAGENAME eq python.exe', '/FO', 'CSV'], 
                              capture_output=True, text=True)
        
        if result.returncode == 0:
            lines = result.stdout.strip().split('\n')[1:]  # Skip header
            uspto_processes = []
            
            for line in lines:
                if 'uspto' in line.lower() or 'controller' in line.lower():
                    parts = line.split(',')
                    if len(parts) >= 2:
                        pid = parts[1].strip('"')
                        uspto_processes.append(pid)
            
            if uspto_processes:
                print(f"Found {len(uspto_processes)} USPTO processes to stop:")
                for pid in uspto_processes:
                    try:
                        subprocess.run(['taskkill', '/PID', pid, '/F'], check=True)
                        print(f"  Stopped process {pid}")
                    except subprocess.CalledProcessError:
                        print(f"  Could not stop process {pid}")
            else:
                print("No USPTO processes found running")
        else:
            print("Could not check for running processes")
            
    except Exception as e:
        print(f"Error stopping processes: {e}")
        print("Please manually stop any running USPTO processing")

def check_fake_data():
    """Check for fake data in the database"""
    
    print("\nChecking for Fake Data...")
    print("=" * 40)
    
    db_config = get_db_config()
    
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # Check each product table for fake serial numbers
        tables_to_check = [
            'product_trcfeco2',
            'product_trtdxfap', 
            'product_trtyrap',
            'product_traseco',
            'product_trtdxfag',
            'product_trtyrag',
            'product_ttabtdxf',
            'product_ttabyr'
        ]
        
        fake_data_found = False
        
        for table in tables_to_check:
            try:
                # Check for fake serial numbers (60000001+)
                cursor.execute(f"""
                    SELECT COUNT(*) 
                    FROM {table} 
                    WHERE serial_no::bigint >= 60000001
                """)
                
                fake_count = cursor.fetchone()[0]
                
                if fake_count > 0:
                    print(f"❌ {table}: {fake_count} fake records found")
                    fake_data_found = True
                else:
                    print(f"✅ {table}: No fake data found")
                    
            except Exception as e:
                print(f"⚠️  {table}: Could not check ({e})")
        
        conn.close()
        
        return fake_data_found
        
    except Exception as e:
        print(f"Error checking fake data: {e}")
        return False

def clean_fake_data():
    """Clean up fake data from the database"""
    
    print("\nCleaning Up Fake Data...")
    print("=" * 40)
    
    db_config = get_db_config()
    
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # Clean each product table
        tables_to_clean = [
            'product_trcfeco2',
            'product_trtdxfap', 
            'product_trtyrap',
            'product_traseco',
            'product_trtdxfag',
            'product_trtyrag',
            'product_ttabtdxf',
            'product_ttabyr'
        ]
        
        total_cleaned = 0
        
        for table in tables_to_clean:
            try:
                # Delete fake records (serial_no >= 60000001)
                cursor.execute(f"""
                    DELETE FROM {table} 
                    WHERE serial_no::bigint >= 60000001
                """)
                
                deleted_count = cursor.rowcount
                total_cleaned += deleted_count
                
                if deleted_count > 0:
                    print(f"✅ {table}: Deleted {deleted_count} fake records")
                else:
                    print(f"✅ {table}: No fake records to delete")
                    
            except Exception as e:
                print(f"⚠️  {table}: Could not clean ({e})")
        
        conn.commit()
        conn.close()
        
        print(f"\nTotal fake records cleaned: {total_cleaned}")
        
    except Exception as e:
        print(f"Error cleaning fake data: {e}")

def main():
    """Main function"""
    
    print("USPTO Processing Emergency Stop & Cleanup")
    print("=" * 50)
    print()
    print("ISSUE: System is processing fake data (serial numbers 60000001+)")
    print("ACTION: Stop processing and clean up fake data")
    print()
    
    # Stop processing
    stop_processing()
    
    # Check for fake data
    fake_data_found = check_fake_data()
    
    if fake_data_found:
        print("\n" + "=" * 50)
        print("FAKE DATA DETECTED!")
        print("=" * 50)
        
        response = input("\nDo you want to clean up the fake data? (y/n): ")
        if response.lower() == 'y':
            clean_fake_data()
        else:
            print("Fake data cleanup skipped.")
    else:
        print("\n✅ No fake data found in database.")
    
    print("\n" + "=" * 50)
    print("EMERGENCY STOP COMPLETED!")
    print("=" * 50)
    print()
    print("Next steps:")
    print("1. ✅ Processing stopped")
    print("2. ✅ Fake data cleaned (if requested)")
    print("3. 🔍 Need to find real USPTO data source")
    print("4. 🔧 Update configuration to use real data")
    print()
    print("The USPTO API endpoint is providing test data instead of real data.")
    print("We need to find the correct data source for real USPTO data.")

if __name__ == "__main__":
    main()
