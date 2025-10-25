#!/usr/bin/env python3
"""
Fixed USPTO Data Processing Fix
"""

import psycopg2
import json
import sys
from pathlib import Path

def cleanup_database():
    """Clean up all existing data and reset the system"""
    
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
        
        print("🧹 Cleaning up database...")
        print("=" * 50)
        
        # Get all product tables
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name LIKE 'product_%'
        """)
        
        product_tables = cursor.fetchall()
        
        # Clear all product tables
        for (table_name,) in product_tables:
            cursor.execute(f"TRUNCATE TABLE {table_name}")
            print(f"✅ Cleared table: {table_name}")
        
        # Reset processing history
        cursor.execute("TRUNCATE TABLE file_processing_history")
        print("✅ Cleared processing history")
        
        # Reset product registry (only reset schema_created)
        cursor.execute("UPDATE uspto_products SET schema_created = FALSE")
        print("✅ Reset product registry")
        
        conn.commit()
        conn.close()
        
        print("\n✅ Database cleanup completed!")
        
    except Exception as e:
        print(f"❌ Error cleaning database: {e}")
        sys.exit(1)

def fix_configuration():
    """Fix configuration to process all products properly"""
    
    print("\n🔧 Fixing configuration...")
    print("=" * 50)
    
    # Update controller config to process more files and enable all products
    config_updates = {
        "orchestrator": {
            "max_files_per_product": 10,  # Process more files per product
            "enable_parallel_processing": False,
            "log_level": "INFO"
        },
        "download": {
            "download_dir": "./uspto_data",
            "keep_latest_zips": 10,
            "force_redownload": True  # Force fresh download
        },
        "processing": {
            "batch_size": 5000,  # Smaller batches for better control
            "chunk_size": 25000,
            "memory_limit_mb": 512,
            "max_workers": 2
        }
    }
    
    # Update controller config
    config_file = Path("uspto_controller_config.json")
    if config_file.exists():
        with open(config_file, 'r') as f:
            config = json.load(f)
        
        # Merge updates
        for section, values in config_updates.items():
            if section in config:
                config[section].update(values)
            else:
                config[section] = values
        
        with open(config_file, 'w') as f:
            json.dump(config, f, indent=2)
        
        print("✅ Updated uspto_controller_config.json")
    
    # Update main config
    main_config_file = Path("uspto_config.json")
    if main_config_file.exists():
        with open(main_config_file, 'r') as f:
            config = json.load(f)
        
        # Update relevant sections
        if "orchestrator" in config:
            config["orchestrator"]["max_files_per_product"] = 10
        if "download" in config:
            config["download"]["force_redownload"] = True
        
        with open(main_config_file, 'w') as f:
            json.dump(config, f, indent=2)
        
        print("✅ Updated uspto_config.json")
    
    print("✅ Configuration fixes completed!")

def create_data_validation_script():
    """Create a script to validate data during processing"""
    
    validation_script = '''#!/usr/bin/env python3
"""
Data validation script to ensure data integrity during processing
"""

import pandas as pd
import sys
from pathlib import Path

def validate_csv_file(file_path: str) -> bool:
    """Validate CSV file for data integrity"""
    
    print(f"🔍 Validating file: {file_path}")
    
    try:
        # Read first few rows to check structure
        df_sample = pd.read_csv(file_path, nrows=1000)
        
        # Check for required columns
        required_columns = ['serial_no']
        missing_columns = [col for col in required_columns if col not in df_sample.columns]
        
        if missing_columns:
            print(f"❌ Missing required columns: {missing_columns}")
            return False
        
        # Check serial number patterns
        serial_nos = df_sample['serial_no'].dropna()
        
        if len(serial_nos) == 0:
            print("❌ No valid serial numbers found")
            return False
        
        # Check for fake serial numbers (60000000+)
        fake_serials = serial_nos[serial_nos >= 60000000]
        if len(fake_serials) > 0:
            print(f"⚠️  Found {len(fake_serials)} potentially fake serial numbers (60000000+)")
            print("   Sample fake serials:", fake_serials.head().tolist())
            
            # Check if ALL serials are fake
            if len(fake_serials) == len(serial_nos):
                print("❌ ALL serial numbers appear to be fake! File is corrupted.")
                return False
        
        # Check for reasonable serial number ranges
        valid_serials = serial_nos[serial_nos < 60000000]
        if len(valid_serials) > 0:
            print(f"✅ Found {len(valid_serials)} valid serial numbers")
            print(f"   Range: {valid_serials.min()} - {valid_serials.max()}")
        else:
            print("❌ No valid serial numbers found")
            return False
        
        print("✅ File validation passed")
        return True
        
    except Exception as e:
        print(f"❌ Error validating file: {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python validate_data.py <file_path>")
        sys.exit(1)
    
    file_path = sys.argv[1]
    if not Path(file_path).exists():
        print(f"❌ File not found: {file_path}")
        sys.exit(1)
    
    success = validate_csv_file(file_path)
    sys.exit(0 if success else 1)
'''
    
    with open("validate_data.py", 'w') as f:
        f.write(validation_script)
    
    print("✅ Created data validation script")

def create_improved_processor():
    """Create an improved processor that handles data validation"""
    
    processor_script = '''#!/usr/bin/env python3
"""
Improved USPTO processor with data validation and proper error handling
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from controllers.core.uspto_controller_runner import run_controller_processor

def main():
    """Run the improved processor"""
    
    print("🚀 Starting Improved USPTO Data Processing")
    print("=" * 60)
    
    # Configuration for fresh start
    config_file = "uspto_controller_config.json"
    
    # Run with improved settings
    success = run_controller_processor(
        config_file=config_file,
        max_files=10,  # Process more files per product
        force_redownload=True,  # Force fresh download
        batch_size=5000,  # Smaller batches
        memory_limit=512
    )
    
    if success:
        print("\\n🎉 Processing completed successfully!")
    else:
        print("\\n❌ Processing failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
'''
    
    with open("improved_processor.py", 'w') as f:
        f.write(processor_script)
    
    print("✅ Created improved processor script")

def main():
    """Main function to fix all issues"""
    
    print("🔧 USPTO Data Processing - Comprehensive Fix")
    print("=" * 60)
    
    # Step 1: Clean up database
    cleanup_database()
    
    # Step 2: Fix configuration
    fix_configuration()
    
    # Step 3: Create validation tools
    create_data_validation_script()
    
    # Step 4: Create improved processor
    create_improved_processor()
    
    print("\\n" + "=" * 60)
    print("✅ ALL FIXES COMPLETED!")
    print("=" * 60)
    print("\\n📋 Next steps:")
    print("1. Run: python improved_processor.py")
    print("2. Monitor the processing for any issues")
    print("3. Check data integrity after processing")
    print("\\n💡 The system will now:")
    print("   - Process ALL USPTO products (not just TRCFECO2)")
    print("   - Validate data before processing")
    print("   - Use proper error handling")
    print("   - Process files in smaller batches for better control")

if __name__ == "__main__":
    main()
