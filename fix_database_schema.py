#!/usr/bin/env python3
"""
Database Schema Fix for USPTO Processing
Fixes column name mismatches between CSV data and database schema
"""

import psycopg2
import sys

def fix_database_schema():
    """Fix database schema column name mismatches"""
    
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
        
        print("Fixing database schema column mismatches...")
        print("=" * 50)
        
        # Get all product tables
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name LIKE 'product_%'
        """)
        
        product_tables = cursor.fetchall()
        
        for (table_name,) in product_tables:
            print(f"\nFixing table: {table_name}")
            
            # Check if serial_number column exists
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = %s 
                AND column_name = 'serial_number'
            """, (table_name,))
            
            has_serial_number = cursor.fetchone() is not None
            
            # Check if serial_no column exists
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = %s 
                AND column_name = 'serial_no'
            """, (table_name,))
            
            has_serial_no = cursor.fetchone() is not None
            
            if has_serial_number and not has_serial_no:
                print(f"  Found serial_number column, renaming to serial_no...")
                
                # Rename serial_number to serial_no
                cursor.execute(f"""
                    ALTER TABLE {table_name} 
                    RENAME COLUMN serial_number TO serial_no
                """)
                print(f"  ✅ Renamed serial_number to serial_no")
                
            elif has_serial_no and not has_serial_number:
                print(f"  ✅ serial_no column already exists")
                
            elif has_serial_number and has_serial_no:
                print(f"  ⚠️  Both columns exist, dropping serial_number...")
                cursor.execute(f"""
                    ALTER TABLE {table_name} 
                    DROP COLUMN serial_number
                """)
                print(f"  ✅ Dropped duplicate serial_number column")
                
            else:
                print(f"  ❌ No serial number column found!")
                
                # Add serial_no column
                cursor.execute(f"""
                    ALTER TABLE {table_name} 
                    ADD COLUMN serial_no VARCHAR(20)
                """)
                print(f"  ✅ Added serial_no column")
        
        conn.commit()
        conn.close()
        
        print("\n✅ Database schema fixes completed!")
        
    except Exception as e:
        print(f"❌ Error fixing schema: {e}")
        sys.exit(1)

def verify_schema_fix():
    """Verify that the schema fix worked"""
    
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
        
        print("\nVerifying schema fix...")
        print("=" * 30)
        
        # Check all product tables
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name LIKE 'product_%'
        """)
        
        product_tables = cursor.fetchall()
        
        for (table_name,) in product_tables:
            # Check for serial_no column
            cursor.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = %s 
                AND column_name = 'serial_no'
            """, (table_name,))
            
            result = cursor.fetchone()
            if result:
                print(f"✅ {table_name}: serial_no ({result[1]})")
            else:
                print(f"❌ {table_name}: serial_no column missing")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error verifying schema: {e}")

if __name__ == "__main__":
    print("USPTO Database Schema Fix")
    print("=" * 40)
    print()
    print("ISSUE: Column name mismatch")
    print("- CSV data uses: serial_no")
    print("- Database schema uses: serial_number")
    print("- This causes INSERT errors")
    print()
    
    # Fix the schema
    fix_database_schema()
    
    # Verify the fix
    verify_schema_fix()
    
    print("\n" + "=" * 40)
    print("✅ Schema fix completed!")
    print("The system should now work without column name errors.")
