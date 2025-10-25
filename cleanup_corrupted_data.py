#!/usr/bin/env python3
"""
Script to clean up corrupted data in product_trcfeco2 table
Remove records with fake serial numbers (60000000+) and clean up the table
"""

import psycopg2
import sys

def cleanup_corrupted_data():
    """Clean up corrupted data in product_trcfeco2 table"""
    
    # Database configuration
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
        
        print("🧹 Cleaning up corrupted data in product_trcfeco2 table...")
        print("=" * 60)
        
        # First, let's check how many records have fake serial numbers
        cursor.execute("""
            SELECT COUNT(*) 
            FROM public.product_trcfeco2 
            WHERE serial_no >= 60000000
        """)
        
        fake_serial_count = cursor.fetchone()[0]
        print(f"📊 Records with fake serial numbers (>= 60000000): {fake_serial_count:,}")
        
        if fake_serial_count == 0:
            print("✅ No fake serial numbers found. Data appears clean.")
            conn.close()
            return
        
        # Check the last valid record (before the corruption)
        cursor.execute("""
            SELECT id, serial_no, mark_id_char, filing_dt, registration_dt
            FROM public.product_trcfeco2 
            WHERE serial_no < 60000000
            ORDER BY serial_no DESC
            LIMIT 1
        """)
        
        last_valid_record = cursor.fetchone()
        if last_valid_record:
            print(f"📋 Last valid record:")
            print(f"   ID: {last_valid_record[0]}")
            print(f"   Serial: {last_valid_record[1]}")
            print(f"   Mark: {last_valid_record[2][:50] if last_valid_record[2] else 'N/A'}...")
            print(f"   Filing Date: {last_valid_record[3]}")
            print(f"   Registration Date: {last_valid_record[4]}")
        
        # Get confirmation from user
        print(f"\n⚠️  WARNING: About to delete {fake_serial_count:,} corrupted records!")
        print("This will remove all records with serial numbers >= 60000000")
        
        # Delete corrupted records
        print(f"\n🗑️  Deleting corrupted records...")
        cursor.execute("""
            DELETE FROM public.product_trcfeco2 
            WHERE serial_no >= 60000000
        """)
        
        deleted_count = cursor.rowcount
        print(f"✅ Deleted {deleted_count:,} corrupted records")
        
        # Check final count
        cursor.execute("SELECT COUNT(*) FROM public.product_trcfeco2")
        final_count = cursor.fetchone()[0]
        print(f"📊 Final record count: {final_count:,}")
        
        # Check the last few records to ensure they're valid
        print(f"\n🔍 Checking last 5 records after cleanup:")
        cursor.execute("""
            SELECT id, serial_no, mark_id_char, filing_dt, registration_dt
            FROM public.product_trcfeco2 
            ORDER BY serial_no DESC
            LIMIT 5
        """)
        
        last_records = cursor.fetchall()
        for i, record in enumerate(last_records, 1):
            print(f"   {i}. ID: {record[0]}, Serial: {record[1]}, Mark: {record[2][:30] if record[2] else 'N/A'}..., Filing: {record[3]}")
        
        # Commit the changes
        conn.commit()
        print(f"\n✅ Cleanup completed successfully!")
        
        conn.close()
        
    except psycopg2.Error as e:
        print(f"❌ Database error: {e}")
        if conn:
            conn.rollback()
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {e}")
        if conn:
            conn.rollback()
        sys.exit(1)

if __name__ == "__main__":
    cleanup_corrupted_data()
