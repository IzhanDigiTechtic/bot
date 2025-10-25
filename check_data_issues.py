#!/usr/bin/env python3
"""
Script to check data issues in product_trcfeco2 table
"""

import psycopg2
import sys

def check_data_issues():
    """Check data issues in product_trcfeco2 table"""
    
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
        
        print("🔍 Checking data issues in product_trcfeco2 table...")
        print("=" * 60)
        
        # Check total records
        cursor.execute("SELECT COUNT(*) FROM public.product_trcfeco2")
        total_records = cursor.fetchone()[0]
        print(f"📊 Total records: {total_records:,}")
        
        # Check records around 10085
        print(f"\n🔍 Checking records around 10085...")
        cursor.execute("""
            SELECT serial_number, filing_date, mark_text, data_source
            FROM public.product_trcfeco2 
            ORDER BY serial_number 
            LIMIT 5 OFFSET 10080
        """)
        
        records_around_10085 = cursor.fetchall()
        print("Records around 10085:")
        for record in records_around_10085:
            print(f"  Serial: {record[0]}, Date: {record[1]}, Mark: {record[2][:50] if record[2] else 'N/A'}..., Source: {record[3]}")
        
        # Check for serial number 60000001
        print(f"\n🔍 Checking for serial number 60000001...")
        cursor.execute("""
            SELECT serial_number, filing_date, mark_text, data_source
            FROM public.product_trcfeco2 
            WHERE serial_number = 60000001
        """)
        
        serial_60000001 = cursor.fetchall()
        if serial_60000001:
            print("❌ Found serial number 60000001:")
            for record in serial_60000001:
                print(f"  Serial: {record[0]}, Date: {record[1]}, Mark: {record[2][:50] if record[2] else 'N/A'}..., Source: {record[3]}")
        else:
            print("✅ Serial number 60000001 not found")
        
        # Check for any serial numbers > 60000000
        print(f"\n🔍 Checking for serial numbers > 60000000...")
        cursor.execute("""
            SELECT COUNT(*) 
            FROM public.product_trcfeco2 
            WHERE serial_number > 60000000
        """)
        
        high_serial_count = cursor.fetchone()[0]
        print(f"Records with serial number > 60000000: {high_serial_count}")
        
        if high_serial_count > 0:
            cursor.execute("""
                SELECT serial_number, filing_date, mark_text, data_source
                FROM public.product_trcfeco2 
                WHERE serial_number > 60000000
                ORDER BY serial_number
                LIMIT 10
            """)
            
            high_serials = cursor.fetchall()
            print("Sample high serial numbers:")
            for record in high_serials:
                print(f"  Serial: {record[0]}, Date: {record[1]}, Mark: {record[2][:50] if record[2] else 'N/A'}..., Source: {record[3]}")
        
        # Check data source distribution
        print(f"\n📊 Data source distribution:")
        cursor.execute("""
            SELECT data_source, COUNT(*) as count
            FROM public.product_trcfeco2 
            GROUP BY data_source
            ORDER BY count DESC
        """)
        
        sources = cursor.fetchall()
        for source, count in sources:
            print(f"  {source}: {count:,} records")
        
        # Check for any NULL or empty serial numbers
        print(f"\n🔍 Checking for NULL or empty serial numbers...")
        cursor.execute("""
            SELECT COUNT(*) 
            FROM public.product_trcfeco2 
            WHERE serial_number IS NULL OR serial_number = 0
        """)
        
        null_serials = cursor.fetchone()[0]
        print(f"Records with NULL or 0 serial numbers: {null_serials}")
        
        # Check the last few records
        print(f"\n🔍 Checking last 10 records...")
        cursor.execute("""
            SELECT serial_number, filing_date, mark_text, data_source
            FROM public.product_trcfeco2 
            ORDER BY serial_number DESC
            LIMIT 10
        """)
        
        last_records = cursor.fetchall()
        print("Last 10 records:")
        for record in last_records:
            print(f"  Serial: {record[0]}, Date: {record[1]}, Mark: {record[2][:50] if record[2] else 'N/A'}..., Source: {record[3]}")
        
        conn.close()
        
        print("\n✅ Data check completed!")
        
    except psycopg2.Error as e:
        print(f"❌ Database error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    check_data_issues()
