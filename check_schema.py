#!/usr/bin/env python3
"""
Script to check table schema and data issues
"""

import psycopg2
import sys

def check_table_schema():
    """Check table schema and data issues"""
    
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
        
        print("🔍 Checking product_trcfeco2 table schema...")
        print("=" * 60)
        
        # Check table schema
        cursor.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_schema = 'public' 
            AND table_name = 'product_trcfeco2'
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        print("📋 Table columns:")
        for col_name, data_type, nullable in columns:
            print(f"  {col_name}: {data_type} ({'NULL' if nullable == 'YES' else 'NOT NULL'})")
        
        # Check total records
        cursor.execute("SELECT COUNT(*) FROM public.product_trcfeco2")
        total_records = cursor.fetchone()[0]
        print(f"\n📊 Total records: {total_records:,}")
        
        # Check first few records
        print(f"\n🔍 First 5 records:")
        cursor.execute("SELECT * FROM public.product_trcfeco2 LIMIT 5")
        first_records = cursor.fetchall()
        
        # Get column names for display
        column_names = [desc[0] for desc in cursor.description]
        print("Columns:", ", ".join(column_names))
        
        for i, record in enumerate(first_records, 1):
            print(f"Record {i}:")
            for j, value in enumerate(record):
                if value is not None and len(str(value)) > 100:
                    print(f"  {column_names[j]}: {str(value)[:100]}...")
                else:
                    print(f"  {column_names[j]}: {value}")
            print()
        
        # Check records around 10085 (using row number)
        print(f"\n🔍 Records around row 10085:")
        cursor.execute("""
            SELECT *
            FROM (
                SELECT *, ROW_NUMBER() OVER (ORDER BY ctid) as row_num
                FROM public.product_trcfeco2
            ) t
            WHERE row_num BETWEEN 10080 AND 10090
            ORDER BY row_num
        """)
        
        records_around_10085 = cursor.fetchall()
        print(f"Found {len(records_around_10085)} records around row 10085")
        
        for record in records_around_10085:
            row_num = record[-1]  # Last column is row_num
            print(f"Row {row_num}:")
            for j, value in enumerate(record[:-1]):  # Exclude row_num
                if value is not None and len(str(value)) > 50:
                    print(f"  {column_names[j]}: {str(value)[:50]}...")
                else:
                    print(f"  {column_names[j]}: {value}")
            print()
        
        # Check last few records
        print(f"\n🔍 Last 5 records:")
        cursor.execute("""
            SELECT *
            FROM (
                SELECT *, ROW_NUMBER() OVER (ORDER BY ctid) as row_num
                FROM public.product_trcfeco2
            ) t
            WHERE row_num > (SELECT COUNT(*) FROM public.product_trcfeco2) - 5
            ORDER BY row_num
        """)
        
        last_records = cursor.fetchall()
        for record in last_records:
            row_num = record[-1]
            print(f"Row {row_num}:")
            for j, value in enumerate(record[:-1]):
                if value is not None and len(str(value)) > 50:
                    print(f"  {column_names[j]}: {str(value)[:50]}...")
                else:
                    print(f"  {column_names[j]}: {value}")
            print()
        
        conn.close()
        
        print("\n✅ Schema check completed!")
        
    except psycopg2.Error as e:
        print(f"❌ Database error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    check_table_schema()
