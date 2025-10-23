#!/usr/bin/env python3
"""
Check the database schema to see what columns exist
"""

import psycopg2

def check_schema():
    """Check the database schema"""
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
        
        # Get column information
        cursor.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_name = 'trademark_case_files'
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        print("Database schema for trademark_case_files:")
        for col in columns:
            print(f"  {col[0]}: {col[1]} ({'NULL' if col[2] == 'YES' else 'NOT NULL'})")
        
        # Check if cfh_status_cd and cfh_status_dt columns exist
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'trademark_case_files'
            AND column_name IN ('cfh_status_cd', 'cfh_status_dt', 'status_code', 'status_date')
        """)
        
        status_columns = cursor.fetchall()
        print(f"\nStatus-related columns found:")
        for col in status_columns:
            print(f"  - {col[0]}")
        
        conn.close()
        
    except Exception as e:
        print(f"Error checking schema: {e}")

if __name__ == "__main__":
    check_schema()
