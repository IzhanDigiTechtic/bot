#!/usr/bin/env python3
"""
Check table schemas for all product tables
"""

import psycopg2

def check_table_schemas():
    """Check schemas of all product tables"""
    
    conn = psycopg2.connect(dbname='trademarks', user='postgres', password='1234', host='localhost', port='5432')
    cursor = conn.cursor()
    
    # Get all product tables
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name LIKE 'product_%' 
        ORDER BY table_name
    """)
    
    tables = [row[0] for row in cursor.fetchall()]
    
    print("Product Table Schemas:")
    print("=" * 50)
    
    for table in tables:
        cursor.execute(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = '{table}' 
            ORDER BY ordinal_position
        """)
        
        columns = [row[0] for row in cursor.fetchall()]
        
        # Determine schema type
        if 'raw_data' in columns:
            schema_type = "GENERIC (❌ Wrong)"
        elif 'registration_number' in columns:
            schema_type = "CASE FILE (✅ Correct)"
        elif 'assignment_id' in columns:
            schema_type = "ASSIGNMENT (✅ Correct)"
        elif 'proceeding_number' in columns:
            schema_type = "TTAB (✅ Correct)"
        else:
            schema_type = "UNKNOWN"
        
        print(f"{table}: {len(columns)} columns - {schema_type}")
        print(f"  Columns: {columns[:5]}...")
        print()
    
    conn.close()

if __name__ == "__main__":
    check_table_schemas()
