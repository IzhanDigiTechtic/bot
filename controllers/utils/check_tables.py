#!/usr/bin/env python3
"""
Quick verification script to check USPTO table creation
Run this after setting up the database to verify tables are created
"""

import psycopg2
import sys

def check_tables():
    """Check if USPTO tables exist"""
    
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
        
        print("🔍 Checking USPTO Multi-Processor Tables...")
        print("=" * 50)
        
        # Check control tables
        control_tables = ['uspto_products', 'file_processing_history', 'batch_processing']
        print("\n📊 Control Tables:")
        for table in control_tables:
            cursor.execute('''
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                )
            ''', (table,))
            exists = cursor.fetchone()[0]
            status = "✅" if exists else "❌"
            print(f"  {status} {table}")
        
        # Check product tables
        cursor.execute('''
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name LIKE 'product_%'
            ORDER BY table_name
        ''')
        
        product_tables = cursor.fetchall()
        print(f"\n🏭 Product Tables ({len(product_tables)} found):")
        if product_tables:
            for (table_name,) in product_tables:
                print(f"  ✅ {table_name}")
        else:
            print("  ⚠️  No product tables found yet")
        
        # Check product registry
        cursor.execute('''
            SELECT product_id, table_name, schema_created, title
            FROM uspto_products 
            ORDER BY product_id
        ''')
        
        products = cursor.fetchall()
        print(f"\n📋 Registered Products ({len(products)} found):")
        if products:
            for product_id, table_name, schema_created, title in products:
                status = "✅" if schema_created else "⏳"
                print(f"  {status} {product_id} → {table_name}")
                print(f"      {title[:60]}...")
        else:
            print("  ⚠️  No products registered yet")
        
        # Check processing status
        cursor.execute('''
            SELECT COUNT(*) as total_files,
                   COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed,
                   COUNT(CASE WHEN status = 'processing' THEN 1 END) as processing,
                   COUNT(CASE WHEN status = 'error' THEN 1 END) as errors
            FROM file_processing_history
        ''')
        
        stats = cursor.fetchone()
        if stats:
            total, completed, processing, errors = stats
            print(f"\n📈 Processing Statistics:")
            print(f"  - Total Files: {total}")
            print(f"  - Completed: {completed}")
            print(f"  - Processing: {processing}")
            print(f"  - Errors: {errors}")
        
        conn.close()
        
        print("\n✅ Database check completed!")
        
    except psycopg2.Error as e:
        print(f"❌ Database error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    check_tables()
