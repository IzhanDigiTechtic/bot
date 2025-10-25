#!/usr/bin/env python3
"""
Progress Monitor for USPTO Processing
Monitors the processing progress and provides real-time updates
"""

import time
import psycopg2
import json
from pathlib import Path

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

def monitor_processing_progress():
    """Monitor processing progress in real-time"""
    
    print("USPTO Processing Progress Monitor")
    print("=" * 50)
    print("Monitoring database for processing progress...")
    print("Press Ctrl+C to stop monitoring")
    print()
    
    db_config = get_db_config()
    
    try:
        while True:
            conn = psycopg2.connect(**db_config)
            cursor = conn.cursor()
            
            # Get processing stats
            cursor.execute("""
                SELECT 
                    product_id,
                    table_name,
                    COUNT(*) as record_count,
                    MAX(batch_number) as latest_batch,
                    MAX(created_at) as last_processed
                FROM (
                    SELECT 
                        p.product_id,
                        p.table_name,
                        COALESCE(COUNT(t.id), 0) as record_count,
                        COALESCE(MAX(t.batch_number), 0) as latest_batch,
                        MAX(t.created_at) as last_processed
                    FROM uspto_products p
                    LEFT JOIN (
                        SELECT 'product_trcfeco2' as table_name, batch_number, created_at, id FROM product_trcfeco2
                        UNION ALL
                        SELECT 'product_trtdxfap' as table_name, batch_number, created_at, id FROM product_trtdxfap
                        UNION ALL
                        SELECT 'product_trtyrap' as table_name, batch_number, created_at, id FROM product_trtyrap
                        UNION ALL
                        SELECT 'product_traseco' as table_name, batch_number, created_at, id FROM product_traseco
                        UNION ALL
                        SELECT 'product_trtdxfag' as table_name, batch_number, created_at, id FROM product_trtdxfag
                        UNION ALL
                        SELECT 'product_trtyrag' as table_name, batch_number, created_at, id FROM product_trtyrag
                        UNION ALL
                        SELECT 'product_ttabtdxf' as table_name, batch_number, created_at, id FROM product_ttabtdxf
                        UNION ALL
                        SELECT 'product_ttabyr' as table_name, batch_number, created_at, id FROM product_ttabyr
                    ) t ON p.table_name = t.table_name
                    GROUP BY p.product_id, p.table_name
                ) stats
                GROUP BY product_id, table_name
                ORDER BY last_processed DESC NULLS LAST
            """)
            
            stats = cursor.fetchall()
            
            # Clear screen and show progress
            print("\033[2J\033[H")  # Clear screen
            print(f"USPTO Processing Progress - {time.strftime('%Y-%m-%d %H:%M:%S')}")
            print("=" * 70)
            
            if stats:
                print(f"{'Product':<12} {'Records':<10} {'Batches':<8} {'Last Processed':<20}")
                print("-" * 70)
                
                for product_id, table_name, record_count, latest_batch, last_processed in stats:
                    last_time = last_processed.strftime('%H:%M:%S') if last_processed else 'Never'
                    print(f"{product_id:<12} {record_count:<10} {latest_batch:<8} {last_time:<20}")
            else:
                print("No processing data found yet...")
            
            conn.close()
            
            # Wait 5 seconds before next update
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("\n\nMonitoring stopped by user.")
    except Exception as e:
        print(f"\nError monitoring progress: {e}")

def check_current_processing():
    """Check current processing status"""
    
    print("Current Processing Status")
    print("=" * 40)
    
    db_config = get_db_config()
    
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # Check recent activity
        cursor.execute("""
            SELECT 
                table_name,
                COUNT(*) as total_records,
                MAX(batch_number) as latest_batch,
                MAX(created_at) as last_activity
            FROM (
                SELECT 'product_trcfeco2' as table_name, batch_number, created_at FROM product_trcfeco2
                UNION ALL
                SELECT 'product_trtdxfap' as table_name, batch_number, created_at FROM product_trtdxfap
                UNION ALL
                SELECT 'product_trtyrap' as table_name, batch_number, created_at FROM product_trtyrap
                UNION ALL
                SELECT 'product_traseco' as table_name, batch_number, created_at FROM product_traseco
                UNION ALL
                SELECT 'product_trtdxfag' as table_name, batch_number, created_at FROM product_trtdxfag
                UNION ALL
                SELECT 'product_trtyrag' as table_name, batch_number, created_at FROM product_trtyrag
                UNION ALL
                SELECT 'product_ttabtdxf' as table_name, batch_number, created_at FROM product_ttabtdxf
                UNION ALL
                SELECT 'product_ttabyr' as table_name, batch_number, created_at FROM product_ttabyr
            ) t
            WHERE created_at > NOW() - INTERVAL '1 hour'
            GROUP BY table_name
            ORDER BY last_activity DESC
        """)
        
        recent_activity = cursor.fetchall()
        
        if recent_activity:
            print("Recent processing activity (last hour):")
            print(f"{'Table':<20} {'Records':<10} {'Batches':<8} {'Last Activity':<20}")
            print("-" * 60)
            
            for table_name, total_records, latest_batch, last_activity in recent_activity:
                last_time = last_activity.strftime('%H:%M:%S')
                print(f"{table_name:<20} {total_records:<10} {latest_batch:<8} {last_time:<20}")
        else:
            print("No recent processing activity found.")
        
        conn.close()
        
    except Exception as e:
        print(f"Error checking status: {e}")

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "monitor":
        monitor_processing_progress()
    else:
        check_current_processing()
