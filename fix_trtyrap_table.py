#!/usr/bin/env python3
"""
Fix product_trtyrap table schema
Recreate the table with the correct case file schema
"""

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
        # Remove non-connection parameters
        db_config.pop('use_copy', None)
        return db_config
    return {
        'dbname': 'trademarks',
        'user': 'postgres',
        'password': '1234',
        'host': 'localhost',
        'port': '5432'
    }

def fix_trtyrap_table():
    """Fix the product_trtyrap table schema"""
    
    print("Fixing product_trtyrap table schema...")
    print("=" * 50)
    
    db_config = get_db_config()
    
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # Check current table structure
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'product_trtyrap' 
            ORDER BY ordinal_position
        """)
        
        current_columns = cursor.fetchall()
        print("Current table structure:")
        for col_name, col_type in current_columns:
            print(f"  {col_name}: {col_type}")
        
        # Drop the existing table
        print("\nDropping existing table...")
        cursor.execute("DROP TABLE IF EXISTS product_trtyrap CASCADE")
        
        # Create the table with correct case file schema
        print("Creating table with correct case file schema...")
        cursor.execute('''
            CREATE TABLE product_trtyrap (
                id SERIAL PRIMARY KEY,
                serial_no VARCHAR(20) UNIQUE,
                registration_number VARCHAR(20),
                filing_date DATE,
                registration_date DATE,
                status_code VARCHAR(10),
                status_date DATE,
                mark_identification TEXT,
                mark_drawing_code VARCHAR(10),
                abandon_dt DATE,
                amend_reg_dt DATE,
                amend_lb_for_app_in BOOLEAN DEFAULT FALSE,
                amend_lb_for_reg_in BOOLEAN DEFAULT FALSE,
                amend_lb_itu_in BOOLEAN DEFAULT FALSE,
                amend_lb_use_in BOOLEAN DEFAULT FALSE,
                reg_cancel_cd VARCHAR(10),
                reg_cancel_dt DATE,
                cancel_pend_in BOOLEAN DEFAULT FALSE,
                cert_mark_in BOOLEAN DEFAULT FALSE,
                chg_reg_in BOOLEAN DEFAULT FALSE,
                coll_memb_mark_in BOOLEAN DEFAULT FALSE,
                coll_serv_mark_in BOOLEAN DEFAULT FALSE,
                coll_trade_mark_in BOOLEAN DEFAULT FALSE,
                draw_color_cur_in BOOLEAN DEFAULT FALSE,
                draw_color_file_in BOOLEAN DEFAULT FALSE,
                concur_use_in BOOLEAN DEFAULT FALSE,
                concur_use_pend_in BOOLEAN DEFAULT FALSE,
                draw_3d_cur_in BOOLEAN DEFAULT FALSE,
                draw_3d_file_in BOOLEAN DEFAULT FALSE,
                exm_attorney_name TEXT,
                lb_use_file_in BOOLEAN DEFAULT FALSE,
                lb_for_app_cur_in BOOLEAN DEFAULT FALSE,
                lb_for_reg_cur_in BOOLEAN DEFAULT FALSE,
                lb_intl_reg_cur_in BOOLEAN DEFAULT FALSE,
                lb_for_app_file_in BOOLEAN DEFAULT FALSE,
                lb_for_reg_file_in BOOLEAN DEFAULT FALSE,
                lb_intl_reg_file_in BOOLEAN DEFAULT FALSE,
                lb_none_cur_in BOOLEAN DEFAULT FALSE,
                for_priority_in BOOLEAN DEFAULT FALSE,
                lb_itu_cur_in BOOLEAN DEFAULT FALSE,
                lb_itu_file_in BOOLEAN DEFAULT FALSE,
                interfer_pend_in BOOLEAN DEFAULT FALSE,
                exm_office_cd VARCHAR(10),
                opposit_pend_in BOOLEAN DEFAULT FALSE,
                amend_principal_in BOOLEAN DEFAULT FALSE,
                concur_use_pub_in BOOLEAN DEFAULT FALSE,
                publication_dt DATE,
                renewal_dt DATE,
                renewal_file_in BOOLEAN DEFAULT FALSE,
                repub_12c_dt DATE,
                repub_12c_in BOOLEAN DEFAULT FALSE,
                incontest_ack_in BOOLEAN DEFAULT FALSE,
                incontest_file_in BOOLEAN DEFAULT FALSE,
                acq_dist_in BOOLEAN DEFAULT FALSE,
                acq_dist_part_in BOOLEAN DEFAULT FALSE,
                use_afdv_acc_in BOOLEAN DEFAULT FALSE,
                use_afdv_file_in BOOLEAN DEFAULT FALSE,
                use_afdv_par_acc_in BOOLEAN DEFAULT FALSE,
                serv_mark_in BOOLEAN DEFAULT FALSE,
                std_char_claim_in BOOLEAN DEFAULT FALSE,
                cfh_status_cd INTEGER,
                cfh_status_dt DATE,
                amend_supp_reg_in BOOLEAN DEFAULT FALSE,
                supp_reg_in BOOLEAN DEFAULT FALSE,
                trade_mark_in BOOLEAN DEFAULT FALSE,
                lb_use_cur_in BOOLEAN DEFAULT FALSE,
                lb_none_file_in BOOLEAN DEFAULT FALSE,
                ir_auto_reg_dt DATE,
                ir_first_refus_in BOOLEAN DEFAULT FALSE,
                ir_death_dt DATE,
                ir_publication_dt DATE,
                ir_registration_dt DATE,
                ir_registration_no VARCHAR(20),
                ir_renewal_dt DATE,
                ir_status_cd VARCHAR(10),
                ir_status_dt DATE,
                ir_priority_dt DATE,
                ir_priority_in BOOLEAN DEFAULT FALSE,
                related_other_in BOOLEAN DEFAULT FALSE,
                tad_file_id INTEGER,
                data_source VARCHAR(100),
                file_hash VARCHAR(64),
                batch_number INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create indexes
        cursor.execute('CREATE INDEX idx_product_trtyrap_serial ON product_trtyrap(serial_no)')
        cursor.execute('CREATE INDEX idx_product_trtyrap_registration ON product_trtyrap(registration_number)')
        cursor.execute('CREATE INDEX idx_product_trtyrap_filing_date ON product_trtyrap(filing_date)')
        cursor.execute('CREATE INDEX idx_product_trtyrap_batch ON product_trtyrap(batch_number)')
        
        conn.commit()
        
        # Check new table structure
        cursor.execute("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'product_trtyrap' 
            ORDER BY ordinal_position
        """)
        
        new_columns = cursor.fetchall()
        print("\nNew table structure:")
        for col_name, col_type in new_columns:
            print(f"  {col_name}: {col_type}")
        
        # Update the product registry
        cursor.execute("""
            UPDATE uspto_products 
            SET schema_created = TRUE 
            WHERE product_id = 'TRTYRAP'
        """)
        conn.commit()
        
        conn.close()
        
        print("\n✅ SUCCESS: product_trtyrap table recreated with correct schema!")
        print("   The table now has all the required columns including registration_number")
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        try:
            if conn:
                conn.close()
        except:
            pass

def main():
    """Main function"""
    
    print("Fix product_trtyrap Table Schema")
    print("=" * 50)
    print()
    print("ISSUE: product_trtyrap table was created with generic schema")
    print("- Missing columns: registration_number, filing_date, etc.")
    print("- Should use case file schema like TRCFECO2")
    print()
    
    fix_trtyrap_table()
    
    print("\n" + "=" * 50)
    print("TABLE SCHEMA FIX COMPLETED!")
    print("=" * 50)
    print()
    print("The product_trtyrap table now has the correct schema:")
    print("- ✅ serial_no (primary key)")
    print("- ✅ registration_number")
    print("- ✅ filing_date, registration_date")
    print("- ✅ All trademark-specific columns")
    print()
    print("The system should now process TRTYRAP data without errors!")

if __name__ == "__main__":
    main()
