#!/usr/bin/env python3
"""
Fix all product tables with wrong schemas
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

def fix_all_tables():
    """Fix all tables with wrong schemas"""
    
    print("Fixing All Product Tables with Wrong Schemas...")
    print("=" * 60)
    
    db_config = get_db_config()
    
    # Tables that need fixing
    tables_to_fix = [
        ('product_trtdxfap', 'TRTDXFAP', 'case_file'),
        ('product_trtdxfag', 'TRTDXFAG', 'assignment'),
        ('product_trtyrag', 'TRTYRAG', 'assignment')
    ]
    
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        for table_name, product_id, schema_type in tables_to_fix:
            print(f"\nFixing {table_name} ({product_id}) - {schema_type} schema...")
            
            # Drop existing table
            cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
            
            if schema_type == 'case_file':
                # Create case file table
                cursor.execute(f'''
                    CREATE TABLE {table_name} (
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
                cursor.execute(f'CREATE INDEX idx_{table_name}_serial ON {table_name}(serial_no)')
                cursor.execute(f'CREATE INDEX idx_{table_name}_registration ON {table_name}(registration_number)')
                cursor.execute(f'CREATE INDEX idx_{table_name}_filing_date ON {table_name}(filing_date)')
                cursor.execute(f'CREATE INDEX idx_{table_name}_batch ON {table_name}(batch_number)')
                
            elif schema_type == 'assignment':
                # Create assignment table
                cursor.execute(f'''
                    CREATE TABLE {table_name} (
                        id SERIAL PRIMARY KEY,
                        assignment_id VARCHAR(50) UNIQUE,
                        serial_no VARCHAR(20),
                        registration_number VARCHAR(20),
                        date_recorded DATE,
                        conveyance_text TEXT,
                        frame_no VARCHAR(10),
                        reel_no VARCHAR(10),
                        assignor_name TEXT,
                        assignor_address TEXT,
                        assignee_name TEXT,
                        assignee_address TEXT,
                        data_source VARCHAR(100),
                        file_hash VARCHAR(64),
                        batch_number INTEGER,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Create indexes
                cursor.execute(f'CREATE INDEX idx_{table_name}_assignment ON {table_name}(assignment_id)')
                cursor.execute(f'CREATE INDEX idx_{table_name}_serial ON {table_name}(serial_no)')
                cursor.execute(f'CREATE INDEX idx_{table_name}_date ON {table_name}(date_recorded)')
                cursor.execute(f'CREATE INDEX idx_{table_name}_batch ON {table_name}(batch_number)')
            
            # Update product registry
            cursor.execute(f"""
                UPDATE uspto_products 
                SET schema_created = TRUE 
                WHERE product_id = '{product_id}'
            """)
            
            print(f"  ✅ {table_name} recreated with {schema_type} schema")
        
        conn.commit()
        conn.close()
        
        print("\n" + "=" * 60)
        print("✅ ALL TABLES FIXED SUCCESSFULLY!")
        print("=" * 60)
        print()
        print("Fixed tables:")
        print("- product_trtdxfap: Case file schema")
        print("- product_trtdxfag: Assignment schema") 
        print("- product_trtyrag: Assignment schema")
        print()
        print("All tables now have the correct schemas for their product types!")
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        try:
            if conn:
                conn.close()
        except:
            pass

def main():
    """Main function"""
    
    print("Fix All Product Tables with Wrong Schemas")
    print("=" * 60)
    print()
    print("ISSUE: Several tables were created with generic schema")
    print("- product_trtdxfap: Should be case file schema")
    print("- product_trtdxfag: Should be assignment schema")
    print("- product_trtyrag: Should be assignment schema")
    print()
    
    fix_all_tables()

if __name__ == "__main__":
    main()
