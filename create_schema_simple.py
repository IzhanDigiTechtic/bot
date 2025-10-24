#!/usr/bin/env python3
"""
Create the USPTO database schema - simplified approach
"""

import psycopg2

def create_schema():
    """Create the database schema"""
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
        
        # Drop existing tables
        print("Dropping existing tables...")
        cursor.execute("DROP TABLE IF EXISTS download_history CASCADE")
        cursor.execute("DROP TABLE IF EXISTS trademark_case_files CASCADE")
        cursor.execute("DROP TABLE IF EXISTS trademark_assignments CASCADE")
        cursor.execute("DROP TABLE IF EXISTS trademark_owners CASCADE")
        cursor.execute("DROP TABLE IF EXISTS trademark_correspondents CASCADE")
        cursor.execute("DROP TABLE IF EXISTS trademark_classifications CASCADE")
        cursor.execute("DROP TABLE IF EXISTS trademark_events CASCADE")
        cursor.execute("DROP TABLE IF EXISTS trademark_statements CASCADE")
        
        # Create download history table
        print("Creating download_history table...")
        cursor.execute('''
            CREATE TABLE download_history (
                id SERIAL PRIMARY KEY,
                file_name VARCHAR(255) UNIQUE,
                file_url TEXT,
                file_size BIGINT,
                download_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                file_hash VARCHAR(64),
                product_id VARCHAR(50),
                processed BOOLEAN DEFAULT FALSE,
                last_processed TIMESTAMP,
                processing_attempts INTEGER DEFAULT 0
            )
        ''')
        
        # Create main trademark case files table
        print("Creating trademark_case_files table...")
        cursor.execute('''
            CREATE TABLE trademark_case_files (
                id SERIAL PRIMARY KEY,
                serial_number VARCHAR(20) UNIQUE,
                registration_number VARCHAR(20),
                transaction_date DATE,
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
                ir_first_refus_in BOOLEAN,
                ir_death_dt DATE,
                ir_publication_dt DATE,
                ir_registration_dt DATE,
                ir_registration_no VARCHAR(20),
                ir_renewal_dt DATE,
                ir_status_cd VARCHAR(10),
                ir_status_dt DATE,
                ir_priority_dt DATE,
                ir_priority_in BOOLEAN,
                related_other_in BOOLEAN,
                tad_file_id INTEGER,
                data_source VARCHAR(100),
                file_hash VARCHAR(64),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create indexes
        print("Creating indexes...")
        cursor.execute('CREATE INDEX idx_trademark_case_files_serial ON trademark_case_files(serial_number)')
        cursor.execute('CREATE INDEX idx_trademark_case_files_registration ON trademark_case_files(registration_number)')
        cursor.execute('CREATE INDEX idx_trademark_case_files_filing_date ON trademark_case_files(filing_date)')
        cursor.execute('CREATE INDEX idx_trademark_case_files_status ON trademark_case_files(status_code)')
        
        # mark_identification is TEXT and can be very large. Creating a btree
        # index on the raw TEXT column may exceed PostgreSQL index row size limits.
        # Use an expression index on md5(mark_identification) which stores a
        # fixed-size hash and is safe to index for equality/lookup use-cases.
        cursor.execute('CREATE INDEX idx_trademark_case_files_mark_id ON trademark_case_files (md5(mark_identification))')
        
        conn.commit()
        print("Database schema created successfully!")
        
        # Verify tables were created
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name LIKE 'trademark_%'
            ORDER BY table_name
        """)
        
        tables = cursor.fetchall()
        print(f"\nCreated {len(tables)} tables:")
        for table in tables:
            print(f"  - {table[0]}")
        
        conn.close()
        
    except Exception as e:
        print(f"Error creating schema: {e}")

if __name__ == "__main__":
    create_schema()