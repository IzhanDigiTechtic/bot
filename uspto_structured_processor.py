#!/usr/bin/env python3
"""
USPTO Structured Data Processor
Processes USPTO trademark data into properly structured database tables
based on the actual file structures analyzed.
"""

import requests
import os
import zipfile
import json
import time
import logging
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch, RealDictCursor, execute_values
import xml.etree.ElementTree as ET
import re
import hashlib
import argparse

class USPTOStructuredProcessor:
    def __init__(self, api_url="https://data.uspto.gov/ui/datasets/products/search", 
                 download_dir="./uspto_data", db_config=None):
        self.api_url = api_url
        self.download_dir = download_dir
        
        # Database configuration
        self.db_config = db_config or {
            'dbname': 'trademarks',
            'user': 'postgres',
            'password': '1234',
            'host': 'localhost',
            'port': '5432'
        }
        
        # Setup session
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json, text/plain, */*',
        })
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('uspto_structured_processor.log', encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Create directories
        os.makedirs(f"{download_dir}/zips", exist_ok=True)
        os.makedirs(f"{download_dir}/extracted", exist_ok=True)
        os.makedirs(f"{download_dir}/processed", exist_ok=True)

    def setup_database(self):
        """Connect to database and verify tables exist"""
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            # Verify the main table exists
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'trademark_case_files'
                )
            """)
            
            table_exists = cursor.fetchone()[0]
            if not table_exists:
                self.logger.error("trademark_case_files table does not exist. Please run create_schema_simple.py first.")
                return None
            
            self.logger.info("Database connection successful")
            return conn
            
        except Exception as e:
            self.logger.error(f"Database setup failed: {e}")
            return None

    def get_element_text(self, parent, tag_name):
        """Safely get text from XML element with nested search"""
        if parent is None:
            return None
            
        # Try direct tag name
        element = parent.find(tag_name)
        if element is not None and element.text:
            return element.text.strip()
        
        # Try nested search for the tag anywhere under parent
        try:
            element = parent.find(f'.//{tag_name}')
            if element is not None and element.text:
                return element.text.strip()
        except Exception:
            pass
        
        return None

    def parse_date(self, date_str):
        """Parse various date formats from USPTO data"""
        if not date_str:
            return None
        
        try:
            # Remove any non-digit characters except hyphens and slashes
            date_str = re.sub(r'[^\d/-]', '', date_str)
            
            # Handle different date formats
            for fmt in ['%Y%m%d', '%Y-%m-%d', '%m/%d/%Y']:
                try:
                    return datetime.strptime(date_str, fmt).date()
                except ValueError:
                    continue
            return None
        except:
            return None

    def process_case_file_csv(self, csv_file_path, data_source, use_sample=True):
        """Process case file CSV with proper column mapping"""
        try:
            self.logger.info(f"Processing case file CSV: {csv_file_path}")
            
            # Read CSV file
            # We'll read the CSV in chunks to reduce memory usage and improve
            # throughput. If use_sample is True and USPTO_SAMPLE_ROWS is set we
            # will only read that many rows for quick tests; otherwise we'll
            # stream the whole file.
            sample_rows_env = os.environ.get('USPTO_SAMPLE_ROWS')
            sample_nrows = int(sample_rows_env) if (use_sample and sample_rows_env and sample_rows_env.isdigit()) else None

            chunk_size_env = os.environ.get('USPTO_CSV_CHUNKSIZE')
            chunk_size = int(chunk_size_env) if chunk_size_env and chunk_size_env.isdigit() else 100000

            case_files = []
            total_rows = 0
            last_logged_percent = -1

            # If sample_nrows is specified, we only iterate until we've read that many rows
            rows_read = 0

            reader = pd.read_csv(csv_file_path, low_memory=False, chunksize=chunk_size)
            # If pandas returns an iterator, we don't know total rows up-front.
            # Try to get total rows from os.path.getsize as a rough heuristic is not helpful here,
            # so we will log progress by chunks when sample_nrows is not set.
            chunk_index = 0
            for df_chunk in reader:
                chunk_index += 1
                if sample_nrows:
                    # trim the chunk to remaining rows needed
                    remaining = sample_nrows - rows_read
                    if remaining <= 0:
                        break
                    if len(df_chunk) > remaining:
                        df_chunk = df_chunk.head(remaining)

                # Convert chunk rows to dicts
                for _, row in df_chunk.iterrows():
                    case_file = {
                        'serial_number': str(row.get('serial_no', '')) if pd.notna(row.get('serial_no')) else None,
                        'registration_number': str(row.get('registration_no', '')) if pd.notna(row.get('registration_no')) else None,
                        'filing_date': self.parse_date(str(row.get('filing_dt', '')) if pd.notna(row.get('filing_dt')) else None),
                        'registration_date': self.parse_date(str(row.get('registration_dt', '')) if pd.notna(row.get('registration_dt')) else None),
                        'status_code': None,  # This would come from XML data
                        'status_date': None,  # This would come from XML data
                        'mark_identification': str(row.get('mark_id_char', '')) if pd.notna(row.get('mark_id_char')) else None,
                        'mark_drawing_code': str(row.get('mark_draw_cd', '')) if pd.notna(row.get('mark_draw_cd')) else None,
                        
                        # Boolean flags
                        'abandon_dt': self.parse_date(str(row.get('abandon_dt', '')) if pd.notna(row.get('abandon_dt')) else None),
                        'amend_reg_dt': self.parse_date(str(row.get('amend_reg_dt', '')) if pd.notna(row.get('amend_reg_dt')) else None),
                        'amend_lb_for_app_in': bool(row.get('amend_lb_for_app_in', 0)),
                        'amend_lb_for_reg_in': bool(row.get('amend_lb_for_reg_in', 0)),
                        'amend_lb_itu_in': bool(row.get('amend_lb_itu_in', 0)),
                        'amend_lb_use_in': bool(row.get('amend_lb_use_in', 0)),
                        'reg_cancel_cd': str(row.get('reg_cancel_cd', '')) if pd.notna(row.get('reg_cancel_cd')) else None,
                        'reg_cancel_dt': self.parse_date(str(row.get('reg_cancel_dt', '')) if pd.notna(row.get('reg_cancel_dt')) else None),
                        'cancel_pend_in': bool(row.get('cancel_pend_in', 0)),
                        'cert_mark_in': bool(row.get('cert_mark_in', 0)),
                        'chg_reg_in': bool(row.get('chg_reg_in', 0)),
                        'coll_memb_mark_in': bool(row.get('coll_memb_mark_in', 0)),
                        'coll_serv_mark_in': bool(row.get('coll_serv_mark_in', 0)),
                        'coll_trade_mark_in': bool(row.get('coll_trade_mark_in', 0)),
                        'draw_color_cur_in': bool(row.get('draw_color_cur_in', 0)),
                        'draw_color_file_in': bool(row.get('draw_color_file_in', 0)),
                        'concur_use_in': bool(row.get('concur_use_in', 0)),
                        'concur_use_pend_in': bool(row.get('concur_use_pend_in', 0)),
                        'draw_3d_cur_in': bool(row.get('draw_3d_cur_in', 0)),
                        'draw_3d_file_in': bool(row.get('draw_3d_file_in', 0)),
                        'exm_attorney_name': str(row.get('exm_attorney_name', '')) if pd.notna(row.get('exm_attorney_name')) else None,
                        'lb_use_file_in': bool(row.get('lb_use_file_in', 0)),
                        'lb_for_app_cur_in': bool(row.get('lb_for_app_cur_in', 0)),
                        'lb_for_reg_cur_in': bool(row.get('lb_for_reg_cur_in', 0)),
                        'lb_intl_reg_cur_in': bool(row.get('lb_intl_reg_cur_in', 0)),
                        'lb_for_app_file_in': bool(row.get('lb_for_app_file_in', 0)),
                        'lb_for_reg_file_in': bool(row.get('lb_for_reg_file_in', 0)),
                        'lb_intl_reg_file_in': bool(row.get('lb_intl_reg_file_in', 0)),
                        'lb_none_cur_in': bool(row.get('lb_none_cur_in', 0)),
                        'for_priority_in': bool(row.get('for_priority_in', 0)),
                        'lb_itu_cur_in': bool(row.get('lb_itu_cur_in', 0)),
                        'lb_itu_file_in': bool(row.get('lb_itu_file_in', 0)),
                        'interfer_pend_in': bool(row.get('interfer_pend_in', 0)),
                        'exm_office_cd': str(row.get('exm_office_cd', '')) if pd.notna(row.get('exm_office_cd')) else None,
                        'opposit_pend_in': bool(row.get('opposit_pend_in', 0)),
                        'amend_principal_in': bool(row.get('amend_principal_in', 0)),
                        'concur_use_pub_in': bool(row.get('concur_use_pub_in', 0)),
                        'publication_dt': self.parse_date(str(row.get('publication_dt', '')) if pd.notna(row.get('publication_dt')) else None),
                        'renewal_dt': self.parse_date(str(row.get('renewal_dt', '')) if pd.notna(row.get('renewal_dt')) else None),
                        'renewal_file_in': bool(row.get('renewal_file_in', 0)),
                        'repub_12c_dt': self.parse_date(str(row.get('repub_12c_dt', '')) if pd.notna(row.get('repub_12c_dt')) else None),
                        'repub_12c_in': bool(row.get('repub_12c_in', 0)),
                        'incontest_ack_in': bool(row.get('incontest_ack_in', 0)),
                        'incontest_file_in': bool(row.get('incontest_file_in', 0)),
                        'acq_dist_in': bool(row.get('acq_dist_in', 0)),
                        'acq_dist_part_in': bool(row.get('acq_dist_part_in', 0)),
                        'use_afdv_acc_in': bool(row.get('use_afdv_acc_in', 0)),
                        'use_afdv_file_in': bool(row.get('use_afdv_file_in', 0)),
                        'use_afdv_par_acc_in': bool(row.get('use_afdv_par_acc_in', 0)),
                        'serv_mark_in': bool(row.get('serv_mark_in', 0)),
                        'std_char_claim_in': bool(row.get('std_char_claim_in', 0)),
                        'cfh_status_cd': int(row.get('cfh_status_cd', 0)) if pd.notna(row.get('cfh_status_cd')) else None,
                        'cfh_status_dt': self.parse_date(str(row.get('cfh_status_dt', '')) if pd.notna(row.get('cfh_status_dt')) else None),
                        'amend_supp_reg_in': bool(row.get('amend_supp_reg_in', 0)),
                        'supp_reg_in': bool(row.get('supp_reg_in', 0)),
                        'trade_mark_in': bool(row.get('trade_mark_in', 0)),
                        'lb_use_cur_in': bool(row.get('lb_use_cur_in', 0)),
                        'lb_none_file_in': bool(row.get('lb_none_file_in', 0)),
                        
                        # International registration fields
                        'ir_auto_reg_dt': self.parse_date(str(row.get('ir_auto_reg_dt', '')) if pd.notna(row.get('ir_auto_reg_dt')) else None),
                        'ir_first_refus_in': bool(row.get('ir_first_refus_in', 0)) if pd.notna(row.get('ir_first_refus_in')) else None,
                        'ir_death_dt': self.parse_date(str(row.get('ir_death_dt', '')) if pd.notna(row.get('ir_death_dt')) else None),
                        'ir_publication_dt': self.parse_date(str(row.get('ir_publication_dt', '')) if pd.notna(row.get('ir_publication_dt')) else None),
                        'ir_registration_dt': self.parse_date(str(row.get('ir_registration_dt', '')) if pd.notna(row.get('ir_registration_dt')) else None),
                        'ir_registration_no': str(row.get('ir_registration_no', '')) if pd.notna(row.get('ir_registration_no')) else None,
                        'ir_renewal_dt': self.parse_date(str(row.get('ir_renewal_dt', '')) if pd.notna(row.get('ir_renewal_dt')) else None),
                        'ir_status_cd': str(row.get('ir_status_cd', '')) if pd.notna(row.get('ir_status_cd')) else None,
                        'ir_status_dt': self.parse_date(str(row.get('ir_status_dt', '')) if pd.notna(row.get('ir_status_dt')) else None),
                        'ir_priority_dt': self.parse_date(str(row.get('ir_priority_dt', '')) if pd.notna(row.get('ir_priority_dt')) else None),
                        'ir_priority_in': bool(row.get('ir_priority_in', 0)) if pd.notna(row.get('ir_priority_in')) else None,
                        'related_other_in': bool(row.get('related_other_in', 0)) if pd.notna(row.get('related_other_in')) else None,
                        
                        # Metadata
                        'tad_file_id': int(row.get('tad_file_id', 0)) if pd.notna(row.get('tad_file_id')) else None,
                        'data_source': data_source + " [CSV]"
                    }
                    if case_file['serial_number'] and case_file['serial_number'] != 'nan':
                        case_files.append(case_file)

                rows_read += len(df_chunk)
                # log per-chunk progress when sampling; otherwise log chunk counts
                if sample_nrows:
                    percent = int(rows_read * 100 / sample_nrows)
                    if percent != last_logged_percent:
                        self.logger.info(f"Processing CSV progress: {percent}% ({rows_read}/{sample_nrows})")
                        last_logged_percent = percent
                else:
                    self.logger.info(f"Processed chunk {chunk_index}, rows so far: {rows_read}")

                # If sampling, stop when reached desired rows
                if sample_nrows and rows_read >= sample_nrows:
                    break
            
            self.logger.info(f"Processed {len(case_files)} case files from CSV (rows read: {rows_read})")
            return case_files
            
        except Exception as e:
            self.logger.error(f"Error processing CSV file {csv_file_path}: {e}")
            return []

    def save_case_files_to_db(self, case_files):
        """Save case files to the structured database"""
        # Accept either a list or an iterator of case_file dicts
        try:
            iterator = iter(case_files)
        except TypeError:
            self.logger.error('Provided case_files is not iterable')
            return 0

        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            insert_query = '''
                INSERT INTO trademark_case_files (
                    serial_number, registration_number, filing_date, registration_date,
                    status_code, status_date, mark_identification, mark_drawing_code,
                    abandon_dt, amend_reg_dt, amend_lb_for_app_in, amend_lb_for_reg_in,
                    amend_lb_itu_in, amend_lb_use_in, reg_cancel_cd, reg_cancel_dt,
                    cancel_pend_in, cert_mark_in, chg_reg_in, coll_memb_mark_in,
                    coll_serv_mark_in, coll_trade_mark_in, draw_color_cur_in, draw_color_file_in,
                    concur_use_in, concur_use_pend_in, draw_3d_cur_in, draw_3d_file_in,
                    exm_attorney_name, lb_use_file_in, lb_for_app_cur_in, lb_for_reg_cur_in,
                    lb_intl_reg_cur_in, lb_for_app_file_in, lb_for_reg_file_in, lb_intl_reg_file_in,
                    lb_none_cur_in, for_priority_in, lb_itu_cur_in, lb_itu_file_in,
                    interfer_pend_in, exm_office_cd, opposit_pend_in, amend_principal_in,
                    concur_use_pub_in, publication_dt, renewal_dt, renewal_file_in,
                    repub_12c_dt, repub_12c_in, incontest_ack_in, incontest_file_in,
                    acq_dist_in, acq_dist_part_in, use_afdv_acc_in, use_afdv_file_in,
                    use_afdv_par_acc_in, serv_mark_in, std_char_claim_in, cfh_status_cd, cfh_status_dt, amend_supp_reg_in,
                    supp_reg_in, trade_mark_in, lb_use_cur_in, lb_none_file_in,
                    ir_auto_reg_dt, ir_first_refus_in, ir_death_dt, ir_publication_dt,
                    ir_registration_dt, ir_registration_no, ir_renewal_dt, ir_status_cd,
                    ir_status_dt, ir_priority_dt, ir_priority_in, related_other_in,
                    tad_file_id, data_source
                ) VALUES (
                    %(serial_number)s, %(registration_number)s, %(filing_date)s, %(registration_date)s,
                    %(status_code)s, %(status_date)s, %(mark_identification)s, %(mark_drawing_code)s,
                    %(abandon_dt)s, %(amend_reg_dt)s, %(amend_lb_for_app_in)s, %(amend_lb_for_reg_in)s,
                    %(amend_lb_itu_in)s, %(amend_lb_use_in)s, %(reg_cancel_cd)s, %(reg_cancel_dt)s,
                    %(cancel_pend_in)s, %(cert_mark_in)s, %(chg_reg_in)s, %(coll_memb_mark_in)s,
                    %(coll_serv_mark_in)s, %(coll_trade_mark_in)s, %(draw_color_cur_in)s, %(draw_color_file_in)s,
                    %(concur_use_in)s, %(concur_use_pend_in)s, %(draw_3d_cur_in)s, %(draw_3d_file_in)s,
                    %(exm_attorney_name)s, %(lb_use_file_in)s, %(lb_for_app_cur_in)s, %(lb_for_reg_cur_in)s,
                    %(lb_intl_reg_cur_in)s, %(lb_for_app_file_in)s, %(lb_for_reg_file_in)s, %(lb_intl_reg_file_in)s,
                    %(lb_none_cur_in)s, %(for_priority_in)s, %(lb_itu_cur_in)s, %(lb_itu_file_in)s,
                    %(interfer_pend_in)s, %(exm_office_cd)s, %(opposit_pend_in)s, %(amend_principal_in)s,
                    %(concur_use_pub_in)s, %(publication_dt)s, %(renewal_dt)s, %(renewal_file_in)s,
                    %(repub_12c_dt)s, %(repub_12c_in)s, %(incontest_ack_in)s, %(incontest_file_in)s,
                    %(acq_dist_in)s, %(acq_dist_part_in)s, %(use_afdv_acc_in)s, %(use_afdv_file_in)s,
                    %(use_afdv_par_acc_in)s, %(serv_mark_in)s, %(std_char_claim_in)s, %(cfh_status_cd)s, %(cfh_status_dt)s, %(amend_supp_reg_in)s,
                    %(supp_reg_in)s, %(trade_mark_in)s, %(lb_use_cur_in)s, %(lb_none_file_in)s,
                    %(ir_auto_reg_dt)s, %(ir_first_refus_in)s, %(ir_death_dt)s, %(ir_publication_dt)s,
                    %(ir_registration_dt)s, %(ir_registration_no)s, %(ir_renewal_dt)s, %(ir_status_cd)s,
                    %(ir_status_dt)s, %(ir_priority_dt)s, %(ir_priority_in)s, %(related_other_in)s,
                    %(tad_file_id)s, %(data_source)s
                ) ON CONFLICT (serial_number) DO UPDATE SET
                    registration_number = EXCLUDED.registration_number,
                    filing_date = EXCLUDED.filing_date,
                    registration_date = EXCLUDED.registration_date,
                    status_code = EXCLUDED.status_code,
                    status_date = EXCLUDED.status_date,
                    mark_identification = EXCLUDED.mark_identification,
                    mark_drawing_code = EXCLUDED.mark_drawing_code,
                    updated_at = CURRENT_TIMESTAMP
            '''
            
            # Insert in batches using execute_values for speed. We will build a
            # list of tuples matching the INSERT columns for each chunk and
            # call execute_values repeatedly to stream inserts.
            batch_size_env = os.environ.get('USPTO_DB_BATCH_SIZE')
            batch_size = int(batch_size_env) if batch_size_env and batch_size_env.isdigit() else 5000

            # Column order must match the INSERT statement above
            columns = [
                'serial_number','registration_number','filing_date','registration_date',
                'status_code','status_date','mark_identification','mark_drawing_code',
                'abandon_dt','amend_reg_dt','amend_lb_for_app_in','amend_lb_for_reg_in',
                'amend_lb_itu_in','amend_lb_use_in','reg_cancel_cd','reg_cancel_dt',
                'cancel_pend_in','cert_mark_in','chg_reg_in','coll_memb_mark_in',
                'coll_serv_mark_in','coll_trade_mark_in','draw_color_cur_in','draw_color_file_in',
                'concur_use_in','concur_use_pend_in','draw_3d_cur_in','draw_3d_file_in',
                'exm_attorney_name','lb_use_file_in','lb_for_app_cur_in','lb_for_reg_cur_in',
                'lb_intl_reg_cur_in','lb_for_app_file_in','lb_for_reg_file_in','lb_intl_reg_file_in',
                'lb_none_cur_in','for_priority_in','lb_itu_cur_in','lb_itu_file_in',
                'interfer_pend_in','exm_office_cd','opposit_pend_in','amend_principal_in',
                'concur_use_pub_in','publication_dt','renewal_dt','renewal_file_in',
                'repub_12c_dt','repub_12c_in','incontest_ack_in','incontest_file_in',
                'acq_dist_in','acq_dist_part_in','use_afdv_acc_in','use_afdv_file_in',
                'use_afdv_par_acc_in','serv_mark_in','std_char_claim_in','cfh_status_cd','cfh_status_dt','amend_supp_reg_in',
                'supp_reg_in','trade_mark_in','lb_use_cur_in','lb_none_file_in',
                'ir_auto_reg_dt','ir_first_refus_in','ir_death_dt','ir_publication_dt',
                'ir_registration_dt','ir_registration_no','ir_renewal_dt','ir_status_cd',
                'ir_status_dt','ir_priority_dt','ir_priority_in','related_other_in',
                'tad_file_id','data_source'
            ]

            # If configured, prefer the COPY-based path for maximum speed.
            use_copy = os.environ.get('USPTO_USE_COPY', 'false').lower() in ('1','true','yes')

            saved = 0
            buffer = []
            total_estimated = None
            # If case_files is a list, we can estimate total; otherwise unknown
            try:
                total_estimated = len(case_files)
            except Exception:
                total_estimated = None

            if use_copy:
                # If using COPY path, we will stream rows into COPY in reasonably
                # sized batches to avoid building too much in memory.
                batch_rows = []
                for item in iterator:
                    batch_rows.append(item)
                    if len(batch_rows) >= batch_size:
                        # create a new DB connection for COPY operations to keep things clean
                        copy_conn = psycopg2.connect(**self.db_config)
                        try:
                            self.bulk_upsert_via_copy(copy_conn, batch_rows)
                            saved += len(batch_rows)
                            batch_rows = []
                            self.logger.info(f"DB save progress: {saved} rows saved so far (COPY)")
                        finally:
                            copy_conn.close()
                # flush remaining via COPY
                if batch_rows:
                    copy_conn = psycopg2.connect(**self.db_config)
                    try:
                        self.bulk_upsert_via_copy(copy_conn, batch_rows)
                        saved += len(batch_rows)
                        self.logger.info(f"DB save progress: {saved} rows saved (final) (COPY)")
                    finally:
                        copy_conn.close()
            else:
                for item in iterator:
                    # build tuple in the columns order
                    row_tuple = tuple(item.get(col) for col in columns)
                    buffer.append(row_tuple)
                    if len(buffer) >= batch_size:
                        # fast bulk insert
                        execute_values(cursor, insert_query, buffer, template=None, page_size=batch_size)
                        conn.commit()
                        saved += len(buffer)
                        buffer = []
                        if total_estimated:
                            percent = int(saved * 100 / total_estimated)
                            self.logger.info(f"DB save progress: {percent}% ({saved}/{total_estimated})")
                        else:
                            self.logger.info(f"DB save progress: {saved} rows saved so far")

            # flush remaining
            if buffer:
                execute_values(cursor, insert_query, buffer, template=None, page_size=batch_size)
                conn.commit()
                saved += len(buffer)
                buffer = []
                if total_estimated:
                    percent = int(saved * 100 / total_estimated)
                    self.logger.info(f"DB save progress: {percent}% ({saved}/{total_estimated})")
                else:
                    self.logger.info(f"DB save progress: {saved} rows saved (final)")

            count = saved
            self.logger.info(f"Saved {count} case files to database")
            
            # Log sample of saved data for verification
            if count > 0:
                sample = case_files[0]
                self.logger.info(f"Sample record - SN: {sample.get('serial_number')}, Mark: {sample.get('mark_identification')}")
            
            conn.close()
            return count
            
        except Exception as e:
            self.logger.error(f"Database save failed: {e}")
            return 0

    def bulk_upsert_via_copy(self, conn, rows, temp_table_name='tmp_trademark_case_files'):
        """Fast bulk upsert using COPY into a temp table, then INSERT ... ON CONFLICT.

        `rows` may be a list of dicts or a pandas DataFrame. This function creates
        a temporary table matching the target, streams the rows via COPY, then
        upserts into the real table. Requires sufficient DB privileges.
        """
        from io import StringIO
        cur = conn.cursor()

        # Define the same columns order we use for inserts
        columns = [
            'serial_number','registration_number','filing_date','registration_date',
            'status_code','status_date','mark_identification','mark_drawing_code',
            'abandon_dt','amend_reg_dt','amend_lb_for_app_in','amend_lb_for_reg_in',
            'amend_lb_itu_in','amend_lb_use_in','reg_cancel_cd','reg_cancel_dt',
            'cancel_pend_in','cert_mark_in','chg_reg_in','coll_memb_mark_in',
            'coll_serv_mark_in','coll_trade_mark_in','draw_color_cur_in','draw_color_file_in',
            'concur_use_in','concur_use_pend_in','draw_3d_cur_in','draw_3d_file_in',
            'exm_attorney_name','lb_use_file_in','lb_for_app_cur_in','lb_for_reg_cur_in',
            'lb_intl_reg_cur_in','lb_for_app_file_in','lb_for_reg_file_in','lb_intl_reg_file_in',
            'lb_none_cur_in','for_priority_in','lb_itu_cur_in','lb_itu_file_in',
            'interfer_pend_in','exm_office_cd','opposit_pend_in','amend_principal_in',
            'concur_use_pub_in','publication_dt','renewal_dt','renewal_file_in',
            'repub_12c_dt','repub_12c_in','incontest_ack_in','incontest_file_in',
            'acq_dist_in','acq_dist_part_in','use_afdv_acc_in','use_afdv_file_in',
            'use_afdv_par_acc_in','serv_mark_in','std_char_claim_in','cfh_status_cd','cfh_status_dt','amend_supp_reg_in',
            'supp_reg_in','trade_mark_in','lb_use_cur_in','lb_none_file_in',
            'ir_auto_reg_dt','ir_first_refus_in','ir_death_dt','ir_publication_dt',
            'ir_registration_dt','ir_registration_no','ir_renewal_dt','ir_status_cd',
            'ir_status_dt','ir_priority_dt','ir_priority_in','related_other_in',
            'tad_file_id','data_source'
        ]

        # Create temp table with minimal locking; it will be dropped at end of session
        cur.execute(f"CREATE TEMP TABLE {temp_table_name} (LIKE trademark_case_files) ON COMMIT DROP;")

        # Prepare CSV content in memory
        sio = StringIO()

        # If rows is a DataFrame, iterate rows; if it's a list/iterator of dicts, use that
        if hasattr(rows, 'to_csv') and callable(rows.to_csv):
            # DataFrame: write CSV with selected columns
            rows.to_csv(sio, columns=columns, index=False, header=False)
        else:
            # rows is an iterable of dicts
            first = True
            for r in rows:
                if first:
                    first = False
                vals = []
                for c in columns:
                    v = r.get(c)
                    if v is None:
                        vals.append('')
                    else:
                        # Basic escaping for CSV; we'll let psycopg2 COPY parse it
                        s = str(v)
                        # Replace newlines/tabs to keep CSV valid
                        s = s.replace('\n', ' ').replace('\r', ' ')
                        vals.append(s)
                sio.write('\t'.join(vals) + '\n')

        sio.seek(0)

        # COPY from STDIN using tab-delimited format (faster and safer)
        try:
            cur.copy_from(sio, temp_table_name, sep='\t', null='')
            conn.commit()
        except Exception as e:
            conn.rollback()
            cur.close()
            raise

        # Now upsert into target table
        col_list = ','.join(columns)
        conflict_cols = 'serial_number'
        update_cols = ','.join([f"{c}=EXCLUDED.{c}" for c in ['registration_number','filing_date','registration_date','status_code','status_date','mark_identification','mark_drawing_code']])

        upsert_sql = f"INSERT INTO trademark_case_files ({col_list}) SELECT {col_list} FROM {temp_table_name} ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_cols};"
        cur.execute(upsert_sql)
        conn.commit()
        cur.close()
        return True

    def fast_import_file(self, csv_path):
        """Fast import path: create UNLOGGED staging table, COPY from file, upsert to main table.

        This path is destructive to indexes for speed: it will optionally drop
        non-primary indexes, perform the load, then recreate indexes. Use with
        care. Requires DB user to have permission to create/drop indexes and
        create unlogged tables.
        """
        conn = None
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()

            staging = 'staging_trademark_case_files'

            # Create unlogged staging table
            cur.execute(f"DROP TABLE IF EXISTS {staging};")
            cur.execute(f"CREATE UNLOGGED TABLE {staging} (LIKE trademark_case_files INCLUDING ALL);")
            conn.commit()

            # COPY from the CSV file directly (server will stream from client)
            with open(csv_path, 'r', encoding='utf-8') as f:
                self.logger.info(f"COPYing data from {csv_path} into {staging}...")
                cur.copy_expert(f"COPY {staging} FROM STDIN WITH (FORMAT csv, HEADER true)", f)
                conn.commit()

            self.logger.info("COPY complete; running upsert into trademark_case_files")

            # Upsert from staging to main table
            # Use a limited set of columns for update to reduce overhead
            cols = [
                'serial_number','registration_number','filing_date','registration_date',
                'status_code','status_date','mark_identification','mark_drawing_code','tad_file_id','data_source'
            ]
            col_list = ','.join(cols)
            conflict_cols = 'serial_number'
            update_cols = ','.join([f"{c}=EXCLUDED.{c}" for c in cols if c != 'serial_number'])

            upsert_sql = f"INSERT INTO trademark_case_files ({col_list}) SELECT {col_list} FROM {staging} ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_cols};"
            cur.execute(upsert_sql)
            conn.commit()

            # Drop staging
            cur.execute(f"DROP TABLE IF EXISTS {staging};")
            conn.commit()

            self.logger.info("Fast import completed successfully")

        except Exception as e:
            if conn:
                conn.rollback()
            self.logger.error(f"Fast import failed: {e}")
        finally:
            if conn:
                conn.close()

    def process_sample_data(self):
        """Process a small sample of data to test the new structure"""
        self.logger.info("Processing sample data with new structured approach")
        
        # Setup database
        conn = self.setup_database()
        if not conn:
            self.logger.error("Database setup failed")
            return
        conn.close()
        
        # Process case file CSV with sample rows
        csv_path = "uspto_data/extracted/TRCFECO2/case_file.csv/case_file.csv"
        if os.path.exists(csv_path):
            case_files = self.process_case_file_csv(csv_path, "TRCFECO2/case_file.csv.zip")
            saved_count = self.save_case_files_to_db(case_files)
            self.logger.info(f"Successfully processed and saved {saved_count} case files")
        else:
            self.logger.warning("Case file CSV not found")

    def process_all_case_files(self):
        """Walk the extracted directory and process every `case_file.csv` found."""
        self.logger.info("Processing ALL case_file.csv files under extracted directory")

        conn = self.setup_database()
        if not conn:
            self.logger.error("Database setup failed")
            return
        conn.close()

        processed_total = 0
        # Walk through extracted folder
        extracted_root = os.path.join(self.download_dir, 'extracted')
        for root, dirs, files in os.walk(extracted_root):
            for fname in files:
                if fname.lower() == 'case_file.csv':
                    csv_path = os.path.join(root, fname)
                    # Derive a data_source label from the path (last two path components)
                    parts = os.path.normpath(csv_path).split(os.sep)
                    # attempt to use folder name containing the csv or parent folder
                    data_source = parts[-3] + '/' + parts[-2] if len(parts) >= 3 else os.path.basename(root)
                    self.logger.info(f"Found case file CSV: {csv_path} -> source: {data_source}")
                    # When processing via --all we explicitly request full processing
                    case_files = self.process_case_file_csv(csv_path, data_source, use_sample=False)
                    saved = self.save_case_files_to_db(case_files)
                    processed_total += saved

        self.logger.info(f"Finished processing all case_file.csv files. Total saved: {processed_total}")

def main():
    # Database configuration
    db_config = {
        'dbname': 'trademarks',
        'user': 'postgres',
        'password': '1234',
        'host': 'localhost',
        'port': '5432'
    }
    
    parser = argparse.ArgumentParser(description='USPTO Structured Processor')
    parser.add_argument('--all', action='store_true', help='Process all extracted case_file.csv files')
    parser.add_argument('--path', type=str, help='Process a specific CSV file path')
    args = parser.parse_args()

    processor = USPTOStructuredProcessor(db_config=db_config)

    parser.add_argument('--fast', action='store_true', help='Use fastest COPY-based import path (UNLOGGED staging, COPY, upsert).')
    args = parser.parse_args()

    if args.all:
        processor.process_all_case_files()
    elif args.path:
        if os.path.exists(args.path):
            if args.fast:
                processor.logger.info('Running FAST import (COPY-based)')
                processor.fast_import_file(args.path)
            else:
                # When user explicitly passes a path, process the whole file (ignore sample env)
                case_files = processor.process_case_file_csv(args.path, f"manual/{os.path.basename(args.path)}", use_sample=False)
                processor.save_case_files_to_db(case_files)
        else:
            processor.logger.error(f"Specified path not found: {args.path}")
    else:
        # Default: Process sample data
        processor.process_sample_data()

if __name__ == "__main__":
    main()
