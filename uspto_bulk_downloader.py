import requests
import os
import zipfile
import json
import time
import logging
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
import xml.etree.ElementTree as ET
import re
import hashlib

class USPTOPostgresDownloader:
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
                logging.FileHandler('uspto_downloader.log', encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Create directories
        os.makedirs(f"{download_dir}/zips", exist_ok=True)
        os.makedirs(f"{download_dir}/extracted", exist_ok=True)
        os.makedirs(f"{download_dir}/processed", exist_ok=True)

    def setup_database(self):
        """Create database tables and indexes if they don't exist"""
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            # Create main trademarks table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS trademarks (
                    id SERIAL PRIMARY KEY,
                    serial_number VARCHAR(20) UNIQUE,
                    registration_number VARCHAR(20),
                    mark_identification TEXT,
                    word_mark TEXT,
                    international_code VARCHAR(10),
                    filing_date DATE,
                    registration_date DATE,
                    status VARCHAR(100),
                    status_date DATE,
                    owner_name TEXT,
                    owner_address TEXT,
                    correspondent_name TEXT,
                    correspondent_address TEXT,
                    attorney_name TEXT,
                    class_code VARCHAR(10),
                    international_class VARCHAR(10),
                    goods_services TEXT,
                    design_search_code TEXT,
                    data_source VARCHAR(100),
                    file_hash VARCHAR(64),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create download history table to track downloaded files
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS download_history (
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
            
            # Create indexes for better performance
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_trademarks_serial ON trademarks(serial_number)
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_trademarks_owner ON trademarks(owner_name)
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_trademarks_filing_date ON trademarks(filing_date)
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_download_history_file ON download_history(file_name)
            ''')
            
            conn.commit()
            self.logger.info("Database setup completed successfully")
            return conn
            
        except Exception as e:
            self.logger.error(f"Database setup failed: {e}")
            return None

    def get_file_hash(self, file_path):
        """Calculate MD5 hash of a file to check if it's changed"""
        try:
            hash_md5 = hashlib.md5()
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except:
            return None

    def is_file_already_processed(self, file_name, file_size, file_hash=None):
        """Check if a file has already been downloaded and processed"""
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT file_name, file_size, file_hash, processed, processing_attempts
                FROM download_history 
                WHERE file_name = %s
            ''', (file_name,))
            
            result = cursor.fetchone()
            conn.close()
            
            if result:
                existing_name, existing_size, existing_hash, processed, attempts = result
                
                # If file has failed processing multiple times, reprocess it
                if attempts >= 3:
                    self.logger.info(f"File {file_name} has failed {attempts} times, will reprocess")
                    return False
                
                # If file hash is provided, check if it matches
                if file_hash and existing_hash:
                    return file_hash == existing_hash and processed
                # Otherwise check file size (less accurate but better than nothing)
                return file_size == existing_size and processed
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error checking file history: {e}")
            return False

    def record_downloaded_file(self, file_name, file_url, file_size, file_hash, product_id, processed=False):
        """Record a downloaded file in the history table"""
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO download_history 
                (file_name, file_url, file_size, file_hash, product_id, processed, processing_attempts)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (file_name) 
                DO UPDATE SET 
                    file_size = EXCLUDED.file_size,
                    file_hash = EXCLUDED.file_hash,
                    processed = EXCLUDED.processed,
                    download_date = CURRENT_TIMESTAMP,
                    processing_attempts = CASE 
                        WHEN download_history.processed = TRUE AND EXCLUDED.processed = FALSE THEN 1
                        ELSE download_history.processing_attempts 
                    END
            ''', (file_name, file_url, file_size, file_hash, product_id, processed, 1 if not processed else 0))
            
            conn.commit()
            conn.close()
            return True
            
        except Exception as e:
            self.logger.error(f"Error recording download history: {e}")
            return False

    def get_all_trademark_datasets(self):
        """Get all trademark datasets from the USPTO API"""
        params = {
            'facets': 'true',
            'latest': 'true', 
            'labels': 'Trademark'
        }
        
        try:
            self.logger.info("Fetching trademark datasets from USPTO API...")
            response = self.session.get(self.api_url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            self.logger.info(f"Found {data.get('count', 0)} trademark datasets")
            
            datasets = []
            for product in data.get('bulkDataProductBag', []):
                dataset_info = {
                    'product_id': product.get('productIdentifier'),
                    'title': product.get('productTitleText'),
                    'description': product.get('productDescriptionText'),
                    'frequency': product.get('productFrequencyText'),
                    'from_date': product.get('productFromDate'),
                    'to_date': product.get('productToDate'),
                    'total_size': product.get('productTotalFileSize'),
                    'file_count': product.get('productFileTotalQuantity'),
                    'last_modified': product.get('lastModifiedDateTime'),
                    'formats': product.get('mimeTypeIdentifierArrayText', []),
                    'files': []
                }
                
                # Get file information
                file_bag = product.get('productFileBag', {})
                for file_data in file_bag.get('fileDataBag', []):
                    # Only include data files (not documentation)
                    if file_data.get('fileTypeText') == 'Data':
                        file_info = {
                            'filename': file_data.get('fileName'),
                            'size': file_data.get('fileSize'),
                            'download_url': file_data.get('fileDownloadURI'),
                            'from_date': file_data.get('fileDataFromDate'),
                            'to_date': file_data.get('fileDataToDate'),
                            'file_type': file_data.get('fileTypeText'),
                            'release_date': file_data.get('fileReleaseDate')
                        }
                        dataset_info['files'].append(file_info)
                
                datasets.append(dataset_info)
                self.logger.info(f"Dataset: {dataset_info['title']} ({len(dataset_info['files'])} data files)")
            
            return datasets
            
        except Exception as e:
            self.logger.error(f"Error fetching datasets: {e}")
            return []

    def download_file(self, file_url, filename, product_id, force_redownload=False):
        """Download a single file with checks for existing files"""
        # Create product-specific directory
        product_dir = f"{self.download_dir}/zips/{product_id}"
        os.makedirs(product_dir, exist_ok=True)
        
        file_path = f"{product_dir}/{filename}"
        
        # Check if file already exists and is processed
        if os.path.exists(file_path) and not force_redownload:
            file_size = os.path.getsize(file_path)
            file_hash = self.get_file_hash(file_path)
            
            if self.is_file_already_processed(filename, file_size, file_hash):
                self.logger.info(f"File already processed: {filename}")
                return file_path
            else:
                self.logger.info(f"File exists but not processed, will process: {filename}")
                return file_path
        
        # If force_redownload is True or file doesn't exist, download it
        try:
            if force_redownload and os.path.exists(file_path):
                self.logger.info(f"Force redownloading: {filename}")
                os.remove(file_path)
            
            self.logger.info(f"Downloading: {filename}")
            self.logger.info(f"From URL: {file_url}")
            
            # Stream download to handle large files
            response = self.session.get(file_url, stream=True, timeout=60)
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            downloaded_size = 0
            
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded_size += len(chunk)
                        if total_size > 0 and downloaded_size % (1024 * 1024) == 0:
                            percent = (downloaded_size / total_size) * 100
                            mb_downloaded = downloaded_size / (1024 * 1024)
                            mb_total = total_size / (1024 * 1024)
                            self.logger.info(f"Progress: {percent:.1f}% ({mb_downloaded:.1f}MB / {mb_total:.1f}MB)")
            
            file_size = os.path.getsize(file_path)
            file_hash = self.get_file_hash(file_path)
            
            # Record download in history (not processed yet)
            self.record_downloaded_file(filename, file_url, file_size, file_hash, product_id, False)
            
            self.logger.info(f"Download completed: {filename} ({file_size} bytes)")
            return file_path
            
        except Exception as e:
            self.logger.error(f"Download failed for {filename}: {e}")
            if os.path.exists(file_path):
                os.remove(file_path)
            return None

    def parse_trademark_xml(self, xml_file_path, data_source):
        """Parse XML file and extract trademark data"""
        try:
            self.logger.info(f"Parsing XML file: {xml_file_path}")
            
            # Parse the XML file
            tree = ET.parse(xml_file_path)
            root = tree.getroot()
            
            trademarks = []
            
            # Check the root tag to determine the XML format
            root_tag = root.tag.lower()
            self.logger.info(f"XML root tag: {root_tag}")
            
            if 'assignment' in root_tag:
                # Assignment data format (assignment-entry structure)
                for assignment in root.findall('.//assignment-entry'):
                    trademark = self.extract_assignment_data(assignment, data_source)
                    if trademark and trademark.get('serial_number'):
                        trademarks.append(trademark)
            else:
                # Default: look for case-file elements
                for case_file in root.findall('.//case-file'):
                    trademark = self.extract_case_file_data(case_file, data_source)
                    if trademark and trademark.get('serial_number'):
                        trademarks.append(trademark)
            
            self.logger.info(f"Extracted {len(trademarks)} trademarks from {os.path.basename(xml_file_path)}")
            return trademarks
            
        except Exception as e:
            self.logger.error(f"Error processing {xml_file_path}: {e}")
            return []

    def extract_case_file_data(self, case_file, data_source):
        """Extract data from a case-file element"""
        trademark = {}
        
        # Basic trademark information
        trademark['serial_number'] = self.get_element_text(case_file, 'serial-number')
        trademark['registration_number'] = self.get_element_text(case_file, 'registration-number')
        
        # Mark information
        trademark['mark_identification'] = self.get_element_text(case_file, 'mark-identification')
        trademark['word_mark'] = self.get_element_text(case_file, 'word-mark')
        
        # Dates
        filing_date = self.get_element_text(case_file, 'filing-date')
        trademark['filing_date'] = self.parse_date(filing_date)
        
        registration_date = self.get_element_text(case_file, 'registration-date')
        trademark['registration_date'] = self.parse_date(registration_date)
        
        # Status information
        trademark['status'] = self.get_element_text(case_file, 'case-file-status')
        status_date = self.get_element_text(case_file, 'status-date')
        trademark['status_date'] = self.parse_date(status_date)
        
        # Owner information
        trademark['owner_name'] = self.get_element_text(case_file, 'owner-name')
        trademark['owner_address'] = self.get_owner_address(case_file)
        
        # Classification information
        trademark['class_code'] = self.get_element_text(case_file, 'class-code')
        trademark['international_class'] = self.get_element_text(case_file, 'international-code')
        
        # Goods and services
        trademark['goods_services'] = self.get_goods_services(case_file)
        
        # Data source
        trademark['data_source'] = data_source
        
        return trademark

    def extract_assignment_data(self, assignment, data_source):
        """Extract data from assignment-record element"""
        trademark = {}
        # Assignment-entry often nests identifiers under properties
        serial_no = self.get_element_text(assignment, 'serial-no') or self.get_element_text(assignment, 'serial-number')
        reg_no = self.get_element_text(assignment, 'registration-no') or self.get_element_text(assignment, 'registration-number')
        trademark['serial_number'] = serial_no
        trademark['registration_number'] = reg_no
        
        # Details
        trademark['mark_identification'] = self.get_element_text(assignment, 'mark-identification')
        trademark['owner_name'] = self.get_element_text(assignment, 'assignee-name')
        conveyance_date = self.get_element_text(assignment, 'conveyance-date') or self.get_element_text(assignment, 'recorded-date')
        trademark['filing_date'] = self.parse_date(conveyance_date)
        
        trademark['data_source'] = data_source + " [ASSIGNMENT]"
        return trademark

    def get_element_text(self, parent, tag_name):
        """Safely get text from XML element"""
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

    def get_owner_address(self, case_file):
        """Extract owner address from XML"""
        address_parts = []
        # Common owner address fields in various USPTO XMLs
        candidate_tags = [
            'owner-street', 'owner-city', 'owner-state', 'owner-country',
            'address-1', 'address-2', 'address-3', 'city', 'state', 'country'
        ]
        seen = set()
        for tag in candidate_tags:
            text = self.get_element_text(case_file, tag)
            if text and text not in seen:
                address_parts.append(text)
                seen.add(text)
        return ', '.join(address_parts) if address_parts else None

    def get_goods_services(self, case_file):
        """Extract goods and services information"""
        goods_services = case_file.find('goods-services')
        if goods_services is not None:
            return ET.tostring(goods_services, encoding='unicode', method='text')
        return None

    def process_csv_file(self, csv_file_path, data_source):
        """Process CSV files and extract trademark data"""
        try:
            self.logger.info(f"Processing CSV file: {csv_file_path}")
            
            # Read CSV file
            sample_rows_env = os.environ.get('USPTO_SAMPLE_ROWS')
            nrows = int(sample_rows_env) if sample_rows_env and sample_rows_env.isdigit() else None
            df = pd.read_csv(csv_file_path, low_memory=False, nrows=nrows)
            self.logger.info(f"CSV file has {len(df)} rows")
            
            trademarks = []
            
            # Map CSV columns to our database schema
            for index, row in df.iterrows():
                # Prefer dataset-native column names; fall back to generic variants
                serial_no = row.get('serial_no') if 'serial_no' in df.columns else row.get('serial_number')
                registration_no = row.get('registration_no') if 'registration_no' in df.columns else row.get('registration_number')
                mark_id_char = row.get('mark_id_char') if 'mark_id_char' in df.columns else row.get('mark_identification')
                filing_dt = row.get('filing_dt') if 'filing_dt' in df.columns else row.get('filing_date')
                registration_dt = row.get('registration_dt') if 'registration_dt' in df.columns else row.get('registration_date')
                status_val = row.get('cfh_status_cd') if 'cfh_status_cd' in df.columns else row.get('status')

                trademark = {
                    'serial_number': str(serial_no) if pd.notna(serial_no) else None,
                    'registration_number': str(registration_no) if pd.notna(registration_no) else None,
                    'mark_identification': str(mark_id_char) if pd.notna(mark_id_char) else None,
                    'word_mark': None,
                    'filing_date': self.parse_date(str(filing_dt) if pd.notna(filing_dt) else None),
                    'registration_date': self.parse_date(str(registration_dt) if pd.notna(registration_dt) else None),
                    'status': str(status_val) if pd.notna(status_val) else None,
                    'owner_name': None,
                    'class_code': None,
                    'international_class': None,
                    'goods_services': None,
                    'data_source': data_source + " [CSV]"
                }
                
                if trademark['serial_number'] and trademark['serial_number'] != 'nan':
                    trademarks.append(trademark)
            
            self.logger.info(f"Processed {len(trademarks)} trademarks from CSV")
            return trademarks
            
        except Exception as e:
            self.logger.error(f"Error processing CSV file {csv_file_path}: {e}")
            return []

    def save_trademarks_to_db(self, trademarks):
        """Save parsed trademarks to PostgreSQL database"""
        if not trademarks:
            self.logger.warning("No trademarks to save")
            return 0
        
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            insert_query = '''
                INSERT INTO trademarks (
                    serial_number, registration_number, mark_identification, word_mark,
                    filing_date, registration_date, status, status_date,
                    owner_name, owner_address, class_code, international_class,
                    goods_services, data_source
                ) VALUES (
                    %(serial_number)s, %(registration_number)s, %(mark_identification)s, %(word_mark)s,
                    %(filing_date)s, %(registration_date)s, %(status)s, %(status_date)s,
                    %(owner_name)s, %(owner_address)s, %(class_code)s, %(international_class)s,
                    %(goods_services)s, %(data_source)s
                ) ON CONFLICT (serial_number) DO UPDATE SET
                    registration_number = EXCLUDED.registration_number,
                    mark_identification = EXCLUDED.mark_identification,
                    word_mark = EXCLUDED.word_mark,
                    filing_date = EXCLUDED.filing_date,
                    registration_date = EXCLUDED.registration_date,
                    status = EXCLUDED.status,
                    status_date = EXCLUDED.status_date,
                    owner_name = EXCLUDED.owner_name,
                    owner_address = EXCLUDED.owner_address,
                    class_code = EXCLUDED.class_code,
                    international_class = EXCLUDED.international_class,
                    goods_services = EXCLUDED.goods_services,
                    data_source = EXCLUDED.data_source,
                    updated_at = CURRENT_TIMESTAMP
            '''
            
            execute_batch(cursor, insert_query, trademarks)
            conn.commit()
            count = len(trademarks)
            self.logger.info(f"Saved {count} trademarks to database")
            
            # Log sample of saved data for verification
            if count > 0:
                sample = trademarks[0]
                self.logger.info(f"Sample record - SN: {sample.get('serial_number')}, Mark: {sample.get('mark_identification')}")
            
            conn.close()
            return count
            
        except Exception as e:
            self.logger.error(f"Database save failed: {e}")
            return 0

    def process_downloaded_files(self, force_reprocess=False):
        """Process all downloaded but unprocessed files"""
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()

            if force_reprocess:
                # Scan filesystem for all ZIP files under the zips directory
                self.logger.info("FORCE REPROCESS: Processing all ZIP files")
                files_to_process = []
                zips_root = f"{self.download_dir}/zips"
                if os.path.exists(zips_root):
                    for product_id in os.listdir(zips_root):
                        product_dir = os.path.join(zips_root, product_id)
                        if os.path.isdir(product_dir):
                            for file in os.listdir(product_dir):
                                if file.endswith('.zip'):
                                    files_to_process.append((file, product_id))
                # If nothing found on disk, fall back to DB query (unlikely)
                if not files_to_process:
                    cursor.execute('SELECT file_name, product_id FROM download_history WHERE file_name LIKE %s', ('%.zip',))
                    files_to_process = cursor.fetchall()
            else:
                cursor.execute('SELECT file_name, product_id FROM download_history WHERE processed = FALSE')
                files_to_process = cursor.fetchall()
            conn.close()
            
            total_processed = 0
            
            for file_name, product_id in files_to_process:
                file_path = f"{self.download_dir}/zips/{product_id}/{file_name}"
                
                if os.path.exists(file_path):
                    self.logger.info(f"Processing file: {file_name}")
                    
                    # Ensure a download_history record exists so we can mark processed later
                    if force_reprocess:
                        file_size = os.path.getsize(file_path)
                        file_hash = self.get_file_hash(file_path)
                        # Attempt to insert/update a record with minimal info
                        try:
                            conn = psycopg2.connect(**self.db_config)
                            cursor = conn.cursor()
                            cursor.execute('''
                                INSERT INTO download_history (file_name, file_url, file_size, file_hash, product_id, processed, processing_attempts)
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (file_name)
                                DO UPDATE SET file_size = EXCLUDED.file_size, file_hash = EXCLUDED.file_hash, product_id = EXCLUDED.product_id
                            ''', (file_name, None, file_size, file_hash, product_id, False, 0))
                            conn.commit()
                            conn.close()
                        except Exception:
                            pass
                    
                    # Extract and parse
                    if file_name.endswith('.zip'):
                        extract_dir = f"{self.download_dir}/extracted/{product_id}/{file_name.replace('.zip', '')}"
                        os.makedirs(extract_dir, exist_ok=True)
                        
                        with zipfile.ZipFile(file_path, 'r') as zip_ref:
                            file_list = zip_ref.namelist()
                            self.logger.info(f"Extracting {len(file_list)} files from {file_name}")
                            zip_ref.extractall(extract_dir)
                        
                        # Process all files in the extracted directory
                        files_processed = 0
                        trademarks_count = 0
                        
                        for root, dirs, files in os.walk(extract_dir):
                            for file in files:
                                file_path_full = os.path.join(root, file)
                                
                                if file.endswith('.xml'):
                                    self.logger.info(f"Processing XML file: {file}")
                                    trademarks = self.parse_trademark_xml(file_path_full, f"{product_id}/{file_name}")
                                    count = self.save_trademarks_to_db(trademarks)
                                    trademarks_count += count
                                    files_processed += 1
                                    
                                elif file.endswith('.csv'):
                                    self.logger.info(f"Processing CSV file: {file}")
                                    trademarks = self.process_csv_file(file_path_full, f"{product_id}/{file_name}")
                                    count = self.save_trademarks_to_db(trademarks)
                                    trademarks_count += count
                                    files_processed += 1
                        
                        self.logger.info(f"Processed {files_processed} files with {trademarks_count} trademarks from {file_name}")
                        total_processed += trademarks_count
                    
                    # Mark file as processed
                    conn = psycopg2.connect(**self.db_config)
                    cursor = conn.cursor()
                    cursor.execute('''
                        UPDATE download_history 
                        SET processed = TRUE, last_processed = CURRENT_TIMESTAMP
                        WHERE file_name = %s
                    ''', (file_name,))
                    conn.commit()
                    conn.close()
            
            self.logger.info(f"Processing completed. Total trademarks processed: {total_processed}")
            return total_processed
            
        except Exception as e:
            self.logger.error(f"Error processing files: {e}")
            return 0

    def clean_old_zip_files(self, keep_latest_files=10):
        """Clean up old ZIP files but keep the database data"""
        try:
            self.logger.info(f"Cleaning up old ZIP files, keeping latest {keep_latest_files} files per product...")
            
            zip_dir = f"{self.download_dir}/zips"
            if not os.path.exists(zip_dir):
                return
            
            for product_id in os.listdir(zip_dir):
                product_dir = os.path.join(zip_dir, product_id)
                if os.path.isdir(product_dir):
                    # Get all ZIP files in product directory
                    zip_files = []
                    for file in os.listdir(product_dir):
                        if file.endswith('.zip'):
                            file_path = os.path.join(product_dir, file)
                            mod_time = os.path.getmtime(file_path)
                            zip_files.append((file_path, mod_time))
                    
                    # Sort by modification time (newest first)
                    zip_files.sort(key=lambda x: x[1], reverse=True)
                    
                    # Keep only the latest files
                    files_to_delete = zip_files[keep_latest_files:]
                    
                    for file_path, _ in files_to_delete:
                        self.logger.info(f"Deleting old ZIP file: {os.path.basename(file_path)}")
                        os.remove(file_path)
            
            self.logger.info("Old ZIP file cleanup completed")
            
        except Exception as e:
            self.logger.error(f"Error cleaning up old ZIP files: {e}")

    def run_smart_update(self, max_files_per_dataset=2, keep_latest_zips=10, force_reprocess=False, force_redownload=False):
        """Run smart update - only download and process new files"""
        self.logger.info("Starting smart USPTO data update")
        
        # Setup database
        conn = self.setup_database()
        if not conn:
            self.logger.error("Database connection failed")
            return
        conn.close()
        
        # Get available datasets
        datasets = self.get_all_trademark_datasets()
        if not datasets:
            self.logger.error("No datasets found")
            return
        
        # Download new files
        downloaded_count = 0
        for dataset in datasets:
            self.logger.info(f"Checking dataset: {dataset['title']}")
            
            files_to_download = dataset['files'][:max_files_per_dataset]
            
            for file_info in files_to_download:
                # Check if we need to download this file
                if force_redownload or not self.is_file_already_processed(file_info['filename'], file_info['size']):
                    result = self.download_file(
                        file_info['download_url'],
                        file_info['filename'],
                        dataset['product_id'],
                        force_redownload
                    )
                    if result:
                        downloaded_count += 1
                else:
                    self.logger.info(f"File already processed: {file_info['filename']}")
        
        self.logger.info(f"Downloaded {downloaded_count} new files")
        
        # Process downloaded files
        processed_count = self.process_downloaded_files(force_reprocess)
        
        # Clean up old ZIP files (but keep database data!)
        self.clean_old_zip_files(keep_latest_zips)
        
        self.logger.info(f"Smart update completed. Processed {processed_count} new trademarks.")

def main():
    # Database configuration
    db_config = {
        'dbname': 'trademarks',
        'user': 'postgres',
        'password': '1234',
        'host': 'localhost',
        'port': '5432'
    }
    
    downloader = USPTOPostgresDownloader(db_config=db_config)
    
    # Run smart update
    downloader.run_smart_update(
        max_files_per_dataset=2,
        keep_latest_zips=10,
        force_reprocess=True,
        force_redownload=False
    )

if __name__ == "__main__":
    main()