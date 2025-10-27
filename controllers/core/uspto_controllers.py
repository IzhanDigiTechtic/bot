#!/usr/bin/env python3
"""
USPTO Data Processing Controllers
Separate controllers for different responsibilities that can be merged into one optimized process
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
import gc
from typing import Dict, List, Optional, Tuple, Any, Generator
from dataclasses import dataclass
from pathlib import Path
import threading
from queue import Queue
import multiprocessing as mp
from abc import ABC, abstractmethod

@dataclass
class ProductInfo:
    """Information about a USPTO product dataset"""
    product_id: str
    title: str
    description: str
    frequency: str
    from_date: str
    to_date: str
    total_size: int
    file_count: int
    last_modified: str
    formats: List[str]
    files: List[Dict[str, Any]]

@dataclass
class FileInfo:
    """Information about a file to be processed"""
    filename: str
    size: int
    download_url: str
    from_date: str
    to_date: str
    file_type: str
    release_date: str
    last_modified: str
    product_id: str

@dataclass
class ProcessingResult:
    """Result of a processing operation"""
    success: bool
    rows_processed: int
    rows_saved: int
    error_message: Optional[str] = None
    processing_time: float = 0.0

class BaseController(ABC):
    """Base controller class with common functionality"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = self._setup_logging()
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging for the controller"""
        logger = logging.getLogger(f"{self.__class__.__name__}")
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    @abstractmethod
    def initialize(self) -> bool:
        """Initialize the controller"""
        pass
    
    @abstractmethod
    def cleanup(self):
        """Cleanup resources"""
        pass

class USPTOAPIController(BaseController):
    """Controller for USPTO API interactions"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.session = None
        self.api_url = config.get('api_url', 'https://data.uspto.gov/ui/datasets/products/search')
    
    def initialize(self) -> bool:
        """Initialize API session"""
        try:
            self.session = requests.Session()
            self.session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/json, text/plain, */*',
            })
            self.logger.info("API Controller initialized successfully")
            return True
        except Exception as e:
            self.logger.error(f"Failed to initialize API controller: {e}")
            return False
    
    def cleanup(self):
        """Cleanup API session"""
        if self.session:
            self.session.close()
    
    def get_trademark_datasets(self) -> List[ProductInfo]:
        """Get all trademark datasets from USPTO API"""
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
            
            products = []
            for product in data.get('bulkDataProductBag', []):
                product_info = ProductInfo(
                    product_id=product.get('productIdentifier'),
                    title=product.get('productTitleText'),
                    description=product.get('productDescriptionText'),
                    frequency=product.get('productFrequencyText'),
                    from_date=product.get('productFromDate'),
                    to_date=product.get('productToDate'),
                    total_size=product.get('productTotalFileSize'),
                    file_count=product.get('productFileTotalQuantity'),
                    last_modified=product.get('lastModifiedDateTime'),
                    formats=product.get('mimeTypeIdentifierArrayText', []),
                    files=[]
                )
                
                # Get file information
                file_bag = product.get('productFileBag', {})
                for file_data in file_bag.get('fileDataBag', []):
                    if file_data.get('fileTypeText') == 'Data':
                        file_info = FileInfo(
                            filename=file_data.get('fileName'),
                            size=file_data.get('fileSize'),
                            download_url=file_data.get('fileDownloadURI'),
                            from_date=file_data.get('fileDataFromDate'),
                            to_date=file_data.get('fileDataToDate'),
                            file_type=file_data.get('fileTypeText'),
                            release_date=file_data.get('fileReleaseDate'),
                            last_modified=file_data.get('fileLastModifiedDateTime'),
                            product_id=product_info.product_id
                        )
                        product_info.files.append(file_info)
                
                products.append(product_info)
                self.logger.info(f"Product: {product_info.title} ({len(product_info.files)} data files)")
            
            return products
            
        except Exception as e:
            self.logger.error(f"Error fetching datasets: {e}")
            return []
    
    def check_file_status(self, file_info: FileInfo) -> Dict[str, Any]:
        """Check if file needs to be downloaded/processed"""
        # This would typically check against a database or cache
        # For now, return a simple status
        return {
            'needs_download': True,
            'needs_processing': True,
            'last_processed': None
        }

class DownloadController(BaseController):
    """Controller for file download management"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.download_dir = Path(config.get('download_dir', './uspto_data'))
        self.session = None
    
    def initialize(self) -> bool:
        """Initialize download controller"""
        try:
            # Create directories
            (self.download_dir / "zips").mkdir(parents=True, exist_ok=True)
            (self.download_dir / "extracted").mkdir(parents=True, exist_ok=True)
            
            # Setup session
            self.session = requests.Session()
            self.session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            })
            
            self.logger.info("Download Controller initialized successfully")
            return True
        except Exception as e:
            self.logger.error(f"Failed to initialize download controller: {e}")
            return False
    
    def cleanup(self):
        """Cleanup download resources"""
        if self.session:
            self.session.close()
    
    def download_file(self, file_info: FileInfo, force_redownload: bool = False) -> Optional[Path]:
        """Download a file with progress tracking"""
        filename = file_info.filename
        download_url = file_info.download_url
        
        # Create product-specific directory
        product_dir = self.download_dir / "zips" / file_info.product_id
        product_dir.mkdir(parents=True, exist_ok=True)
        
        file_path = product_dir / filename
        
        # Check if file exists and is complete
        if file_path.exists() and not force_redownload:
            file_size = file_path.stat().st_size
            if file_size == file_info.size:
                self.logger.info(f"File {filename} already exists and is complete")
                return file_path
            else:
                self.logger.info(f"File {filename} exists but size mismatch, redownloading")
                file_path.unlink()
        
        try:
            self.logger.info(f"Downloading {filename} ({file_info.size} bytes)")
            
            # Stream download
            response = self.session.get(download_url, stream=True, timeout=60)
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            downloaded_size = 0
            
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded_size += len(chunk)
                        
                        # Log progress every 10MB
                        if downloaded_size % (10 * 1024 * 1024) == 0:
                            percent = (downloaded_size / total_size) * 100 if total_size > 0 else 0
                            mb_downloaded = downloaded_size / (1024 * 1024)
                            self.logger.info(f"Download progress: {percent:.1f}% ({mb_downloaded:.1f}MB)")
            
            # Verify download
            actual_size = file_path.stat().st_size
            if actual_size != file_info.size:
                raise Exception(f"Download size mismatch: expected {file_info.size}, got {actual_size}")
            
            self.logger.info(f"Download completed: {filename}")
            return file_path
            
        except Exception as e:
            self.logger.error(f"Download failed for {filename}: {e}")
            if file_path.exists():
                file_path.unlink()
            return None
    
    def extract_zip_file(self, zip_path: Path, product_id: str) -> Optional[Path]:
        """Extract ZIP file and return extraction directory"""
        try:
            extract_dir = self.download_dir / "extracted" / product_id / zip_path.stem
            extract_dir.mkdir(parents=True, exist_ok=True)
            
            # Check if already extracted and has data files
            if extract_dir.exists():
                csv_files = list(extract_dir.rglob('*.csv'))
                xml_files = list(extract_dir.rglob('*.xml'))
                if csv_files or xml_files:
                    self.logger.info(f"Files already extracted to {extract_dir} ({len(csv_files)} CSV, {len(xml_files)} XML files)")
                    return extract_dir
            
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)
            
            self.logger.info(f"Extracted {zip_path.name} to {extract_dir}")
            return extract_dir
            
        except Exception as e:
            self.logger.error(f"Error extracting {zip_path}: {e}")
            return None
    
    def find_data_files(self, directory: Path) -> List[Path]:
        """Find data files (CSV, XML) in directory"""
        data_files = []
        for file_path in directory.rglob('*'):
            if file_path.is_file() and file_path.suffix.lower() in ['.csv', '.xml']:
                data_files.append(file_path)
        return data_files
    
    def check_extracted_files_exist(self, file_info: FileInfo) -> Optional[Path]:
        """Check if extracted files already exist for a given file"""
        zip_filename = file_info.filename
        if not zip_filename.endswith('.zip'):
            return None
        
        # Expected extraction directory path
        extract_dir = self.download_dir / "extracted" / file_info.product_id / zip_filename.replace('.zip', '')
        
        # Check if directory exists and has data files
        if extract_dir.exists():
            csv_files = list(extract_dir.rglob('*.csv'))
            xml_files = list(extract_dir.rglob('*.xml'))
            if csv_files or xml_files:
                self.logger.info(f"Found existing extracted files in {extract_dir} ({len(csv_files)} CSV, {len(xml_files)} XML)")
                return extract_dir
        
        return None

class ProcessingController(BaseController):
    """Controller for data processing and transformation"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.batch_size = config.get('batch_size', 10000)
        self.chunk_size = config.get('chunk_size', 50000)
        self.memory_limit_mb = config.get('memory_limit_mb', 512)
    
    def initialize(self) -> bool:
        """Initialize processing controller"""
        try:
            self.logger.info("Processing Controller initialized successfully")
            return True
        except Exception as e:
            self.logger.error(f"Failed to initialize processing controller: {e}")
            return False
    
    def cleanup(self):
        """Cleanup processing resources"""
        gc.collect()
    
    def process_csv_file(self, file_path: Path, product_id: str) -> Generator[List[Dict], None, None]:
        """Process CSV file in batches with proper chunking for large files"""
        try:
            self.logger.info(f"Processing CSV file: {file_path}")
            
            # Check file size
            file_size = file_path.stat().st_size
            large_file_threshold = 100 * 1024 * 1024  # 100MB
            
            if file_size > large_file_threshold:
                self.logger.info(f"Large CSV file detected ({file_size / (1024*1024):.1f}MB), using chunked processing")
                yield from self._process_large_csv_file(file_path, product_id)
            else:
                self.logger.info(f"Small CSV file detected ({file_size / (1024*1024):.1f}MB), using regular processing")
                yield from self._process_small_csv_file(file_path, product_id)
                
        except Exception as e:
            self.logger.error(f"Error processing CSV {file_path}: {e}")
            yield []
    
    def _process_large_csv_file(self, file_path: Path, product_id: str) -> Generator[List[Dict], None, None]:
        """Process large CSV files using chunked reading"""
        try:
            batch_count = 0
            total_records = 0
            
            # Use pandas chunked reading for large files
            chunk_size = min(self.chunk_size, 50000)  # Limit chunk size for memory
            self.logger.info(f"Using CSV chunk size: {chunk_size}")
            
            for chunk_df in pd.read_csv(file_path, chunksize=chunk_size, low_memory=False):
                batch_records = []
                
                # For TRCFECO2, log first chunk to debug column issues
                if product_id == 'TRCFECO2' and batch_count == 0:
                    self.logger.info(f"TRCFECO2 CSV columns: {list(chunk_df.columns)}")
                    self.logger.info(f"Number of columns: {len(chunk_df.columns)}")
                    self.logger.info(f"First row sample: {chunk_df.iloc[0].to_dict()}")
                
                # Process chunk
                for _, row in chunk_df.iterrows():
                    record = row.to_dict()
                    cleaned_record = self._clean_record(record, product_id)
                    if cleaned_record:
                        batch_records.append(cleaned_record)
                
                # Yield batch when full
                if len(batch_records) >= self.batch_size:
                    batch_count += 1
                    total_records += len(batch_records)
                    self.logger.info(f"Yielding CSV batch {batch_count} with {len(batch_records)} records (total: {total_records})")
                    yield batch_records
                    batch_records = []
                
                # Progress reporting every 10 chunks
                if batch_count > 0 and batch_count % 10 == 0:
                    self.logger.info(f"Processed {total_records} CSV records so far...")
            
            # Yield remaining records
            if batch_records:
                batch_count += 1
                total_records += len(batch_records)
                self.logger.info(f"Yielding final CSV batch {batch_count} with {len(batch_records)} records (total: {total_records})")
                yield batch_records
            
            self.logger.info(f"CSV processing complete. Total records: {total_records}")
                
        except Exception as e:
            self.logger.error(f"Error in chunked CSV processing: {e}")
            raise
    
    def _process_small_csv_file(self, file_path: Path, product_id: str) -> Generator[List[Dict], None, None]:
        """Process small CSV files using regular processing"""
        try:
            batch = []
            
            # Read CSV normally for small files
            df = pd.read_csv(file_path, low_memory=False)
            
            for _, row in df.iterrows():
                record = row.to_dict()
                cleaned_record = self._clean_record(record, product_id)
                if cleaned_record:
                    batch.append(cleaned_record)
                    
                    # Yield batch when full
                    if len(batch) >= self.batch_size:
                        yield batch
                        batch = []
            
            # Yield remaining records
            if batch:
                yield batch
                
        except Exception as e:
            self.logger.error(f"Error processing small CSV file: {e}")
            raise
    
    def process_xml_file(self, file_path: Path, product_id: str) -> Generator[List[Dict], None, None]:
        """Process XML file in batches with proper chunking for large files"""
        try:
            self.logger.info(f"Processing XML file: {file_path}")
            
            # Check file size
            file_size = file_path.stat().st_size
            large_file_threshold = 100 * 1024 * 1024  # 100MB
            
            if file_size > large_file_threshold:
                self.logger.info(f"Large file detected ({file_size / (1024*1024):.1f}MB), using iterative parsing")
                yield from self._process_large_xml_iteratively(file_path, product_id)
            else:
                self.logger.info(f"Small file detected ({file_size / (1024*1024):.1f}MB), using regular parsing")
                yield from self._process_small_xml_file(file_path, product_id)
                
        except Exception as e:
            self.logger.error(f"Error processing XML {file_path}: {e}")
            yield []
    
    def _process_large_xml_iteratively(self, file_path: Path, product_id: str) -> Generator[List[Dict], None, None]:
        """Process large XML files using iterative parsing with progress reporting"""
        try:
            batch = []
            record_count = 0
            batch_count = 0
            
            # Determine which elements to look for based on product type
            target_elements = self._get_target_elements(product_id)
            
            self.logger.info(f"Looking for XML elements: {target_elements}")
            
            # Parse XML iteratively
            for event, elem in ET.iterparse(file_path, events=('end',)):
                if elem.tag in target_elements:
                    record = self._extract_record(elem, product_id)
                    if record:
                        batch.append(record)
                        record_count += 1
                        
                        # Yield batch when full
                        if len(batch) >= self.batch_size:
                            batch_count += 1
                            self.logger.info(f"Yielding batch {batch_count} with {len(batch)} records (total: {record_count})")
                            yield batch
                            batch = []
                
                # Clear element to save memory
                elem.clear()
                
                # Progress reporting every 10000 elements
                if record_count > 0 and record_count % 10000 == 0:
                    self.logger.info(f"Processed {record_count} records so far...")
            
            # Yield remaining records
            if batch:
                batch_count += 1
                self.logger.info(f"Yielding final batch {batch_count} with {len(batch)} records (total: {record_count})")
                yield batch
            
            self.logger.info(f"XML processing complete. Total records: {record_count}")
                
        except Exception as e:
            self.logger.error(f"Error in iterative XML processing: {e}")
            raise
    
    def _process_small_xml_file(self, file_path: Path, product_id: str) -> Generator[List[Dict], None, None]:
        """Process small XML files using regular parsing"""
        try:
            batch = []
            
            # Parse XML normally for small files
            tree = ET.parse(file_path)
            root = tree.getroot()
            
            # Find record elements based on product type
            target_elements = self._get_target_elements(product_id)
            
            for elem in root.iter():
                if elem.tag in target_elements:
                    record = self._extract_record(elem, product_id)
                    if record:
                        batch.append(record)
                        
                        # Yield batch when full
                        if len(batch) >= self.batch_size:
                            yield batch
                            batch = []
            
            # Yield remaining records
            if batch:
                yield batch
                
        except Exception as e:
            self.logger.error(f"Error processing small XML file: {e}")
            raise
    
    def _get_target_elements(self, product_id: str) -> List[str]:
        """Get target XML elements based on product type"""
        element_mapping = {
            'TRTYRAG': ['assignment-entry'],
            'TRTDXFAG': ['assignment-entry'],
            'TRTDXFAP': ['case-file', 'trademark-application'],
            'TRTYRAP': ['case-file', 'trademark-application'],
            'TTABTDXF': ['proceeding', 'ttab-proceeding'],
            'TTABYR': ['proceeding', 'ttab-proceeding']
        }
        
        return element_mapping.get(product_id, ['record', 'item'])
    
    def _clean_record(self, record: Dict, product_id: str) -> Optional[Dict]:
        """Clean and normalize a record with proper column mapping"""
        try:
            # Always apply mapping to convert CSV column names to database column names
            # For TRCFECO2, we need to map specific columns
            if product_id == 'TRCFECO2':
                mapped_record = self._map_trcfeco2_columns(record)
            else:
                # Map column names to match database schema for other products
                mapped_record = self._map_column_names(record)
            
            cleaned = {}
            for key, value in mapped_record.items():
                # Skip None/NaN values immediately
                if pd.isna(value):
                    cleaned[key] = None
                    continue
                
                # Convert to string and check if empty
                str_value = str(value).strip()
                if str_value == '' or str_value.lower() in ['nan', 'none', 'null']:
                    cleaned[key] = None
                    continue
                
                # For date columns (_dt or _date), treat empty strings as None
                if (key.endswith('_dt') or key.endswith('_date')) and (not str_value or str_value in ['', '0000-00-00', 'nan']):
                    cleaned[key] = None
                    continue
                
                # Convert values based on their type and column name
                converted = self._convert_value(value, key)
                # Double-check for empty strings after conversion
                if isinstance(converted, str) and converted.strip() == '':
                    cleaned[key] = None
                else:
                    cleaned[key] = converted
            
            # Add metadata
            cleaned['data_source'] = f"{product_id} [CSV]"
            cleaned['batch_number'] = 0  # Will be set by database controller
            
            return cleaned
            
        except Exception as e:
            self.logger.error(f"Error cleaning record: {e}")
            return None
    
    def _map_trcfeco2_columns(self, record: Dict) -> Dict:
        """Map TRCFECO2 CSV column names to database column names"""
        # TRCFECO2 specific column mappings
        # Only map columns that have different names in database vs CSV
        column_mappings = {
            'mark_id_char': 'mark_identification',
            'registration_no': 'registration_number',
            'filing_dt': 'filing_date',
            'registration_dt': 'registration_date',
            # Skip file_location as it's not in database schema
        }
        
        mapped_record = {}
        for key, value in record.items():
            # Map the key if it exists in our mappings
            if key in column_mappings:
                mapped_key = column_mappings[key]
                mapped_record[mapped_key] = value
            else:
                # Keep the original key if no mapping exists
                # But skip 'file_location' as it's not in database
                if key != 'file_location':
                    mapped_record[key] = value
        
        return mapped_record
    
    def _map_column_names(self, record: Dict) -> Dict:
        """Map column names to match database schema"""
        
        # Column name mappings for TRCFECO2 CSV files
        column_mappings = {
            # TRCFECO2 specific mappings
            'file_location': 'file_location_cd',  # This is the current location field
            'exm_attorney_name': 'exm_attorney_name',  # This is correct as-is
            'filing_dt': 'filing_dt',  # Already correct
            'publication_dt': 'publication_dt',  # Already correct  
            'registration_dt': 'registration_dt',  # Already correct
            'registration_no': 'registration_number',  # Map to correct database column
            'serial_no': 'serial_no',  # Already correct
            
            # Generic mappings for other products
            'serial_number': 'serial_no',
            'filing_date': 'filing_dt',
            'registration_date': 'registration_dt',
            'mark_identification': 'mark_id_char',
            'mark_drawing_code': 'mark_draw_cd',
            'abandon_date': 'abandon_dt',
            'amend_registration_date': 'amend_reg_dt',
            'reg_cancel_code': 'reg_cancel_cd',
            'reg_cancel_date': 'reg_cancel_dt',
            'examiner_attorney_name': 'exm_attorney_name',
            'file_location_date': 'file_location_dt',
            'publication_date': 'publication_dt',
            'renewal_date': 'renewal_dt',
            'repub_12c_date': 'repub_12c_dt',
            'cfh_status_code': 'cfh_status_cd',
            'cfh_status_date': 'cfh_status_dt',
            'ir_auto_registration_date': 'ir_auto_reg_dt',
            'ir_first_refusal_in': 'ir_first_refus_in',
            'ir_death_date': 'ir_death_dt',
            'ir_publication_date': 'ir_publication_dt',
            'ir_registration_date': 'ir_registration_dt',
            'ir_registration_number': 'ir_registration_no',
            'ir_renewal_date': 'ir_renewal_dt',
            'ir_status_code': 'ir_status_cd',
            'ir_status_date': 'ir_status_dt',
            'ir_priority_date': 'ir_priority_dt',
        }
        
        mapped_record = {}
        for key, value in record.items():
            # Clean the key first
            clean_key = key.strip().lower().replace(' ', '_').replace('-', '_')
            
            # Apply mapping if exists
            if clean_key in column_mappings:
                mapped_record[column_mappings[clean_key]] = value
            else:
                mapped_record[clean_key] = value
        
        return mapped_record
    
    def _convert_value(self, value, column_name: str):
        """Convert value to appropriate type based on column name and value"""
        try:
            # Handle boolean columns (ending with _in)
            if column_name.endswith('_in'):
                if pd.isna(value):
                    return None
                # Convert float/int to boolean
                if isinstance(value, (int, float)):
                    return bool(value)
                # Convert string representations
                if str(value).lower() in ['true', '1', '1.0', 'yes', 'y']:
                    return True
                elif str(value).lower() in ['false', '0', '0.0', 'no', 'n']:
                    return False
                return None
            
            # Handle date columns (ending with _dt or _date)
            elif column_name.endswith('_dt') or column_name.endswith('_date'):
                if pd.isna(value):
                    return None
                # Convert pandas timestamp to date string
                if hasattr(value, 'date'):
                    return value.date()
                # Handle string dates
                elif isinstance(value, str):
                    stripped = value.strip()
                    # Return None for empty strings to avoid PostgreSQL date parsing errors
                    if stripped == '' or stripped == '0000-00-00':
                        return None
                    return stripped
                return None
            
            # Handle integer columns (specific columns)
            elif column_name in ['serial_no', 'registration_number', 'registration_no', 'tad_file_id', 'cfh_status_cd', 'mark_draw_cd']:
                if pd.isna(value):
                    return None
                if isinstance(value, (int, float)):
                    return int(value) if not pd.isna(value) else None
                str_val = str(value).strip()
                return int(str_val) if str_val and str_val != 'nan' else None
            
            # Handle all other columns as TEXT (safe approach)
            else:
                if pd.isna(value):
                    return None
                return str(value).strip() if str(value).strip() else None
                
        except Exception as e:
            self.logger.error(f"Error converting value {value} for column {column_name}: {e}")
            return None
    
    def _extract_record(self, element, product_id: str) -> Optional[Dict]:
        """Extract record from XML element"""
        try:
            # Special handling for assignment XML files (TRTYRAG, TRTDXFAG)
            if product_id in ['TRTYRAG', 'TRTDXFAG'] and element.tag == 'assignment-entry':
                return self._extract_assignment_record(element, product_id)
            # Special handling for case-file XML (applications) for TRTDXFAP/TRTYRAP
            if product_id in ['TRTDXFAP', 'TRTYRAP'] and element.tag == 'case-file':
                return self._extract_case_file_record(element, product_id)
            
            # Default extraction for other XML types
            record = {}
            
            # Extract basic fields
            for child in element:
                if child.text and child.text.strip():
                    key = child.tag.replace('-', '_').lower()
                    record[key] = child.text.strip()
            
            # Add metadata
            record['data_source'] = f"{product_id} [XML]"
            record['batch_number'] = 0  # Will be set by database controller
            
            return record
            
        except Exception as e:
            self.logger.error(f"Error extracting record: {e}")
            return None
    
    def _extract_assignment_record(self, assignment_elem, product_id: str) -> Optional[Dict]:
        """Extract assignment record from XML element"""
        
        record = {}
        
        try:
            # Extract assignment data
            assignment = assignment_elem.find('assignment')
            if assignment is not None:
                record['reel_no'] = self._get_xml_text(assignment.find('reel-no'))
                record['frame_no'] = self._get_xml_text(assignment.find('frame-no'))
                # Normalize dates from YYYYMMDD -> YYYY-MM-DD
                record['date_recorded'] = self._normalize_xml_date(self._get_xml_text(assignment.find('date-recorded')))
                record['conveyance_text'] = self._get_xml_text(assignment.find('conveyance-text'))
                record['last_update_date'] = self._normalize_xml_date(self._get_xml_text(assignment.find('last-update-date')))
                record['purge_indicator'] = self._get_xml_text(assignment.find('purge-indicator'))
                page_count_text = self._get_xml_text(assignment.find('page-count'))
                record['page_count'] = int(page_count_text) if page_count_text and page_count_text.isdigit() else None
                
                # Extract correspondent data
                correspondent = assignment.find('correspondent')
                if correspondent is not None:
                    record['correspondent_name'] = self._get_xml_text(correspondent.find('person-or-organization-name'))
                    addr1 = self._get_xml_text(correspondent.find('address-1'))
                    addr2 = self._get_xml_text(correspondent.find('address-2'))
                    addr3 = self._get_xml_text(correspondent.find('address-3'))
                    addr4 = self._get_xml_text(correspondent.find('address-4'))
                    record['correspondent_address_1'] = addr1
                    record['correspondent_address_2'] = addr2
                    # Merge address-3 and address-4 into address_3 to fit schema
                    merged_addr3 = ', '.join([part for part in [addr3, addr4] if part]) if (addr3 or addr4) else None
                    record['correspondent_address_3'] = merged_addr3
            
            # Extract assignor data (take first assignor)
            assignors = assignment_elem.find('assignors')
            if assignors is not None:
                assignor = assignors.find('assignor')
                if assignor is not None:
                    record['assignor_name'] = self._get_xml_text(assignor.find('person-or-organization-name'))
                    # Build single assignor_address as per schema
                    a_addr1 = self._get_xml_text(assignor.find('address-1'))
                    a_addr2 = self._get_xml_text(assignor.find('address-2'))
                    a_city = self._get_xml_text(assignor.find('city'))
                    a_state = self._get_xml_text(assignor.find('state'))
                    a_post = self._get_xml_text(assignor.find('postcode'))
                    assignor_parts = [a_addr1, a_addr2, a_city, a_state, a_post]
                    record['assignor_address'] = ', '.join([p for p in assignor_parts if p]) if any(assignor_parts) else None
            
            # Extract assignee data (take first assignee)
            assignees = assignment_elem.find('assignees')
            if assignees is not None:
                assignee = assignees.find('assignee')
                if assignee is not None:
                    record['assignee_name'] = self._get_xml_text(assignee.find('person-or-organization-name'))
                    # Build single assignee_address as per schema
                    b_addr1 = self._get_xml_text(assignee.find('address-1'))
                    b_addr2 = self._get_xml_text(assignee.find('address-2'))
                    b_city = self._get_xml_text(assignee.find('city'))
                    b_state = self._get_xml_text(assignee.find('state'))
                    b_post = self._get_xml_text(assignee.find('postcode'))
                    assignee_parts = [b_addr1, b_addr2, b_city, b_state, b_post]
                    record['assignee_address'] = ', '.join([p for p in assignee_parts if p]) if any(assignee_parts) else None
            
            # Extract property data (take first property)
            properties = assignment_elem.find('properties')
            if properties is not None:
                property_elem = properties.find('property')
                if property_elem is not None:
                    record['serial_no'] = self._get_xml_text(property_elem.find('serial-no'))
                    record['registration_number'] = self._get_xml_text(property_elem.find('registration-no'))
                    record['intl_reg_no'] = self._get_xml_text(property_elem.find('intl-reg-no'))
                    
                    # Extract trademark law treaty property
                    tlt_property = property_elem.find('trademark-law-treaty-property')
                    if tlt_property is not None:
                        record['tlt_mark_name'] = self._get_xml_text(tlt_property.find('tlt-mark-name'))
                        record['tlt_mark_description'] = self._get_xml_text(tlt_property.find('tlt-mark-description'))
            
            # Create assignment_id from reel_no and frame_no
            if record.get('reel_no') and record.get('frame_no'):
                record['assignment_id'] = f"{record['reel_no']}-{record['frame_no']}"
            
            # Add metadata
            record['data_source'] = f"{product_id} [XML]"
            record['batch_number'] = 0  # Will be set by database controller
            
            return record
            
        except Exception as e:
            self.logger.error(f"Error extracting assignment record: {e}")
            return None

    def _extract_case_file_record(self, case_elem, product_id: str) -> Optional[Dict]:
        """Extract case-file application record (TRTDXFAP/TRTYRAP)"""
        try:
            record: Dict[str, Any] = {}
            # Basic IDs
            record['serial_no'] = self._get_xml_text(case_elem.find('serial-number'))
            reg_no = self._get_xml_text(case_elem.find('registration-number'))
            record['registration_number'] = None if (reg_no == '0000000') else reg_no
            
            header = case_elem.find('case-file-header')
            if header is not None:
                # Dates and codes
                record['filing_date'] = self._normalize_xml_date(self._get_xml_text(header.find('filing-date')))
                record['registration_date'] = self._normalize_xml_date(self._get_xml_text(header.find('registration-date')))
                record['status_code'] = self._get_xml_text(header.find('status-code'))
                record['status_date'] = self._normalize_xml_date(self._get_xml_text(header.find('status-date')))
                record['mark_identification'] = self._get_xml_text(header.find('mark-identification'))
                record['mark_drawing_code'] = self._get_xml_text(header.find('mark-drawing-code'))
                record['publication_dt'] = self._normalize_xml_date(self._get_xml_text(header.find('published-for-opposition-date')))
                record['renewal_dt'] = self._normalize_xml_date(self._get_xml_text(header.find('renewal-date')))
                record['exm_office_cd'] = self._get_xml_text(header.find('law-office-assigned-location-code'))
                # Booleans → *_in
                def flag(tag):
                    v = self._get_xml_text(header.find(tag))
                    return 'T' if (v and v.upper().startswith('T')) else 'F' if v else None
                record['trade_mark_in'] = flag('trademark-in')
                record['coll_trade_mark_in'] = flag('collective-trademark-in')
                record['serv_mark_in'] = flag('service-mark-in')
                record['coll_serv_mark_in'] = flag('collective-service-mark-in')
                record['coll_memb_mark_in'] = flag('collective-membership-mark-in')
                record['cert_mark_in'] = flag('certification-mark-in')
                record['cancel_pend_in'] = flag('cancellation-pending-in')
                record['concur_use_pub_in'] = flag('published-concurrent-in')
                record['concur_use_in'] = flag('concurrent-use-in')
                record['concur_use_pend_in'] = flag('concurrent-use-proceeding-in')
                record['interfer_pend_in'] = flag('interference-pending-in')
                record['opposit_pend_in'] = flag('opposition-pending-in')
                record['repub_12c_in'] = flag('section-12c-in')
                record['std_char_claim_in'] = flag('standard-characters-claimed-in')
                record['for_priority_in'] = flag('foreign-priority-in')
                record['lb_itu_file_in'] = flag('intent-to-use-in')
                record['lb_itu_cur_in'] = flag('intent-to-use-current-in')
                record['lb_use_file_in'] = flag('filed-as-use-application-in')
                record['lb_use_cur_in'] = flag('use-application-currently-in')
                record['amend_supp_reg_in'] = flag('supplemental-register-amended-in')
                record['supp_reg_in'] = flag('supplemental-register-in')
                record['amend_principal_in'] = flag('principal-register-amended-in')
                record['renewal_file_in'] = flag('renewal-filed-in')
                record['draw_color_file_in'] = flag('color-drawing-filed-in')
                record['draw_color_cur_in'] = flag('color-drawing-current-in')
                record['draw_3d_file_in'] = flag('drawing-3d-filed-in')
                record['draw_3d_cur_in'] = flag('drawing-3d-current-in')
                
            # International registration block → ir_*
            intl = case_elem.find('international-registration')
            if intl is not None:
                record['ir_registration_no'] = self._get_xml_text(intl.find('international-registration-number'))
                record['ir_registration_dt'] = self._normalize_xml_date(self._get_xml_text(intl.find('international-registration-date')))
                record['ir_publication_dt'] = self._normalize_xml_date(self._get_xml_text(intl.find('international-publication-date')))
                record['ir_renewal_dt'] = self._normalize_xml_date(self._get_xml_text(intl.find('international-renewal-date')))
                record['ir_auto_reg_dt'] = self._normalize_xml_date(self._get_xml_text(intl.find('auto-protection-date')))
                record['ir_status_cd'] = self._get_xml_text(intl.find('international-status-code'))
                record['ir_status_dt'] = self._normalize_xml_date(self._get_xml_text(intl.find('international-status-date')))
                # Priority
                prio_in = self._get_xml_text(intl.find('priority-claimed-in'))
                record['ir_priority_in'] = 'T' if (prio_in and prio_in.upper().startswith('T')) else ('F' if prio_in else None)
                record['ir_priority_dt'] = self._normalize_xml_date(self._get_xml_text(intl.find('priority-claimed-date')))
                # First refusal
                first_ref = self._get_xml_text(intl.find('first-refusal-in'))
                record['ir_first_refus_in'] = 'T' if (first_ref and first_ref.upper().startswith('T')) else ('F' if first_ref else None)
            
            # Add metadata
            record['data_source'] = f"{product_id} [XML]"
            record['batch_number'] = 0
            return record
        except Exception as e:
            self.logger.error(f"Error extracting case-file record: {e}")
            return None
    
    def _get_xml_text(self, element) -> Optional[str]:
        """Get text content from XML element, return None if empty or missing"""
        if element is not None and element.text and element.text.strip():
            return element.text.strip()
        return None  # Return None instead of empty string to better indicate missing data

    def _normalize_xml_date(self, yyyymmdd: Optional[str]) -> Optional[str]:
        """Convert dates like 19550104 to 1955-01-04; return None if invalid"""
        if not yyyymmdd:
            return None
        s = yyyymmdd.strip()
        if len(s) != 8 or not s.isdigit():
            return None
        year, month, day = s[:4], s[4:6], s[6:8]
        # Basic sanity checks
        if month == '00' or day == '00':
            return None
        return f"{year}-{month}-{day}"

class DatabaseController(BaseController):
    """Controller for database operations and optimization"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        # Handle both flat and nested config formats
        if 'database' in config:
            self.db_config = config.get('database', {})
            # Remove schema from connection config if present
            if 'schema' in self.db_config:
                self.schema = self.db_config.pop('schema')
            else:
                self.schema = 'public'
        else:
            # Flat config - extract database keys
            self.db_config = {
                'host': config.get('host', 'localhost'),
                'port': config.get('port', '5432'),
                'dbname': config.get('dbname', 'trademarks'),
                'user': config.get('user', 'postgres'),
                'password': config.get('password', '')
            }
            # Store schema separately as it's not a connection parameter
            self.schema = config.get('schema', 'public')
        self.batch_size = config.get('batch_size', 10000)
        self.use_copy = config.get('use_copy', True)
    
    def initialize(self) -> bool:
        """Initialize database controller"""
        try:
            # Test database connection
            conn = psycopg2.connect(**self.db_config)
            conn.close()
            
            # Setup control tables
            self._setup_control_tables()
            
            self.logger.info("Database Controller initialized successfully")
            return True
        except Exception as e:
            self.logger.error(f"Failed to initialize database controller: {e}")
            return False

    def has_existing_rows(self, product_id: str) -> bool:
        """Return True if the product's table already contains data."""
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()
            # Get table name for product
            cur.execute('SELECT table_name FROM uspto_products WHERE product_id = %s', (product_id,))
            row = cur.fetchone()
            if not row:
                conn.close()
                return False
            table_name = row[0]
            # Count rows
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cur.fetchone()[0]
            conn.close()
            return count > 0
        except Exception as e:
            self.logger.error(f"Error checking existing rows for {product_id}: {e}")
            return False

    def is_file_completed(self, product_id: str, file_name: str) -> bool:
        """Return True if the given product file has status 'completed'."""
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()
            cur.execute(
                """
                SELECT status FROM file_processing_history
                WHERE product_id = %s AND file_name = %s
                """,
                (product_id, file_name),
            )
            row = cur.fetchone()
            conn.close()
            return bool(row and row[0] == 'completed')
        except Exception as e:
            self.logger.error(f"Error checking file status for {product_id}/{file_name}: {e}")
            return False

    def is_product_completed_today(self, product_id: str) -> bool:
        """Return True if any file for this product was marked completed (ignore date)."""
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()
            cur.execute(
                """
                SELECT 1
                FROM file_processing_history
                WHERE product_id = %s AND status = 'completed'
                LIMIT 1
                """,
                (product_id,),
            )
            row = cur.fetchone()
            conn.close()
            return bool(row)
        except Exception as e:
            self.logger.error(f"Error checking product completed today {product_id}: {e}")
            return False

    def mark_file_processing(self, product_id: str, file_name: str, file_url: str, file_size: int):
        """Upsert a history row with status 'processing'."""
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()
            # Do not downgrade a completed file to processing
            cur.execute(
                """
                SELECT status FROM file_processing_history
                WHERE product_id = %s AND file_name = %s
                """,
                (product_id, file_name),
            )
            row = cur.fetchone()
            if row and row[0] == 'completed':
                conn.close()
                return
            cur.execute(
                """
                INSERT INTO file_processing_history
                    (product_id, file_name, file_url, file_size, processing_started, status, processing_attempts)
                VALUES (%s, %s, %s, %s, NOW(), 'processing', 1)
                ON CONFLICT (product_id, file_name) DO UPDATE SET
                    file_url = EXCLUDED.file_url,
                    file_size = EXCLUDED.file_size,
                    processing_started = CASE WHEN file_processing_history.status = 'completed' THEN file_processing_history.processing_started ELSE NOW() END,
                    status = CASE WHEN file_processing_history.status = 'completed' THEN 'completed' ELSE 'processing' END,
                    processing_attempts = CASE WHEN file_processing_history.status = 'completed' THEN file_processing_history.processing_attempts ELSE file_processing_history.processing_attempts + 1 END,
                    error_message = CASE WHEN file_processing_history.status = 'completed' THEN file_processing_history.error_message ELSE NULL END
                """,
                (product_id, file_name, file_url, file_size),
            )
            conn.commit()
            conn.close()
        except Exception as e:
            self.logger.error(f"Error marking file processing {product_id}/{file_name}: {e}")

    def mark_file_completed(self, product_id: str, file_name: str, rows_processed: int, rows_saved: int, batch_count: int):
        """Update history row to completed with counts."""
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE file_processing_history
                SET processing_completed = NOW(),
                    rows_processed = %s,
                    rows_saved = %s,
                    batch_count = %s,
                    status = 'completed',
                    error_message = NULL
                WHERE product_id = %s AND file_name = %s
                """,
                (rows_processed, rows_saved, batch_count, product_id, file_name),
            )
            conn.commit()
            conn.close()
        except Exception as e:
            self.logger.error(f"Error marking file completed {product_id}/{file_name}: {e}")

    def mark_file_error(self, product_id: str, file_name: str, error_message: str):
        """Update history row to error with message."""
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE file_processing_history
                SET status = 'error', error_message = %s
                WHERE product_id = %s AND file_name = %s
                """,
                (error_message[:1000] if error_message else None, product_id, file_name),
            )
            conn.commit()
            conn.close()
        except Exception as e:
            self.logger.error(f"Error marking file error {product_id}/{file_name}: {e}")

    def upsert_file_completed(self, product_id: str, file_name: str):
        """Insert or update a file as completed with today's timestamp."""
        try:
            conn = psycopg2.connect(**self.db_config)
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO file_processing_history
                    (product_id, file_name, status, processing_started, processing_completed, rows_processed, rows_saved, batch_count)
                VALUES (%s, %s, 'completed', NOW(), NOW(), 0, 0, 0)
                ON CONFLICT (product_id, file_name) DO UPDATE SET
                    status = 'completed',
                    processing_completed = NOW()
                """,
                (product_id, file_name),
            )
            conn.commit()
            conn.close()
        except Exception as e:
            self.logger.error(f"Error upserting file completed {product_id}/{file_name}: {e}")
    
    def cleanup(self):
        """Cleanup database resources"""
        pass
    
    def _setup_control_tables(self):
        """Setup control tables if they don't exist"""
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            # Product registry table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS uspto_products (
                    id SERIAL PRIMARY KEY,
                    product_id VARCHAR(50) UNIQUE,
                    title TEXT,
                    description TEXT,
                    frequency VARCHAR(20),
                    from_date DATE,
                    to_date DATE,
                    total_size BIGINT,
                    file_count INTEGER,
                    last_modified TIMESTAMP,
                    formats TEXT[],
                    table_name VARCHAR(50),
                    schema_created BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # File processing history
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS file_processing_history (
                    id SERIAL PRIMARY KEY,
                    product_id VARCHAR(50),
                    file_name VARCHAR(255),
                    file_url TEXT,
                    file_size BIGINT,
                    file_hash VARCHAR(64),
                    download_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    processing_started TIMESTAMP,
                    processing_completed TIMESTAMP,
                    rows_processed INTEGER DEFAULT 0,
                    rows_saved INTEGER DEFAULT 0,
                    status VARCHAR(20) DEFAULT 'pending',
                    error_message TEXT,
                    processing_attempts INTEGER DEFAULT 0,
                    batch_count INTEGER DEFAULT 0,
                    last_batch_processed INTEGER DEFAULT 0,
                    UNIQUE(product_id, file_name)
                )
            ''')
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            self.logger.error(f"Error setting up control tables: {e}")
    
    def register_product(self, product_info: ProductInfo) -> bool:
        """Register a product and create its table"""
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            table_name = f"product_{product_info.product_id.lower()}"
            
            # Register product
            cursor.execute('''
                INSERT INTO uspto_products 
                (product_id, title, description, frequency, from_date, to_date, 
                 total_size, file_count, last_modified, formats, table_name)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (product_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    description = EXCLUDED.description,
                    frequency = EXCLUDED.frequency,
                    from_date = EXCLUDED.from_date,
                    to_date = EXCLUDED.to_date,
                    total_size = EXCLUDED.total_size,
                    file_count = EXCLUDED.file_count,
                    last_modified = EXCLUDED.last_modified,
                    formats = EXCLUDED.formats,
                    updated_at = CURRENT_TIMESTAMP
            ''', (
                product_info.product_id, product_info.title, product_info.description,
                product_info.frequency, product_info.from_date, product_info.to_date,
                product_info.total_size, product_info.file_count, product_info.last_modified,
                product_info.formats, table_name
            ))
            
            conn.commit()
            
            # Create product table
            self._create_product_table(product_info.product_id, table_name)
            
            conn.close()
            return True
            
        except Exception as e:
            self.logger.error(f"Error registering product {product_info.product_id}: {e}")
            return False
    
    def _create_product_table(self, product_id: str, table_name: str):
        """Create a product-specific table"""
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            # Check if table already exists
            cursor.execute('''
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = %s
                )
            ''', (table_name,))
            
            if cursor.fetchone()[0]:
                self.logger.info(f"Table {table_name} already exists")
                conn.close()
                return
            
            # Create table based on product type
            if 'TRCFECO' in product_id or 'TRTDXFAP' in product_id or 'TRTYRAP' in product_id:  # Case file data
                self._create_case_file_table(cursor, table_name)
            elif 'TRASECO' in product_id or 'TRTDXFAG' in product_id or 'TRTYRAG' in product_id:  # Assignment data
                self._create_assignment_table(cursor, table_name)
            elif 'TTAB' in product_id:  # TTAB proceedings
                self._create_ttab_table(cursor, table_name)
            else:
                # Generic table for unknown products
                self._create_generic_table(cursor, table_name)
            
            conn.commit()
            
            # Update product registry
            cursor.execute('''
                UPDATE uspto_products 
                SET schema_created = TRUE 
                WHERE product_id = %s
            ''', (product_id,))
            conn.commit()
            
            conn.close()
            self.logger.info(f"Created table {table_name} for product {product_id}")
            
        except Exception as e:
            self.logger.error(f"Error creating table for product {product_id}: {e}")
    
    def _create_case_file_table(self, cursor, table_name: str):
        """Create table for case file data"""
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
    
    def _create_assignment_table(self, cursor, table_name: str):
        """Create table for assignment data"""
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
                page_count INTEGER,
                last_update_date DATE,
                purge_indicator CHAR(1),
                correspondent_name TEXT,
                correspondent_address_1 TEXT,
                correspondent_address_2 TEXT,
                correspondent_address_3 TEXT,
                assignor_name TEXT,
                assignee_name TEXT,
                assignor_address TEXT,
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
    
    def _create_ttab_table(self, cursor, table_name: str):
        """Create table for TTAB proceedings"""
        cursor.execute(f'''
            CREATE TABLE {table_name} (
                id SERIAL PRIMARY KEY,
                proceeding_number VARCHAR(20) UNIQUE,
                proceeding_type VARCHAR(50),
                status VARCHAR(50),
                filing_date DATE,
                applicant_name TEXT,
                opposer_name TEXT,
                mark_description TEXT,
                goods_services TEXT,
                data_source VARCHAR(100),
                file_hash VARCHAR(64),
                batch_number INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create indexes
        cursor.execute(f'CREATE INDEX idx_{table_name}_proceeding ON {table_name}(proceeding_number)')
        cursor.execute(f'CREATE INDEX idx_{table_name}_filing_date ON {table_name}(filing_date)')
        cursor.execute(f'CREATE INDEX idx_{table_name}_batch ON {table_name}(batch_number)')
    
    def _create_generic_table(self, cursor, table_name: str):
        """Create generic table for unknown product types"""
        cursor.execute(f'''
            CREATE TABLE {table_name} (
                id SERIAL PRIMARY KEY,
                raw_data JSONB,
                data_source VARCHAR(100),
                file_hash VARCHAR(64),
                batch_number INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create indexes
        cursor.execute(f'CREATE INDEX idx_{table_name}_batch ON {table_name}(batch_number)')
        cursor.execute(f'CREATE INDEX idx_{table_name}_data_source ON {table_name}(data_source)')
        cursor.execute(f'CREATE INDEX idx_{table_name}_raw_data ON {table_name} USING GIN(raw_data)')
    
    def _map_insert_columns(self, batch_data: List[Dict]) -> List[Dict]:
        """Map column names in batch data to match database schema"""
        
        if not batch_data:
            return batch_data
        
        # Column name mappings
        column_mappings = {
            'serial_number': 'serial_no',
            # Note: registration_number is already correct in database, don't map it
            'filing_date': 'filing_dt',
            'registration_date': 'registration_dt',
            'mark_identification': 'mark_id_char',
            'mark_drawing_code': 'mark_draw_cd',
            'abandon_date': 'abandon_dt',
            'amend_registration_date': 'amend_reg_dt',
            'reg_cancel_code': 'reg_cancel_cd',
            'reg_cancel_date': 'reg_cancel_dt',
            'examiner_attorney_name': 'exm_attorney_name',
            'file_location_date': 'file_location_dt',
            'publication_date': 'publication_dt',
            'renewal_date': 'renewal_dt',
            'repub_12c_date': 'repub_12c_dt',
            'cfh_status_code': 'cfh_status_cd',
            'cfh_status_date': 'cfh_status_dt',
            'ir_auto_registration_date': 'ir_auto_reg_dt',
            'ir_first_refusal_in': 'ir_first_refus_in',
            'ir_death_date': 'ir_death_dt',
            'ir_publication_date': 'ir_publication_dt',
            'ir_registration_date': 'ir_registration_dt',
            'ir_registration_number': 'ir_registration_no',
            'ir_renewal_date': 'ir_renewal_dt',
            'ir_status_code': 'ir_status_cd',
            'ir_status_date': 'ir_status_dt',
            'ir_priority_date': 'ir_priority_dt',
        }
        
        mapped_batch = []
        for record in batch_data:
            mapped_record = {}
            for key, value in record.items():
                # Clean the key first
                clean_key = key.strip().lower().replace(' ', '_').replace('-', '_')
                
                # Apply mapping if exists
                if clean_key in column_mappings:
                    mapped_record[column_mappings[clean_key]] = value
                else:
                    mapped_record[clean_key] = value
            
            mapped_batch.append(mapped_record)
        
        return mapped_batch
    
    def save_batch(self, batch_data: List[Dict], product_id: str, batch_number: int) -> ProcessingResult:
        """Save a batch of data to the database with column mapping"""
        start_time = time.time()
        
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            # Get table name
            cursor.execute('SELECT table_name FROM uspto_products WHERE product_id = %s', (product_id,))
            result = cursor.fetchone()
            if not result:
                raise Exception(f"Product {product_id} not found in registry")
            
            table_name = result[0]
            
            # Get actual columns in the database table
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = %s 
                ORDER BY ordinal_position
            """, (table_name,))
            
            db_columns = set(row[0] for row in cursor.fetchall())
            
            # For TRCFECO2, use data as-is (columns already match database)
            if product_id == 'TRCFECO2':
                mapped_batch_data = batch_data  # No mapping needed
            else:
                # Apply column mapping for other products
                mapped_batch_data = self._map_insert_columns(batch_data)
            
            # Filter to only include columns that exist in the database
            # and ensure no empty strings are passed to date columns
            filtered_batch = []
            for record in mapped_batch_data:
                filtered_record = {}
                for k, v in record.items():
                    if k in db_columns:
                        # Final check for empty strings (especially date columns)
                        if isinstance(v, str) and v.strip() == '':
                            filtered_record[k] = None
                        elif (k.endswith('_dt') or k.endswith('_date')) and (v is None or v == ''):
                            filtered_record[k] = None
                        else:
                            filtered_record[k] = v
                
                filtered_record['batch_number'] = batch_number
                filtered_batch.append(filtered_record)
            
            # Prepare insert query
            if filtered_batch:
                columns = list(filtered_batch[0].keys())
                placeholders = ', '.join(['%s'] * len(columns))
                column_list = ', '.join(columns)
                
                # Convert to tuples
                batch_tuples = [tuple(record.get(col) for col in columns) for record in filtered_batch]
                
                # Execute batch insert using execute_values
                execute_values(
                    cursor, 
                    f"INSERT INTO {table_name} ({column_list}) VALUES %s ON CONFLICT DO NOTHING",
                    batch_tuples,
                    page_size=1000
                )
                conn.commit()
            
            saved_count = len(filtered_batch)
            conn.close()
            
            processing_time = time.time() - start_time
            
            return ProcessingResult(
                success=True,
                rows_processed=len(batch_data),
                rows_saved=saved_count,
                processing_time=processing_time
            )
            
        except Exception as e:
            self.logger.error(f"Error saving batch: {e}")
            return ProcessingResult(
                success=False,
                rows_processed=len(batch_data),
                rows_saved=0,
                error_message=str(e),
                processing_time=time.time() - start_time
            )

    def get_processing_stats(self) -> Dict[str, Any]:
        """Get processing statistics"""
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT 
                    COUNT(*) as total_products,
                    COUNT(CASE WHEN schema_created = TRUE THEN 1 END) as tables_created,
                    COUNT(CASE WHEN schema_created = FALSE THEN 1 END) as pending_tables
                FROM uspto_products
            ''')
            
            stats = cursor.fetchone()
            conn.close()
            
            return {
                'total_products': stats[0],
                'tables_created': stats[1],
                'pending_tables': stats[2]
            }
            
        except Exception as e:
            self.logger.error(f"Error getting processing stats: {e}")
            return {}

class USPTOOrchestrator:
    """Main orchestrator that merges all controllers into one optimized process"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = self._setup_logging()
        
        # Initialize controllers
        self.api_controller = USPTOAPIController(config)
        self.download_controller = DownloadController(config)
        self.processing_controller = ProcessingController(config)
        self.database_controller = DatabaseController(config)
        # Optional product filters
        self.skip_products: List[str] = [p.strip().upper() for p in config.get('skip_products', [])]
        self.only_products: List[str] = [p.strip().upper() for p in config.get('only_products', [])]
        
        self.controllers = [
            self.api_controller,
            self.download_controller,
            self.processing_controller,
            self.database_controller
        ]
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging for the orchestrator"""
        logger = logging.getLogger('USPTOOrchestrator')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def initialize(self) -> bool:
        """Initialize all controllers"""
        self.logger.info("Initializing USPTO Orchestrator...")
        
        for controller in self.controllers:
            if not controller.initialize():
                self.logger.error(f"Failed to initialize {controller.__class__.__name__}")
                return False
        
        self.logger.info("All controllers initialized successfully")
        return True
    
    def cleanup(self):
        """Cleanup all controllers"""
        self.logger.info("Cleaning up USPTO Orchestrator...")
        
        for controller in self.controllers:
            try:
                controller.cleanup()
            except Exception as e:
                self.logger.error(f"Error cleaning up {controller.__class__.__name__}: {e}")
        
        self.logger.info("Cleanup completed")
    
    def run_full_process(self, max_files_per_product: int = 2, force_redownload: bool = False) -> Dict[str, Any]:
        """Run the complete USPTO data processing pipeline"""
        start_time = time.time()
        results = {
            'success': False,
            'products_processed': 0,
            'files_processed': 0,
            'total_rows_processed': 0,
            'total_rows_saved': 0,
            'errors': []
        }
        
        try:
            self.logger.info("Starting full USPTO data processing pipeline...")
            
            # Step 1: Get available datasets
            self.logger.info("Step 1: Fetching available datasets...")
            products = self.api_controller.get_trademark_datasets()
            if not products:
                raise Exception("No datasets found")
            
            self.logger.info(f"Found {len(products)} products")
            
            # Step 2: Register products and create tables
            self.logger.info("Step 2: Registering products and creating tables...")
            for product in products:
                if self.database_controller.register_product(product):
                    results['products_processed'] += 1
                    self.logger.info(f"Registered product: {product.product_id}")
                else:
                    results['errors'].append(f"Failed to register product: {product.product_id}")
            
            # Step 3: Process each product
            self.logger.info("Step 3: Processing products...")
            for product in products:
                # Respect only/skip product filters
                self.logger.info(
                    f"Filter state → only={self.only_products or []}, skip={self.skip_products or []}, force={force_redownload}"
                )
                if self.only_products and product.product_id.upper() not in self.only_products:
                    self.logger.info(f"Skipping {product.product_id} - not in only_products filter")
                    continue
                if self.skip_products and product.product_id.upper() in self.skip_products:
                    self.logger.info(f"Skipping {product.product_id} - in skip_products filter")
                    continue
                self.logger.info(f"Processing product: {product.title}")
                # Skip product if it was completed before (any date) OR its table already has rows
                if not force_redownload:
                    completed = self.database_controller.is_product_completed_today(product.product_id)
                    has_rows = self.database_controller.has_existing_rows(product.product_id)
                    self.logger.info(
                        f"Skip checks for {product.product_id} → completed={completed}, has_rows={has_rows}, force={force_redownload}"
                    )
                    if completed:
                        self.logger.info(f"Skipping {product.product_id} - product already marked completed (history)")
                        continue
                    if has_rows:
                        self.logger.info(f"Skipping {product.product_id} - table already has data (rows exist)")
                        continue
                else:
                    self.logger.info(f"Force mode: bypassing skip checks for {product.product_id}")
                
                # Limit files per product
                files_to_process = product.files[:max_files_per_product]
                
                for file_info in files_to_process:
                    try:
                        # Skip file if already completed previously
                        file_completed = False
                        if not force_redownload:
                            file_completed = self.database_controller.is_file_completed(file_info.product_id, file_info.filename)
                        self.logger.info(
                            f"File skip check for {file_info.product_id}/{file_info.filename} → completed={file_completed}, force={force_redownload}"
                        )
                        if not force_redownload and file_completed:
                            self.logger.info(f"Skipping {file_info.product_id}/{file_info.filename} - already completed")
                            results['files_processed'] += 1
                            continue
                        
                        # Check if files are already extracted
                        extracted_path = self.download_controller.check_extracted_files_exist(file_info)
                        file_path = None
                        file_path_to_process = None
                        
                        if not extracted_path:
                            # Download file if not already extracted
                            file_path = self.download_controller.download_file(file_info, force_redownload)
                            if not file_path:
                                results['errors'].append(f"Failed to download: {file_info.filename}")
                                continue
                            
                            # Extract if ZIP
                            if file_info.filename.endswith('.zip'):
                                extracted_path = self.download_controller.extract_zip_file(file_path, file_info.product_id)
                            else:
                                # Process single file
                                file_path_to_process = file_path
                        
                        if extracted_path:
                            # Process extracted files
                            # Mark file as processing once we begin
                            self.logger.info(
                                f"Begin processing extracted files for {file_info.product_id}/{file_info.filename} at {extracted_path}"
                            )
                            self.database_controller.mark_file_processing(file_info.product_id, file_info.filename, file_info.download_url, file_info.size or 0)
                            for extracted_file in self.download_controller.find_data_files(extracted_path):
                                batch_count = 0
                                for batch in self._process_file(extracted_file, file_info.product_id):
                                    batch_count += 1
                                    result = self.database_controller.save_batch(batch, file_info.product_id, batch_count)
                                    
                                    results['total_rows_processed'] += result.rows_processed
                                    results['total_rows_saved'] += result.rows_saved
                                    
                                    if not result.success:
                                        results['errors'].append(f"Batch save error: {result.error_message}")
                        elif file_path_to_process:
                            # Process single file
                            self.logger.info(
                                f"Begin processing single file for {file_info.product_id}/{file_info.filename} at {file_path_to_process}"
                            )
                            self.database_controller.mark_file_processing(file_info.product_id, file_info.filename, file_info.download_url, file_info.size or 0)
                            batch_count = 0
                            for batch in self._process_file(file_path_to_process, file_info.product_id):
                                batch_count += 1
                                result = self.database_controller.save_batch(batch, file_info.product_id, batch_count)
                                
                                results['total_rows_processed'] += result.rows_processed
                                results['total_rows_saved'] += result.rows_saved
                                
                                if not result.success:
                                    results['errors'].append(f"Batch save error: {result.error_message}")
                        
                        # Mark file completed with today's date
                        self.logger.info(
                            f"Marking completed in history for {file_info.product_id}/{file_info.filename}"
                        )
                        self.database_controller.upsert_file_completed(file_info.product_id, file_info.filename)
                        results['files_processed'] += 1
                        
                    except Exception as e:
                        error_msg = f"Error processing file {file_info.filename}: {e}"
                        results['errors'].append(error_msg)
                        self.logger.error(error_msg)
                        # Mark file error
                        self.database_controller.mark_file_error(file_info.product_id, file_info.filename, error_msg)
            
            results['success'] = True
            processing_time = time.time() - start_time
            
            self.logger.info(f"Pipeline completed successfully in {processing_time:.2f} seconds")
            self.logger.info(f"Results: {results}")
            
        except Exception as e:
            error_msg = f"Pipeline failed: {e}"
            results['errors'].append(error_msg)
            self.logger.error(error_msg)
        
        return results
    
    def _process_file(self, file_path: Path, product_id: str) -> Generator[List[Dict], None, None]:
        """Process a file using the appropriate processor"""
        if file_path.suffix.lower() == '.csv':
            yield from self.processing_controller.process_csv_file(file_path, product_id)
        elif file_path.suffix.lower() == '.xml':
            yield from self.processing_controller.process_xml_file(file_path, product_id)
        else:
            self.logger.warning(f"Unsupported file type: {file_path.suffix}")
            yield []

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='USPTO Controller-Based Processor')
    parser.add_argument('--max-files', type=int, default=2, help='Max files per product')
    parser.add_argument('--force-redownload', action='store_true', help='Force redownload of files')
    parser.add_argument('--batch-size', type=int, default=10000, help='Batch size for processing')
    parser.add_argument('--memory-limit', type=int, default=512, help='Memory limit in MB')
    
    args = parser.parse_args()
    
    # Configuration
    config = {
        'api_url': 'https://data.uspto.gov/ui/datasets/products/search',
        'download_dir': './uspto_data',
        'database': {
            'dbname': 'trademarks',
            'user': 'postgres',
            'password': '1234',
            'host': 'localhost',
            'port': '5432'
        },
        'batch_size': args.batch_size,
        'chunk_size': 50000,
        'memory_limit_mb': args.memory_limit,
        'use_copy': True
    }
    
    # Create orchestrator
    orchestrator = USPTOOrchestrator(config)
    
    try:
        # Initialize
        if not orchestrator.initialize():
            print("Failed to initialize orchestrator")
            return
        
        # Run full process
        results = orchestrator.run_full_process(
            max_files_per_product=args.max_files,
            force_redownload=args.force_redownload
        )
        
        # Print results
        print("\n" + "="*50)
        print("PROCESSING RESULTS")
        print("="*50)
        print(f"Success: {results['success']}")
        print(f"Products Processed: {results['products_processed']}")
        print(f"Files Processed: {results['files_processed']}")
        print(f"Total Rows Processed: {results['total_rows_processed']}")
        print(f"Total Rows Saved: {results['total_rows_saved']}")
        
        if results['errors']:
            print(f"\nErrors ({len(results['errors'])}):")
            for error in results['errors']:
                print(f"  - {error}")
        
    finally:
        # Cleanup
        orchestrator.cleanup()

if __name__ == "__main__":
    main()
