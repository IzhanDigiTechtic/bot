#!/usr/bin/env python3
"""
USPTO Specialized File Processors
Handles different file formats (CSV, XML, DTA) with specialized processing logic.
"""

import pandas as pd
import xml.etree.ElementTree as ET
import zipfile
import json
import os
import hashlib
from pathlib import Path
from typing import Dict, List, Any, Optional, Generator, Tuple
import logging
from datetime import datetime

class USPTOFileProcessor:
    """Base class for USPTO file processors"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.batch_size = config.get('processing', {}).get('batch_size', 10000)
        self.chunk_size = config.get('processing', {}).get('chunk_size', 50000)
    
    def process_file(self, file_path: str, product_id: str) -> Generator[List[Dict[str, Any]], None, None]:
        """Process file and yield batches of records"""
        raise NotImplementedError("Subclasses must implement process_file method")
    
    def _clean_record(self, record: Dict[str, Any], product_id: str) -> Dict[str, Any]:
        """Clean and normalize record data with proper column mapping"""
        # First map column names to match database schema
        mapped_record = self._map_column_names(record)
        
        cleaned = {}
        
        for key, value in mapped_record.items():
            # Clean values
            if value is None or value == '':
                cleaned[key] = None
            elif isinstance(value, str):
                cleaned[key] = value.strip()
            else:
                cleaned[key] = value
        
        # Add metadata
        cleaned['data_source'] = f"{product_id}_file"
        cleaned['file_hash'] = None  # Will be set by caller if needed
        cleaned['processing_timestamp'] = datetime.now().isoformat()
        
        return cleaned
    
    def _map_column_names(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Map column names to match database schema"""
        
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
        
        mapped_record = {}
        for key, value in record.items():
            # Clean the key first
            clean_key = self._clean_column_name(key)
            
            # Apply mapping if exists
            if clean_key in column_mappings:
                mapped_record[column_mappings[clean_key]] = value
            else:
                mapped_record[clean_key] = value
        
        return mapped_record
    
    def _clean_column_name(self, column_name: str) -> str:
        """Clean column names for database compatibility"""
        # Remove special characters and replace with underscores
        import re
        clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', column_name)
        clean_name = re.sub(r'_+', '_', clean_name)  # Replace multiple underscores with single
        clean_name = clean_name.strip('_').lower()  # Remove leading/trailing underscores and lowercase
        
        return clean_name
    
    def _calculate_file_hash(self, file_path: str) -> str:
        """Calculate MD5 hash of file"""
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def _get_xml_text(self, element: Optional[ET.Element]) -> Optional[str]:
        """Safely extract text from XML element, handling None or empty cases."""
        if element is not None and element.text and element.text.strip():
            return element.text.strip()
        return None

class CSVProcessor(USPTOFileProcessor):
    """Processor for CSV files"""
    
    def process_file(self, file_path: str, product_id: str) -> Generator[List[Dict[str, Any]], None, None]:
        """Process CSV file in batches using chunked reading for large files"""
        try:
            # Check file size to determine processing method
            file_size = os.path.getsize(file_path)
            large_file_threshold = 100 * 1024 * 1024  # 100MB
            
            if file_size > large_file_threshold:
                # Use chunked reading for large files
                yield from self._process_large_csv_file(file_path, product_id)
            else:
                # Use regular reading for smaller files
                yield from self._process_small_csv_file(file_path, product_id)
                
        except Exception as e:
            self.logger.error(f"Error processing CSV file {file_path}: {e}")
            raise
    
    def _process_large_csv_file(self, file_path: str, product_id: str) -> Generator[List[Dict[str, Any]], None, None]:
        """Process large CSV files using chunked reading"""
        try:
            batch_number = 0
            total_records = 0
            
            # Use pandas chunked reading for large files
            chunk_size = min(self.chunk_size, 50000)  # Limit chunk size for memory
            
            for chunk_df in pd.read_csv(file_path, chunksize=chunk_size, low_memory=False):
                batch_records = []
                
                # Process chunk
                for _, row in chunk_df.iterrows():
                    record = row.to_dict()
                    cleaned_record = self._clean_record(record, product_id)
                    cleaned_record['batch_number'] = batch_number
                    
                    batch_records.append(cleaned_record)
                    
                    # Yield batch when full
                    if len(batch_records) >= self.batch_size:
                        total_records += len(batch_records)
                        yield batch_records
                        batch_records = []
                        batch_number += 1
                
                # Yield remaining records from this chunk
                if batch_records:
                    total_records += len(batch_records)
                    yield batch_records
                    batch_number += 1
                    
        except Exception as e:
            self.logger.error(f"Error processing large CSV file {file_path}: {e}")
            raise
    
    def _process_small_csv_file(self, file_path: str, product_id: str) -> Generator[List[Dict[str, Any]], None, None]:
        """Process small CSV files using regular reading"""
        try:
            batch_number = 0
            batch_records = []
            
            # Read CSV normally for small files
            df = pd.read_csv(file_path, low_memory=False)
            
            for _, row in df.iterrows():
                record = row.to_dict()
                cleaned_record = self._clean_record(record, product_id)
                cleaned_record['batch_number'] = batch_number
                
                batch_records.append(cleaned_record)
                
                # Yield batch when full
                if len(batch_records) >= self.batch_size:
                    yield batch_records
                    batch_records = []
                    batch_number += 1
            
            # Yield remaining records
            if batch_records:
                yield batch_records
                    
        except Exception as e:
            self.logger.error(f"Error processing small CSV file {file_path}: {e}")
            raise

class XMLProcessor(USPTOFileProcessor):
    """Processor for XML files"""
    
    def process_file(self, file_path: str, product_id: str) -> Generator[List[Dict[str, Any]], None, None]:
        """Process XML file in batches using iterative parsing for large files"""
        try:
            # Check file size to determine processing method
            file_size = os.path.getsize(file_path)
            large_file_threshold = 100 * 1024 * 1024  # 100MB
            
            if file_size > large_file_threshold:
                # Use iterative parsing for large files
                yield from self._process_large_xml_file(file_path, product_id)
            else:
                # Use regular parsing for smaller files
                yield from self._process_small_xml_file(file_path, product_id)
                
        except Exception as e:
            self.logger.error(f"Error processing XML file {file_path}: {e}")
            raise
    
    def _process_large_xml_file(self, file_path: str, product_id: str) -> Generator[List[Dict[str, Any]], None, None]:
        """Process large XML files using iterative parsing"""
        try:
            batch_records = []
            batch_number = 0
            
            # Use iterative parsing for large files
            for event, elem in ET.iterparse(file_path, events=('end',)):
                if elem.tag == 'assignment-entry' and product_id in ['TRTYRAG', 'TRTDXFAG']:
                    # Use product-specific processor
                    record = TRTYRAGProcessor(self.config)._extract_single_assignment(elem)
                    if record:
                        cleaned_record = self._clean_record(record, product_id)
                        cleaned_record['batch_number'] = batch_number
                        batch_records.append(cleaned_record)
                        
                        # Yield batch when full
                        if len(batch_records) >= self.batch_size:
                            yield batch_records
                            batch_records = []
                            batch_number += 1
                
                elif elem.tag in ['proceeding', 'ttab-proceeding'] and product_id in ['TTABTDXF', 'TTABYR']:
                    record = TTABProcessor(self.config)._element_to_dict(elem)
                    if record:
                        cleaned_record = self._clean_record(record, product_id)
                        cleaned_record['batch_number'] = batch_number
                        batch_records.append(cleaned_record)
                        
                        # Yield batch when full
                        if len(batch_records) >= self.batch_size:
                            yield batch_records
                            batch_records = []
                            batch_number += 1
                
                # Clear element to save memory
                elem.clear()
            
            # Yield remaining records
            if batch_records:
                yield batch_records
                
        except Exception as e:
            self.logger.error(f"Error processing large XML file {file_path}: {e}")
            raise
    
    def _process_small_xml_file(self, file_path: str, product_id: str) -> Generator[List[Dict[str, Any]], None, None]:
        """Process small XML files using regular parsing"""
        try:
            # Parse XML file
            tree = ET.parse(file_path)
            root = tree.getroot()
            
            # Dispatch to product-specific processors
            if product_id in ['TRTDXFAP', 'TRTYRAP']:
                records = TRTDXFAPProcessor(self.config).extract_case_file_records(root)
            elif product_id in ['TRTYRAG', 'TRTDXFAG']:
                records = TRTYRAGProcessor(self.config).extract_assignment_records(root)
            elif product_id in ['TTABTDXF', 'TTABYR']:
                records = TTABProcessor(self.config).extract_ttab_records(root)
            else:
                records = self._extract_xml_records(root, product_id)
            
            batch_number = 0
            batch_records = []
            
            for record in records:
                cleaned_record = self._clean_record(record, product_id)
                cleaned_record['batch_number'] = batch_number
                
                batch_records.append(cleaned_record)
                
                # Yield batch when full
                if len(batch_records) >= self.batch_size:
                    yield batch_records
                    batch_records = []
                    batch_number += 1
            
            # Yield remaining records
            if batch_records:
                yield batch_records
                
        except Exception as e:
            self.logger.error(f"Error processing small XML file {file_path}: {e}")
            raise
    
    def _extract_xml_records(self, root: ET.Element, product_id: str) -> List[Dict[str, Any]]:
        """Extract records from XML based on product type"""
        records = []
        
        # Special handling for assignment XML files (TRTYRAG, TRTDXFAG)
        if product_id in ['TRTYRAG', 'TRTDXFAG']:
            return self._extract_assignment_records(root)
        
        # Define record patterns for other product types
        record_patterns = {
            'TRTDXFAP': ['application', 'trademark-application'],
            'TRTYRAP': ['application', 'trademark-application'],
            'TTABTDXF': ['proceeding', 'ttab-proceeding'],
            'TTABYR': ['proceeding', 'ttab-proceeding']
        }
        
        patterns = record_patterns.get(product_id, ['record', 'item'])
        
        for pattern in patterns:
            elements = root.findall(f".//{pattern}")
            if elements:
                for element in elements:
                    record = self._xml_element_to_dict(element)
                    records.append(record)
                break
        
        return records
    
class TRTDXFAPProcessor(USPTOFileProcessor):
    """Product-specific XML processor for TRTDXFAP/TRTYRAP case-file data"""
    
    def extract_case_file_records(self, root: ET.Element) -> List[Dict[str, Any]]:
        records: List[Dict[str, Any]] = []
        for case_elem in root.findall('.//case-file'):
            rec = self._extract_case_file_record(case_elem)
            if rec:
                records.append(rec)
        return records
    
    def _extract_case_file_record(self, case_elem: ET.Element) -> Optional[Dict[str, Any]]:
        try:
            record: Dict[str, Any] = {}
            # Basic IDs
            record['serial_no'] = self._get_text(case_elem.find('serial-number'))
            reg_no = self._get_text(case_elem.find('registration-number'))
            record['registration_number'] = None if (reg_no == '0000000') else reg_no
            header = case_elem.find('case-file-header')
            if header is not None:
                record['filing_date'] = self._normalize_date(self._get_text(header.find('filing-date')))
                record['registration_date'] = self._normalize_date(self._get_text(header.find('registration-date')))
                record['status_code'] = self._get_text(header.find('status-code'))
                record['status_date'] = self._normalize_date(self._get_text(header.find('status-date')))
                record['mark_identification'] = self._get_text(header.find('mark-identification'))
                record['mark_drawing_code'] = self._get_text(header.find('mark-drawing-code'))
                record['publication_dt'] = self._normalize_date(self._get_text(header.find('published-for-opposition-date')))
                record['renewal_dt'] = self._normalize_date(self._get_text(header.find('renewal-date')))
                record['exm_office_cd'] = self._get_text(header.find('law-office-assigned-location-code'))
                # T/F flags to *_in
                def flag(tag):
                    v = self._get_text(header.find(tag))
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
            # IR block
            intl = case_elem.find('international-registration')
            if intl is not None:
                record['ir_registration_no'] = self._get_text(intl.find('international-registration-number'))
                record['ir_registration_dt'] = self._normalize_date(self._get_text(intl.find('international-registration-date')))
                record['ir_publication_dt'] = self._normalize_date(self._get_text(intl.find('international-publication-date')))
                record['ir_renewal_dt'] = self._normalize_date(self._get_text(intl.find('international-renewal-date')))
                record['ir_auto_reg_dt'] = self._normalize_date(self._get_text(intl.find('auto-protection-date')))
                record['ir_status_cd'] = self._get_text(intl.find('international-status-code'))
                record['ir_status_dt'] = self._normalize_date(self._get_text(intl.find('international-status-date')))
                prio_in = self._get_text(intl.find('priority-claimed-in'))
                record['ir_priority_in'] = 'T' if (prio_in and prio_in.upper().startswith('T')) else ('F' if prio_in else None)
                record['ir_priority_dt'] = self._normalize_date(self._get_text(intl.find('priority-claimed-date')))
                first_ref = self._get_text(intl.find('first-refusal-in'))
                record['ir_first_refus_in'] = 'T' if (first_ref and first_ref.upper().startswith('T')) else ('F' if first_ref else None)
            record['data_source'] = 'TRTDXFAP [XML]'
            record['batch_number'] = 0
            return record
        except Exception as e:
            self.logger.error(f"Error extracting TRTDXFAP case-file record: {e}")
            return None
    
    def _normalize_date(self, s: Optional[str]) -> Optional[str]:
        if not s or len(s) != 8 or not s.isdigit():
            return None
        y, m, d = s[:4], s[4:6], s[6:8]
        if m == '00' or d == '00':
            return None
        return f"{y}-{m}-{d}"
    
    def _get_text(self, el: Optional[ET.Element]) -> Optional[str]:
        return el.text.strip() if el is not None and el.text and el.text.strip() else None

class TRTYRAGProcessor(USPTOFileProcessor):
    """Product-specific XML processor for TRTYRAG/TRTDXFAG assignments"""
    
    def extract_assignment_records(self, root: ET.Element) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for entry in root.findall('.//assignment-entry'):
            base = self._extract_assignment_base(entry)
            if base is None:
                continue
            props = entry.find('properties')
            prop_list = props.findall('property') if props is not None else []
            if not prop_list:
                out.append(base)
                continue
            for prop in prop_list:
                rec = dict(base)
                rec['serial_no'] = self._text(prop.find('serial-no'))
                rec['registration_number'] = self._text(prop.find('registration-no'))
                rec['intl_reg_no'] = self._text(prop.find('intl-reg-no'))
                out.append(rec)
        return out
    
    def _extract_assignment_base(self, entry: ET.Element) -> Optional[Dict[str, Any]]:
        try:
            rec: Dict[str, Any] = {}
            assignment = entry.find('assignment')
            if assignment is not None:
                rec['reel_no'] = self._text(assignment.find('reel-no'))
                rec['frame_no'] = self._text(assignment.find('frame-no'))
                rec['date_recorded'] = self._norm(self._text(assignment.find('date-recorded')))
                rec['conveyance_text'] = self._text(assignment.find('conveyance-text'))
                rec['last_update_date'] = self._norm(self._text(assignment.find('last-update-date')))
                rec['purge_indicator'] = self._text(assignment.find('purge-indicator'))
                pc = self._text(assignment.find('page-count'))
                rec['page_count'] = int(pc) if pc and pc.isdigit() else None
            if rec.get('reel_no') and rec.get('frame_no'):
                rec['assignment_id'] = f"{rec['reel_no']}-{rec['frame_no']}"
            rec['assignor_name'] = self._text(entry.find('.//assignors/assignor/person-or-organization-name'))
            rec['assignee_name'] = self._text(entry.find('.//assignees/assignee/person-or-organization-name'))
            rec['data_source'] = 'TRTYRAG [XML]'
            rec['batch_number'] = 0
            return rec
        except Exception as e:
            self.logger.error(f"Error extracting TRTYRAG base: {e}")
            return None
    
    def _text(self, el: Optional[ET.Element]) -> Optional[str]:
        return el.text.strip() if el is not None and el.text and el.text.strip() else None
    
    def _norm(self, s: Optional[str]) -> Optional[str]:
        if not s or len(s) != 8 or not s.isdigit():
            return None
        y, m, d = s[:4], s[4:6], s[6:8]
        if m == '00' or d == '00':
            return None
        return f"{y}-{m}-{d}"

class TTABProcessor(USPTOFileProcessor):
    """Product-specific XML processor for TTAB datasets"""
    
    def extract_ttab_records(self, root: ET.Element) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        for elem in root.findall('.//proceeding') + root.findall('.//ttab-proceeding'):
            results.append(self._element_to_dict(elem))
        return results
    
    def _element_to_dict(self, element: ET.Element) -> Dict[str, Any]:
        return XMLProcessor(self.config)._xml_element_to_dict(element)
    def _extract_assignment_records(self, root: ET.Element) -> List[Dict[str, Any]]:
        """Extract assignment records from XML"""
        records = []
        
        # Find all assignment-entry elements
        assignment_entries = root.findall(".//assignment-entry")
        
        for assignment_elem in assignment_entries:
            record = self._extract_single_assignment(assignment_elem)
            if record:
                records.append(record)
        
        return records
    
    def _extract_single_assignment(self, assignment_elem: ET.Element) -> Dict[str, Any]:
        """Extract data from a single assignment-entry element"""
        
        record = {}
        
        try:
            # Extract assignment data
            assignment = assignment_elem.find('assignment')
            if assignment is not None:
                record['reel_no'] = self._get_text(assignment.find('reel-no'))
                record['frame_no'] = self._get_text(assignment.find('frame-no'))
                record['date_recorded'] = self._get_text(assignment.find('date-recorded'))
                record['conveyance_text'] = self._get_text(assignment.find('conveyance-text'))
                record['last_update_date'] = self._get_text(assignment.find('last-update-date'))
                record['purge_indicator'] = self._get_text(assignment.find('purge-indicator'))
                record['page_count'] = self._get_text(assignment.find('page-count'))
                
                # Extract correspondent data
                correspondent = assignment.find('correspondent')
                if correspondent is not None:
                    record['correspondent_name'] = self._get_text(correspondent.find('person-or-organization-name'))
                    record['correspondent_address1'] = self._get_text(correspondent.find('address-1'))
                    record['correspondent_address2'] = self._get_text(correspondent.find('address-2'))
                    record['correspondent_address3'] = self._get_text(correspondent.find('address-3'))
                    record['correspondent_address4'] = self._get_text(correspondent.find('address-4'))
            
            # Extract assignor data (take first assignor)
            assignors = assignment_elem.find('assignors')
            if assignors is not None:
                assignor = assignors.find('assignor')
                if assignor is not None:
                    record['assignor_name'] = self._get_text(assignor.find('person-or-organization-name'))
                    record['assignor_city'] = self._get_text(assignor.find('city'))
                    record['assignor_state'] = self._get_text(assignor.find('state'))
                    record['assignor_country'] = self._get_text(assignor.find('country-name'))
                    record['assignor_postcode'] = self._get_text(assignor.find('postcode'))
                    record['assignor_execution_date'] = self._get_text(assignor.find('execution-date'))
                    record['assignor_date_acknowledged'] = self._get_text(assignor.find('date-acknowledged'))
                    record['assignor_legal_entity'] = self._get_text(assignor.find('legal-entity-text'))
                    record['assignor_nationality'] = self._get_text(assignor.find('nationality'))
                    record['assignor_address1'] = self._get_text(assignor.find('address-1'))
                    record['assignor_address2'] = self._get_text(assignor.find('address-2'))
            
            # Extract assignee data (take first assignee)
            assignees = assignment_elem.find('assignees')
            if assignees is not None:
                assignee = assignees.find('assignee')
                if assignee is not None:
                    record['assignee_name'] = self._get_text(assignee.find('person-or-organization-name'))
                    record['assignee_city'] = self._get_text(assignee.find('city'))
                    record['assignee_state'] = self._get_text(assignee.find('state'))
                    record['assignee_country'] = self._get_text(assignee.find('country-name'))
                    record['assignee_postcode'] = self._get_text(assignee.find('postcode'))
                    record['assignee_legal_entity'] = self._get_text(assignee.find('legal-entity-text'))
                    record['assignee_nationality'] = self._get_text(assignee.find('nationality'))
                    record['assignee_address1'] = self._get_text(assignee.find('address-1'))
                    record['assignee_address2'] = self._get_text(assignee.find('address-2'))
            
            # Extract property data (take first property)
            properties = assignment_elem.find('properties')
            if properties is not None:
                property_elem = properties.find('property')
                if property_elem is not None:
                    record['serial_no'] = self._get_text(property_elem.find('serial-no'))
                    record['registration_number'] = self._get_text(property_elem.find('registration-no'))
                    record['intl_reg_no'] = self._get_text(property_elem.find('intl-reg-no'))
                    
                    # Extract trademark law treaty property
                    tlt_property = property_elem.find('trademark-law-treaty-property')
                    if tlt_property is not None:
                        record['tlt_mark_name'] = self._get_text(tlt_property.find('tlt-mark-name'))
                        record['tlt_mark_description'] = self._get_text(tlt_property.find('tlt-mark-description'))
            
            # Create assignment_id from reel_no and frame_no
            if record.get('reel_no') and record.get('frame_no'):
                record['assignment_id'] = f"{record['reel_no']}-{record['frame_no']}"
            
            return record
            
        except Exception as e:
            self.logger.error(f"Error extracting assignment data: {e}")
            return None
    
    def _get_text(self, element: ET.Element) -> str:
        """Get text content from XML element, return empty string if None"""
        if element is not None and element.text:
            return element.text.strip()
        return ""
    
    def _xml_element_to_dict(self, element: ET.Element) -> Dict[str, Any]:
        """Convert XML element to dictionary"""
        result = {}
        
        # Add attributes
        for attr_name, attr_value in element.attrib.items():
            result[f"@{attr_name}"] = attr_value
        
        # Add text content
        if element.text and element.text.strip():
            result['text'] = element.text.strip()
        
        # Add children
        for child in element:
            child_dict = self._xml_element_to_dict(child)
            if child.tag in result:
                if not isinstance(result[child.tag], list):
                    result[child.tag] = [result[child.tag]]
                result[child.tag].append(child_dict)
            else:
                result[child.tag] = child_dict
        
        return result

class DTAProcessor(USPTOFileProcessor):
    """Processor for DTA (Stata) files"""
    
    def process_file(self, file_path: str, product_id: str) -> Generator[List[Dict[str, Any]], None, None]:
        """Process DTA file in batches"""
        try:
            # Read DTA file
            df = pd.read_stata(file_path, encoding='utf-8')
            
            batch_number = 0
            batch_records = []
            
            for _, row in df.iterrows():
                record = row.to_dict()
                cleaned_record = self._clean_record(record, product_id)
                cleaned_record['batch_number'] = batch_number
                
                batch_records.append(cleaned_record)
                
                # Yield batch when full
                if len(batch_records) >= self.batch_size:
                    yield batch_records
                    batch_records = []
                    batch_number += 1
            
            # Yield remaining records
            if batch_records:
                yield batch_records
                
        except Exception as e:
            self.logger.error(f"Error processing DTA file {file_path}: {e}")
            raise

class ZIPProcessor(USPTOFileProcessor):
    """Processor for ZIP files"""
    
    def process_file(self, file_path: str, product_id: str) -> Generator[List[Dict[str, Any]], None, None]:
        """Process ZIP file by extracting and processing main data file"""
        try:
            with zipfile.ZipFile(file_path, 'r') as zip_file:
                # Find main data file
                main_file = self._find_main_data_file(zip_file.namelist())
                
                if not main_file:
                    raise ValueError(f"No main data file found in ZIP: {file_path}")
                
                # Extract main file
                with zip_file.open(main_file) as f:
                    temp_path = f"/tmp/{main_file}"
                    with open(temp_path, 'wb') as temp_file:
                        temp_file.write(f.read())
                
                try:
                    # Process extracted file based on extension
                    file_ext = Path(main_file).suffix.lower()
                    
                    if file_ext == '.csv':
                        processor = CSVProcessor(self.config)
                    elif file_ext == '.xml':
                        processor = XMLProcessor(self.config)
                    elif file_ext == '.dta':
                        processor = DTAProcessor(self.config)
                    else:
                        raise ValueError(f"Unsupported file format in ZIP: {file_ext}")
                    
                    # Process the extracted file
                    for batch in processor.process_file(temp_path, product_id):
                        yield batch
                        
                finally:
                    # Clean up temp file
                    if os.path.exists(temp_path):
                        os.remove(temp_path)
                        
        except Exception as e:
            self.logger.error(f"Error processing ZIP file {file_path}: {e}")
            raise
    
    def _find_main_data_file(self, file_list: List[str]) -> Optional[str]:
        """Find the main data file in ZIP archive"""
        # Priority order for main files
        priority_patterns = [
            r'.*\.csv$',
            r'.*\.xml$',
            r'.*\.dta$',
            r'case_file.*',
            r'assignment.*',
            r'proceeding.*'
        ]
        
        for pattern in priority_patterns:
            import re
            for file_name in file_list:
                if re.match(pattern, file_name, re.IGNORECASE):
                    return file_name
        
        # If no pattern matches, return first non-documentation file
        for file_name in file_list:
            if not any(doc_ext in file_name.lower() for doc_ext in ['.doc', '.pdf', '.txt', '.md']):
                return file_name
        
        return None

class USPTOProcessorFactory:
    """Factory for creating appropriate processors based on file type"""
    
    @staticmethod
    def create_processor(file_path: str, config: Dict[str, Any]) -> USPTOFileProcessor:
        """Create appropriate processor based on file extension"""
        file_ext = Path(file_path).suffix.lower()
        
        if file_ext == '.csv':
            return CSVProcessor(config)
        elif file_ext == '.xml':
            return XMLProcessor(config)
        elif file_ext == '.dta':
            return DTAProcessor(config)
        elif file_ext == '.zip':
            return ZIPProcessor(config)
        else:
            raise ValueError(f"Unsupported file format: {file_ext}")

def main():
    """Test the processors"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Test USPTO file processors")
    parser.add_argument("file_path", help="Path to file to process")
    parser.add_argument("--product-id", required=True, help="Product identifier")
    parser.add_argument("--batch-size", type=int, default=1000, help="Batch size for processing")
    
    args = parser.parse_args()
    
    # Load config
    config = {
        "processing": {
            "batch_size": args.batch_size,
            "chunk_size": 50000
        }
    }
    
    try:
        # Create processor
        processor = USPTOProcessorFactory.create_processor(args.file_path, config)
        
        print(f"🔄 Processing {args.file_path} with {processor.__class__.__name__}")
        print("=" * 50)
        
        total_records = 0
        batch_count = 0
        
        # Process file
        for batch in processor.process_file(args.file_path, args.product_id):
            batch_count += 1
            total_records += len(batch)
            
            print(f"Batch {batch_count}: {len(batch)} records")
            
            # Show sample record
            if batch_count == 1 and batch:
                print(f"Sample record: {batch[0]}")
        
        print("=" * 50)
        print(f"✅ Processing complete!")
        print(f"Total records: {total_records}")
        print(f"Total batches: {batch_count}")
        
    except Exception as e:
        print(f"❌ Error processing file: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
