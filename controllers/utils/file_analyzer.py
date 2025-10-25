#!/usr/bin/env python3
"""
USPTO File Structure Analyzer
Analyzes different file formats (CSV, XML, DTA) to determine their structure
and create appropriate database schemas.
"""

import pandas as pd
import xml.etree.ElementTree as ET
import zipfile
import json
import os
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import logging

class USPTOFileAnalyzer:
    """Analyzes USPTO file structures to determine database schemas"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
    def analyze_file_structure(self, file_path: str, product_id: str) -> Dict[str, Any]:
        """Analyze file structure and return schema information"""
        file_ext = Path(file_path).suffix.lower()
        
        if file_ext == '.csv':
            return self._analyze_csv_structure(file_path, product_id)
        elif file_ext == '.xml':
            return self._analyze_xml_structure(file_path, product_id)
        elif file_ext == '.dta':
            return self._analyze_dta_structure(file_path, product_id)
        elif file_ext == '.zip':
            return self._analyze_zip_structure(file_path, product_id)
        else:
            raise ValueError(f"Unsupported file format: {file_ext}")
    
    def _analyze_csv_structure(self, file_path: str, product_id: str) -> Dict[str, Any]:
        """Analyze CSV file structure"""
        try:
            # Read first few rows to understand structure
            df_sample = pd.read_csv(file_path, nrows=1000, encoding='utf-8')
            
            schema_info = {
                'file_type': 'csv',
                'product_id': product_id,
                'total_rows': self._count_csv_rows(file_path),
                'columns': list(df_sample.columns),
                'column_count': len(df_sample.columns),
                'sample_data': df_sample.head(5).to_dict('records'),
                'data_types': df_sample.dtypes.to_dict(),
                'null_counts': df_sample.isnull().sum().to_dict(),
                'unique_counts': {col: df_sample[col].nunique() for col in df_sample.columns}
            }
            
            # Determine primary key based on product type
            schema_info['primary_key'] = self._determine_primary_key(product_id, df_sample.columns)
            
            # Analyze data patterns
            schema_info['patterns'] = self._analyze_csv_patterns(df_sample, product_id)
            
            return schema_info
            
        except Exception as e:
            self.logger.error(f"Error analyzing CSV structure: {e}")
            raise
    
    def _analyze_xml_structure(self, file_path: str, product_id: str) -> Dict[str, Any]:
        """Analyze XML file structure"""
        try:
            # Parse XML to understand structure
            tree = ET.parse(file_path)
            root = tree.getroot()
            
            schema_info = {
                'file_type': 'xml',
                'product_id': product_id,
                'root_element': root.tag,
                'namespaces': self._extract_namespaces(root),
                'elements': self._analyze_xml_elements(root),
                'attributes': self._extract_xml_attributes(root),
                'sample_records': self._extract_xml_sample_records(root, 5)
            }
            
            # Determine schema type based on product
            schema_info['schema_type'] = self._determine_xml_schema_type(product_id, root)
            
            # Extract field mappings
            schema_info['field_mappings'] = self._create_xml_field_mappings(root, product_id)
            
            return schema_info
            
        except Exception as e:
            self.logger.error(f"Error analyzing XML structure: {e}")
            raise
    
    def _analyze_dta_structure(self, file_path: str, product_id: str) -> Dict[str, Any]:
        """Analyze DTA (Stata) file structure"""
        try:
            # Convert DTA to CSV for analysis
            df = pd.read_stata(file_path, encoding='utf-8')
            
            schema_info = {
                'file_type': 'dta',
                'product_id': product_id,
                'total_rows': len(df),
                'columns': list(df.columns),
                'column_count': len(df.columns),
                'sample_data': df.head(5).to_dict('records'),
                'data_types': df.dtypes.to_dict(),
                'null_counts': df.isnull().sum().to_dict(),
                'unique_counts': {col: df[col].nunique() for col in df.columns}
            }
            
            # Determine primary key
            schema_info['primary_key'] = self._determine_primary_key(product_id, df.columns)
            
            return schema_info
            
        except Exception as e:
            self.logger.error(f"Error analyzing DTA structure: {e}")
            raise
    
    def _analyze_zip_structure(self, file_path: str, product_id: str) -> Dict[str, Any]:
        """Analyze ZIP file structure"""
        try:
            with zipfile.ZipFile(file_path, 'r') as zip_file:
                file_list = zip_file.namelist()
                
                schema_info = {
                    'file_type': 'zip',
                    'product_id': product_id,
                    'files_in_zip': file_list,
                    'file_count': len(file_list),
                    'total_size': sum(zip_file.getinfo(f).file_size for f in file_list)
                }
                
                # Analyze the main data file
                main_file = self._find_main_data_file(file_list)
                if main_file:
                    # Extract and analyze the main file
                    with zip_file.open(main_file) as f:
                        temp_path = f"/tmp/{main_file}"
                        with open(temp_path, 'wb') as temp_file:
                            temp_file.write(f.read())
                        
                        # Analyze the extracted file
                        file_schema = self.analyze_file_structure(temp_path, product_id)
                        schema_info['main_file_schema'] = file_schema
                        
                        # Clean up temp file
                        os.remove(temp_path)
                
                return schema_info
                
        except Exception as e:
            self.logger.error(f"Error analyzing ZIP structure: {e}")
            raise
    
    def _count_csv_rows(self, file_path: str) -> int:
        """Count total rows in CSV file efficiently"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return sum(1 for line in f) - 1  # Subtract header
        except:
            return 0
    
    def _determine_primary_key(self, product_id: str, columns: List[str]) -> str:
        """Determine primary key based on product type and available columns"""
        primary_key_mapping = {
            'TRCFECO2': 'serial_no',
            'TRASECO': 'assignment_id',
            'TRTDXFAP': 'serial_no',
            'TRTDXFAG': 'assignment_id',
            'TRTYRAP': 'serial_no',
            'TRTYRAG': 'assignment_id',
            'TTABTDXF': 'proceeding_number',
            'TTABYR': 'proceeding_number'
        }
        
        preferred_key = primary_key_mapping.get(product_id, 'id')
        
        # Check if preferred key exists in columns
        if preferred_key in columns:
            return preferred_key
        
        # Fallback to common primary key patterns
        for pattern in ['serial_no', 'registration_number', 'assignment_id', 'proceeding_number', 'id']:
            if pattern in columns:
                return pattern
        
        # If no pattern matches, use first column
        return columns[0] if columns else 'id'
    
    def _analyze_csv_patterns(self, df: pd.DataFrame, product_id: str) -> Dict[str, Any]:
        """Analyze patterns in CSV data"""
        patterns = {}
        
        # Date patterns
        date_columns = [col for col in df.columns if 'date' in col.lower()]
        patterns['date_columns'] = date_columns
        
        # Numeric patterns
        numeric_columns = df.select_dtypes(include=['int64', 'float64']).columns.tolist()
        patterns['numeric_columns'] = numeric_columns
        
        # Text patterns
        text_columns = df.select_dtypes(include=['object']).columns.tolist()
        patterns['text_columns'] = text_columns
        
        # Product-specific patterns
        if product_id == 'TRCFECO2':
            patterns['case_file_fields'] = [col for col in df.columns if any(
                keyword in col.lower() for keyword in ['serial', 'registration', 'mark', 'status', 'filing']
            )]
        elif product_id == 'TRASECO':
            patterns['assignment_fields'] = [col for col in df.columns if any(
                keyword in col.lower() for keyword in ['assignment', 'assignee', 'assignor', 'conveyance']
            )]
        
        return patterns
    
    def _extract_namespaces(self, root: ET.Element) -> Dict[str, str]:
        """Extract XML namespaces"""
        namespaces = {}
        for prefix, uri in root.attrib.items():
            if prefix.startswith('xmlns'):
                namespaces[prefix] = uri
        return namespaces
    
    def _analyze_xml_elements(self, root: ET.Element) -> Dict[str, Any]:
        """Analyze XML element structure"""
        elements = {}
        
        def analyze_element(element, path=""):
            current_path = f"{path}/{element.tag}" if path else element.tag
            
            if current_path not in elements:
                elements[current_path] = {
                    'count': 0,
                    'attributes': list(element.attrib.keys()),
                    'children': set(),
                    'has_text': bool(element.text and element.text.strip())
                }
            
            elements[current_path]['count'] += 1
            
            for child in element:
                elements[current_path]['children'].add(child.tag)
                analyze_element(child, current_path)
        
        analyze_element(root)
        
        # Convert sets to lists for JSON serialization
        for element_info in elements.values():
            element_info['children'] = list(element_info['children'])
        
        return elements
    
    def _extract_xml_attributes(self, root: ET.Element) -> Dict[str, List[str]]:
        """Extract all XML attributes"""
        attributes = {}
        
        def collect_attributes(element):
            for attr_name, attr_value in element.attrib.items():
                if attr_name not in attributes:
                    attributes[attr_name] = []
                if attr_value not in attributes[attr_name]:
                    attributes[attr_name].append(attr_value)
            
            for child in element:
                collect_attributes(child)
        
        collect_attributes(root)
        return attributes
    
    def _extract_xml_sample_records(self, root: ET.Element, count: int = 5) -> List[Dict[str, Any]]:
        """Extract sample records from XML"""
        records = []
        
        # Find record elements (common patterns)
        record_patterns = ['record', 'application', 'assignment', 'proceeding', 'trademark']
        
        for pattern in record_patterns:
            elements = root.findall(f".//{pattern}")
            if elements:
                for i, element in enumerate(elements[:count]):
                    record = self._xml_element_to_dict(element)
                    records.append(record)
                break
        
        return records
    
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
    
    def _determine_xml_schema_type(self, product_id: str, root: ET.Element) -> str:
        """Determine XML schema type based on product and structure"""
        schema_mapping = {
            'TRTDXFAP': 'trademark_application',
            'TRTDXFAG': 'trademark_assignment',
            'TRTYRAP': 'trademark_application',
            'TRTYRAG': 'trademark_assignment',
            'TTABTDXF': 'ttab_proceeding',
            'TTABYR': 'ttab_proceeding'
        }
        
        return schema_mapping.get(product_id, 'generic')
    
    def _create_xml_field_mappings(self, root: ET.Element, product_id: str) -> Dict[str, str]:
        """Create field mappings for XML to database columns"""
        mappings = {}
        
        # Common field mappings
        common_mappings = {
            'serial-number': 'serial_no',
            'registration-number': 'registration_number',
            'filing-date': 'filing_date',
            'registration-date': 'registration_date',
            'mark-identification': 'mark_identification',
            'goods-and-services': 'goods_and_services',
            'status-code': 'status_code',
            'status-date': 'status_date'
        }
        
        # Product-specific mappings
        if 'assignment' in product_id.lower():
            common_mappings.update({
                'assignment-id': 'assignment_id',
                'assignee-name': 'assignee_name',
                'assignor-name': 'assignor_name',
                'conveyance-text': 'conveyance_text',
                'date-recorded': 'date_recorded'
            })
        
        if 'ttab' in product_id.lower():
            common_mappings.update({
                'proceeding-number': 'proceeding_number',
                'proceeding-type': 'proceeding_type',
                'filing-date': 'filing_date',
                'status': 'status'
            })
        
        # Extract actual field names from XML
        def extract_fields(element, path=""):
            current_path = f"{path}/{element.tag}" if path else element.tag
            
            # Map to database column name
            db_column = common_mappings.get(element.tag, element.tag.replace('-', '_'))
            mappings[current_path] = db_column
            
            for child in element:
                extract_fields(child, current_path)
        
        extract_fields(root)
        return mappings
    
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
    
    def generate_database_schema(self, schema_info: Dict[str, Any]) -> str:
        """Generate SQL CREATE TABLE statement based on schema analysis"""
        product_id = schema_info['product_id']
        file_type = schema_info['file_type']
        
        table_name = f"product_{product_id.lower()}"
        
        if file_type == 'csv' or file_type == 'dta':
            return self._generate_csv_schema(schema_info, table_name)
        elif file_type == 'xml':
            return self._generate_xml_schema(schema_info, table_name)
        else:
            return self._generate_generic_schema(schema_info, table_name)
    
    def _generate_csv_schema(self, schema_info: Dict[str, Any], table_name: str) -> str:
        """Generate schema for CSV/DTA files"""
        columns = schema_info['columns']
        primary_key = schema_info['primary_key']
        data_types = schema_info['data_types']
        
        sql_parts = [f"CREATE TABLE {table_name} ("]
        sql_parts.append("    id SERIAL PRIMARY KEY,")
        
        for col in columns:
            if col == primary_key:
                continue
            
            # Map pandas dtypes to PostgreSQL types
            dtype = str(data_types[col])
            if 'int' in dtype:
                pg_type = "INTEGER"
            elif 'float' in dtype:
                pg_type = "DECIMAL"
            elif 'bool' in dtype:
                pg_type = "BOOLEAN"
            elif 'datetime' in dtype:
                pg_type = "TIMESTAMP"
            else:
                pg_type = "TEXT"
            
            sql_parts.append(f"    {col} {pg_type},")
        
        # Add primary key constraint
        sql_parts.append(f"    {primary_key} VARCHAR(50) UNIQUE,")
        
        # Add metadata columns
        sql_parts.extend([
            "    data_source VARCHAR(100),",
            "    file_hash VARCHAR(64),",
            "    batch_number INTEGER,",
            "    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,",
            "    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
        ])
        
        sql_parts.append(");")
        
        return "\n".join(sql_parts)
    
    def _generate_xml_schema(self, schema_info: Dict[str, Any], table_name: str) -> str:
        """Generate schema for XML files"""
        schema_type = schema_info.get('schema_type', 'generic')
        
        if schema_type == 'trademark_application':
            return self._generate_trademark_application_schema(table_name)
        elif schema_type == 'trademark_assignment':
            return self._generate_trademark_assignment_schema(table_name)
        elif schema_type == 'ttab_proceeding':
            return self._generate_ttab_proceeding_schema(table_name)
        else:
            return self._generate_generic_xml_schema(schema_info, table_name)
    
    def _generate_trademark_application_schema(self, table_name: str) -> str:
        """Generate schema for trademark application XML"""
        return f"""
CREATE TABLE {table_name} (
    id SERIAL PRIMARY KEY,
    serial_no VARCHAR(20) UNIQUE,
    registration_number VARCHAR(20),
    filing_date DATE,
    registration_date DATE,
    mark_identification TEXT,
    mark_drawing_code VARCHAR(10),
    goods_and_services TEXT,
    classification_code VARCHAR(10),
    status_code VARCHAR(10),
    status_date DATE,
    owner_name TEXT,
    owner_address TEXT,
    attorney_name TEXT,
    attorney_address TEXT,
    data_source VARCHAR(100),
    file_hash VARCHAR(64),
    batch_number INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);"""
    
    def _generate_trademark_assignment_schema(self, table_name: str) -> str:
        """Generate schema for trademark assignment XML"""
        return f"""
CREATE TABLE {table_name} (
    id SERIAL PRIMARY KEY,
    assignment_id VARCHAR(50) UNIQUE,
    serial_no VARCHAR(20),
    registration_number VARCHAR(20),
    date_recorded DATE,
    conveyance_text TEXT,
    assignee_name TEXT,
    assignee_address TEXT,
    assignor_name TEXT,
    assignor_address TEXT,
    frame_no VARCHAR(10),
    reel_no VARCHAR(10),
    data_source VARCHAR(100),
    file_hash VARCHAR(64),
    batch_number INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);"""
    
    def _generate_ttab_proceeding_schema(self, table_name: str) -> str:
        """Generate schema for TTAB proceeding XML"""
        return f"""
CREATE TABLE {table_name} (
    id SERIAL PRIMARY KEY,
    proceeding_number VARCHAR(20) UNIQUE,
    proceeding_type VARCHAR(50),
    filing_date DATE,
    status VARCHAR(50),
    status_date DATE,
    applicant_name TEXT,
    applicant_address TEXT,
    respondent_name TEXT,
    respondent_address TEXT,
    attorney_name TEXT,
    attorney_address TEXT,
    mark_identification TEXT,
    goods_and_services TEXT,
    data_source VARCHAR(100),
    file_hash VARCHAR(64),
    batch_number INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);"""
    
    def _generate_generic_xml_schema(self, schema_info: Dict[str, Any], table_name: str) -> str:
        """Generate generic schema for XML files"""
        return f"""
CREATE TABLE {table_name} (
    id SERIAL PRIMARY KEY,
    xml_data JSONB,
    data_source VARCHAR(100),
    file_hash VARCHAR(64),
    batch_number INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);"""
    
    def _generate_generic_schema(self, schema_info: Dict[str, Any], table_name: str) -> str:
        """Generate generic schema for unknown file types"""
        return f"""
CREATE TABLE {table_name} (
    id SERIAL PRIMARY KEY,
    raw_data JSONB,
    data_source VARCHAR(100),
    file_hash VARCHAR(64),
    batch_number INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);"""

def main():
    """Test the file analyzer"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Analyze USPTO file structures")
    parser.add_argument("file_path", help="Path to file to analyze")
    parser.add_argument("--product-id", required=True, help="Product identifier")
    parser.add_argument("--output", help="Output file for schema")
    
    args = parser.parse_args()
    
    # Load config
    config = {
        "processing": {
            "batch_size": 10000,
            "chunk_size": 50000
        }
    }
    
    analyzer = USPTOFileAnalyzer(config)
    
    try:
        # Analyze file structure
        schema_info = analyzer.analyze_file_structure(args.file_path, args.product_id)
        
        print(f"📊 Analysis Results for {args.product_id}")
        print("=" * 50)
        print(f"File Type: {schema_info['file_type']}")
        print(f"Total Rows: {schema_info.get('total_rows', 'N/A')}")
        print(f"Columns: {schema_info.get('column_count', 'N/A')}")
        print(f"Primary Key: {schema_info.get('primary_key', 'N/A')}")
        
        # Generate database schema
        schema_sql = analyzer.generate_database_schema(schema_info)
        
        if args.output:
            with open(args.output, 'w') as f:
                f.write(schema_sql)
            print(f"✅ Schema saved to {args.output}")
        else:
            print("\n📋 Generated Database Schema:")
            print(schema_sql)
        
    except Exception as e:
        print(f"❌ Error analyzing file: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
