#!/usr/bin/env python3
"""
USPTO Processor Test Suite
Tests all file processors with actual USPTO data structures.
"""

import sys
import os
import json
import tempfile
from pathlib import Path
from typing import Dict, List, Any
import logging

# Add controllers to path
sys.path.insert(0, str(Path(__file__).parent / "controllers"))

from controllers.core.file_processors import USPTOProcessorFactory
from controllers.utils.file_analyzer import USPTOFileAnalyzer

class USPTOProcessorTester:
    """Test suite for USPTO file processors"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.test_results = {}
        
    def test_all_processors(self) -> Dict[str, Any]:
        """Test all processors with sample data"""
        print("🧪 USPTO Processor Test Suite")
        print("=" * 50)
        
        # Test CSV processor
        self._test_csv_processor()
        
        # Test XML processor
        self._test_xml_processor()
        
        # Test DTA processor
        self._test_dta_processor()
        
        # Test ZIP processor
        self._test_zip_processor()
        
        # Test file analyzer
        self._test_file_analyzer()
        
        # Print results
        self._print_results()
        
        return self.test_results
    
    def _test_csv_processor(self):
        """Test CSV processor with sample data"""
        print("\n📊 Testing CSV Processor...")
        
        try:
            # Create sample CSV data
            sample_csv = self._create_sample_csv()
            
            # Test processor
            processor = USPTOProcessorFactory.create_processor(sample_csv, self.config)
            
            total_records = 0
            batch_count = 0
            
            for batch in processor.process_file(sample_csv, "TRCFECO2"):
                batch_count += 1
                total_records += len(batch)
                
                # Verify batch structure
                if batch:
                    sample_record = batch[0]
                    required_fields = ['data_source', 'file_hash', 'batch_number']
                    for field in required_fields:
                        if field not in sample_record:
                            raise ValueError(f"Missing required field: {field}")
            
            self.test_results['csv_processor'] = {
                'status': 'PASS',
                'total_records': total_records,
                'batch_count': batch_count,
                'message': f'Successfully processed {total_records} records in {batch_count} batches'
            }
            
            print(f"✅ CSV Processor: {total_records} records, {batch_count} batches")
            
        except Exception as e:
            self.test_results['csv_processor'] = {
                'status': 'FAIL',
                'error': str(e),
                'message': f'CSV processor failed: {e}'
            }
            print(f"❌ CSV Processor failed: {e}")
        
        finally:
            # Clean up
            if 'sample_csv' in locals() and os.path.exists(sample_csv):
                os.remove(sample_csv)
    
    def _test_xml_processor(self):
        """Test XML processor with sample data"""
        print("\n📄 Testing XML Processor...")
        
        try:
            # Create sample XML data
            sample_xml = self._create_sample_xml()
            
            # Test processor
            processor = USPTOProcessorFactory.create_processor(sample_xml, self.config)
            
            total_records = 0
            batch_count = 0
            
            for batch in processor.process_file(sample_xml, "TRTDXFAP"):
                batch_count += 1
                total_records += len(batch)
                
                # Verify batch structure
                if batch:
                    sample_record = batch[0]
                    required_fields = ['data_source', 'file_hash', 'batch_number']
                    for field in required_fields:
                        if field not in sample_record:
                            raise ValueError(f"Missing required field: {field}")
            
            self.test_results['xml_processor'] = {
                'status': 'PASS',
                'total_records': total_records,
                'batch_count': batch_count,
                'message': f'Successfully processed {total_records} records in {batch_count} batches'
            }
            
            print(f"✅ XML Processor: {total_records} records, {batch_count} batches")
            
        except Exception as e:
            self.test_results['xml_processor'] = {
                'status': 'FAIL',
                'error': str(e),
                'message': f'XML processor failed: {e}'
            }
            print(f"❌ XML Processor failed: {e}")
        
        finally:
            # Clean up
            if 'sample_xml' in locals() and os.path.exists(sample_xml):
                os.remove(sample_xml)
    
    def _test_dta_processor(self):
        """Test DTA processor with sample data"""
        print("\n📈 Testing DTA Processor...")
        
        try:
            # Create sample DTA data
            sample_dta = self._create_sample_dta()
            
            # Test processor
            processor = USPTOProcessorFactory.create_processor(sample_dta, self.config)
            
            total_records = 0
            batch_count = 0
            
            for batch in processor.process_file(sample_dta, "TRASECO"):
                batch_count += 1
                total_records += len(batch)
                
                # Verify batch structure
                if batch:
                    sample_record = batch[0]
                    required_fields = ['data_source', 'file_hash', 'batch_number']
                    for field in required_fields:
                        if field not in sample_record:
                            raise ValueError(f"Missing required field: {field}")
            
            self.test_results['dta_processor'] = {
                'status': 'PASS',
                'total_records': total_records,
                'batch_count': batch_count,
                'message': f'Successfully processed {total_records} records in {batch_count} batches'
            }
            
            print(f"✅ DTA Processor: {total_records} records, {batch_count} batches")
            
        except Exception as e:
            self.test_results['dta_processor'] = {
                'status': 'FAIL',
                'error': str(e),
                'message': f'DTA processor failed: {e}'
            }
            print(f"❌ DTA Processor failed: {e}")
        
        finally:
            # Clean up
            if 'sample_dta' in locals() and os.path.exists(sample_dta):
                os.remove(sample_dta)
    
    def _test_zip_processor(self):
        """Test ZIP processor with sample data"""
        print("\n📦 Testing ZIP Processor...")
        
        try:
            # Create sample ZIP data
            sample_zip = self._create_sample_zip()
            
            # Test processor
            processor = USPTOProcessorFactory.create_processor(sample_zip, self.config)
            
            total_records = 0
            batch_count = 0
            
            for batch in processor.process_file(sample_zip, "TRCFECO2"):
                batch_count += 1
                total_records += len(batch)
                
                # Verify batch structure
                if batch:
                    sample_record = batch[0]
                    required_fields = ['data_source', 'file_hash', 'batch_number']
                    for field in required_fields:
                        if field not in sample_record:
                            raise ValueError(f"Missing required field: {field}")
            
            self.test_results['zip_processor'] = {
                'status': 'PASS',
                'total_records': total_records,
                'batch_count': batch_count,
                'message': f'Successfully processed {total_records} records in {batch_count} batches'
            }
            
            print(f"✅ ZIP Processor: {total_records} records, {batch_count} batches")
            
        except Exception as e:
            self.test_results['zip_processor'] = {
                'status': 'FAIL',
                'error': str(e),
                'message': f'ZIP processor failed: {e}'
            }
            print(f"❌ ZIP Processor failed: {e}")
        
        finally:
            # Clean up
            if 'sample_zip' in locals() and os.path.exists(sample_zip):
                os.remove(sample_zip)
    
    def _test_file_analyzer(self):
        """Test file analyzer with sample data"""
        print("\n🔍 Testing File Analyzer...")
        
        try:
            analyzer = USPTOFileAnalyzer(self.config)
            
            # Test CSV analysis
            sample_csv = self._create_sample_csv()
            csv_schema = analyzer.analyze_file_structure(sample_csv, "TRCFECO2")
            
            # Test XML analysis
            sample_xml = self._create_sample_xml()
            xml_schema = analyzer.analyze_file_structure(sample_xml, "TRTDXFAP")
            
            # Verify schema structure
            required_fields = ['file_type', 'product_id', 'columns', 'primary_key']
            for schema in [csv_schema, xml_schema]:
                for field in required_fields:
                    if field not in schema:
                        raise ValueError(f"Missing schema field: {field}")
            
            self.test_results['file_analyzer'] = {
                'status': 'PASS',
                'csv_schema': csv_schema,
                'xml_schema': xml_schema,
                'message': 'Successfully analyzed CSV and XML file structures'
            }
            
            print(f"✅ File Analyzer: Analyzed CSV and XML structures")
            
        except Exception as e:
            self.test_results['file_analyzer'] = {
                'status': 'FAIL',
                'error': str(e),
                'message': f'File analyzer failed: {e}'
            }
            print(f"❌ File Analyzer failed: {e}")
        
        finally:
            # Clean up
            for file_path in ['sample_csv', 'sample_xml']:
                if file_path in locals() and os.path.exists(locals()[file_path]):
                    os.remove(locals()[file_path])
    
    def _create_sample_csv(self) -> str:
        """Create sample CSV file for testing"""
        import pandas as pd
        
        sample_data = {
            'serial_number': ['12345678', '87654321', '11223344'],
            'registration_number': ['1234567', '7654321', '1122334'],
            'filing_date': ['2023-01-01', '2023-02-01', '2023-03-01'],
            'registration_date': ['2023-06-01', '2023-07-01', '2023-08-01'],
            'status_code': ['REGISTERED', 'PENDING', 'REGISTERED'],
            'mark_identification': ['Sample Mark 1', 'Sample Mark 2', 'Sample Mark 3'],
            'goods_and_services': ['Computer software', 'Clothing', 'Food products']
        }
        
        df = pd.DataFrame(sample_data)
        
        # Create temporary file
        temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
        df.to_csv(temp_file.name, index=False)
        temp_file.close()
        
        return temp_file.name
    
    def _create_sample_xml(self) -> str:
        """Create sample XML file for testing"""
        sample_xml = '''<?xml version="1.0" encoding="UTF-8"?>
<trademark-application>
    <application>
        <serial-number>12345678</serial-number>
        <registration-number>1234567</registration-number>
        <filing-date>2023-01-01</filing-date>
        <registration-date>2023-06-01</registration-date>
        <mark-identification>Sample Mark 1</mark-identification>
        <goods-and-services>Computer software</goods-and-services>
        <status-code>REGISTERED</status-code>
    </application>
    <application>
        <serial-number>87654321</serial-number>
        <registration-number>7654321</registration-number>
        <filing-date>2023-02-01</filing-date>
        <registration-date>2023-07-01</registration-date>
        <mark-identification>Sample Mark 2</mark-identification>
        <goods-and-services>Clothing</goods-and-services>
        <status-code>PENDING</status-code>
    </application>
</trademark-application>'''
        
        # Create temporary file
        temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.xml', delete=False)
        temp_file.write(sample_xml)
        temp_file.close()
        
        return temp_file.name
    
    def _create_sample_dta(self) -> str:
        """Create sample DTA file for testing"""
        import pandas as pd
        
        sample_data = {
            'assignment_id': ['ASS001', 'ASS002', 'ASS003'],
            'serial_number': ['12345678', '87654321', '11223344'],
            'date_recorded': ['2023-01-01', '2023-02-01', '2023-03-01'],
            'conveyance_text': ['Assignment', 'Assignment', 'Assignment'],
            'assignee_name': ['Assignee 1', 'Assignee 2', 'Assignee 3'],
            'assignor_name': ['Assignor 1', 'Assignor 2', 'Assignor 3']
        }
        
        df = pd.DataFrame(sample_data)
        
        # Create temporary file
        temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.dta', delete=False)
        df.to_stata(temp_file.name, write_index=False)
        temp_file.close()
        
        return temp_file.name
    
    def _create_sample_zip(self) -> str:
        """Create sample ZIP file for testing"""
        import zipfile
        
        # Create sample CSV content
        csv_content = '''serial_number,registration_number,filing_date,status_code
12345678,1234567,2023-01-01,REGISTERED
87654321,7654321,2023-02-01,PENDING
11223344,1122334,2023-03-01,REGISTERED'''
        
        # Create temporary ZIP file
        temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.zip', delete=False)
        temp_file.close()
        
        with zipfile.ZipFile(temp_file.name, 'w') as zip_file:
            zip_file.writestr('sample_data.csv', csv_content)
        
        return temp_file.name
    
    def _print_results(self):
        """Print test results summary"""
        print("\n" + "=" * 50)
        print("📊 Test Results Summary")
        print("=" * 50)
        
        passed = 0
        failed = 0
        
        for test_name, result in self.test_results.items():
            status = result['status']
            message = result['message']
            
            if status == 'PASS':
                print(f"✅ {test_name}: {message}")
                passed += 1
            else:
                print(f"❌ {test_name}: {message}")
                failed += 1
        
        print("=" * 50)
        print(f"Total Tests: {passed + failed}")
        print(f"Passed: {passed}")
        print(f"Failed: {failed}")
        
        if failed == 0:
            print("🎉 All tests passed!")
        else:
            print("⚠️ Some tests failed. Check the errors above.")

def main():
    """Main test function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Test USPTO file processors")
    parser.add_argument("--config", default="uspto_config.json", help="Configuration file")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    
    args = parser.parse_args()
    
    # Setup logging
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    
    # Load configuration
    try:
        with open(args.config, 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        print(f"❌ Configuration file not found: {args.config}")
        return 1
    except json.JSONDecodeError as e:
        print(f"❌ Invalid configuration file: {e}")
        return 1
    
    # Run tests
    tester = USPTOProcessorTester(config)
    results = tester.test_all_processors()
    
    # Check if all tests passed
    failed_tests = [name for name, result in results.items() if result['status'] == 'FAIL']
    
    if failed_tests:
        print(f"\n❌ {len(failed_tests)} test(s) failed: {', '.join(failed_tests)}")
        return 1
    else:
        print(f"\n✅ All tests passed!")
        return 0

if __name__ == "__main__":
    exit(main())
