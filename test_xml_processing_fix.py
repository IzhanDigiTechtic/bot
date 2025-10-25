#!/usr/bin/env python3
"""
Test the XML processing fix for TRTYRAG
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from controllers.core.file_processors import XMLProcessor

def test_xml_processing_fix():
    """Test the XML processing fix for TRTYRAG"""
    
    print("Testing XML Processing Fix for TRTYRAG...")
    print("=" * 60)
    
    # Create XML processor
    config = {
        'processing': {
            'batch_size': 1000,
            'chunk_size': 5000
        }
    }
    
    processor = XMLProcessor(config)
    
    # Test with TRTYRAG XML file
    xml_file = "uspto_data/extracted/TRTYRAG/asb19550103-20241231-01/asb19550103-20241231-01.xml"
    
    if not os.path.exists(xml_file):
        print(f"❌ XML file not found: {xml_file}")
        return
    
    print(f"Processing XML file: {xml_file}")
    
    try:
        # Process the file
        batches = list(processor.process_file(xml_file, 'TRTYRAG'))
        
        print(f"Generated {len(batches)} batches")
        
        if batches:
            first_batch = batches[0]
            print(f"First batch has {len(first_batch)} records")
            
            if first_batch:
                first_record = first_batch[0]
                print("\nFirst record extracted:")
                
                # Show non-empty fields
                non_empty_fields = {k: v for k, v in first_record.items() if v}
                for key, value in non_empty_fields.items():
                    print(f"  {key}: {value}")
                
                # Check key fields
                key_fields = ['assignment_id', 'serial_no', 'registration_number', 'assignor_name', 'assignee_name']
                print(f"\nKey field check:")
                for field in key_fields:
                    if field in first_record and first_record[field]:
                        print(f"  ✅ {field}: {first_record[field]}")
                    else:
                        print(f"  ❌ {field}: Missing or empty")
                
                # Count total non-empty fields
                total_fields = len(first_record)
                non_empty_count = len(non_empty_fields)
                print(f"\nField usage: {non_empty_count}/{total_fields} fields have data ({non_empty_count/total_fields*100:.1f}%)")
        
    except Exception as e:
        print(f"❌ Error processing XML: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_xml_processing_fix()
