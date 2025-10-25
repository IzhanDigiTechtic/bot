#!/usr/bin/env python3
"""
Test chunked XML processing
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from controllers.core.uspto_controllers import ProcessingController
from pathlib import Path

def test_chunked_processing():
    """Test the chunked XML processing"""
    
    print("Testing Chunked XML Processing...")
    print("=" * 50)
    
    # Create processing controller
    config = {
        'processing': {
            'batch_size': 1000,  # Small batch size for testing
            'chunk_size': 5000
        }
    }
    
    processor = ProcessingController(config)
    
    # Test with TTABYR XML file
    xml_file = Path("uspto_data/extracted/TTABYR/tt19511002-20241231-01/tt19511002-20241231-01.xml")
    
    if not xml_file.exists():
        print(f"❌ XML file not found: {xml_file}")
        return
    
    print(f"Processing XML file: {xml_file}")
    print(f"File size: {xml_file.stat().st_size / (1024*1024):.1f}MB")
    
    try:
        # Process the file with chunking
        batch_count = 0
        total_records = 0
        
        for batch in processor.process_xml_file(xml_file, 'TTABYR'):
            batch_count += 1
            total_records += len(batch)
            
            print(f"Batch {batch_count}: {len(batch)} records (total: {total_records})")
            
            # Show first record from first batch
            if batch_count == 1 and batch:
                print(f"\nFirst record sample:")
                first_record = batch[0]
                for key, value in first_record.items():
                    if value:  # Only show non-empty values
                        print(f"  {key}: {value}")
            
            # Stop after 5 batches for testing
            if batch_count >= 5:
                print(f"\nStopped after {batch_count} batches for testing")
                break
        
        print(f"\n✅ Processing test completed!")
        print(f"Total batches processed: {batch_count}")
        print(f"Total records processed: {total_records}")
        
    except Exception as e:
        print(f"❌ Error processing XML: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_chunked_processing()
