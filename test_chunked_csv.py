#!/usr/bin/env python3
"""
Test chunked CSV processing
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from controllers.core.uspto_controllers import ProcessingController
from pathlib import Path

def test_chunked_csv_processing():
    """Test the chunked CSV processing"""
    
    print("Testing Chunked CSV Processing...")
    print("=" * 50)
    
    # Create processing controller
    config = {
        'processing': {
            'batch_size': 1000,  # Small batch size for testing
            'chunk_size': 10000  # Small chunk size for testing
        }
    }
    
    processor = ProcessingController(config)
    
    # Test with TRCFECO2 CSV file
    csv_file = Path("uspto_data/extracted/TRCFECO2/case_file.csv/case_file.csv")
    
    if not csv_file.exists():
        print(f"❌ CSV file not found: {csv_file}")
        return
    
    print(f"Processing CSV file: {csv_file}")
    print(f"File size: {csv_file.stat().st_size / (1024*1024):.1f}MB")
    
    try:
        # Process the file with chunking
        batch_count = 0
        total_records = 0
        
        for batch in processor.process_csv_file(csv_file, 'TRCFECO2'):
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
        
        print(f"\n✅ CSV processing test completed!")
        print(f"Total batches processed: {batch_count}")
        print(f"Total records processed: {total_records}")
        
    except Exception as e:
        print(f"❌ Error processing CSV: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_chunked_csv_processing()
