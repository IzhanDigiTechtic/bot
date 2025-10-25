#!/usr/bin/env python3
"""
Improved USPTO processor with data validation and proper error handling
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from controllers.core.uspto_controller_runner import run_controller_processor

def main():
    """Run the improved processor"""
    
    print("Starting Improved USPTO Data Processing")
    print("=" * 60)
    
    # Configuration for fresh start
    config_file = "uspto_controller_config.json"
    
    # Run with improved settings
    success = run_controller_processor(
        config_file=config_file,
        max_files=10,  # Process more files per product
        force_redownload=True,  # Force fresh download
        batch_size=5000,  # Smaller batches
        memory_limit=512
    )
    
    if success:
        print("\nProcessing completed successfully!")
    else:
        print("\nProcessing failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
