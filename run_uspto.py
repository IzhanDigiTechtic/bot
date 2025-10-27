#!/usr/bin/env python3
"""
USPTO Controller-Based Data Processor - Main Runner
Main entry point for the USPTO controller-based data processing system.
"""

import sys
import os
from pathlib import Path

# Add controllers directory to Python path
controllers_dir = Path(__file__).parent / "controllers"
sys.path.insert(0, str(controllers_dir))
# in run_uspto.py before creating USPTOOrchestrator, or pass in however you build config
# config['skip_products'] = ['TRCFECO2']
# or to run only TRTYRAG:
# config['only_products'] = ['TRTYRAG']
# Import from controllers package
from core.uspto_controller_runner import main

if __name__ == "__main__":
    main()
