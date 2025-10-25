#!/usr/bin/env python3
"""
USPTO Controller System Setup Script
Quick setup script to initialize the USPTO controller system.
"""

import os
import sys
import json
from pathlib import Path

def create_directories():
    """Create necessary directories"""
    directories = [
        "uspto_data/zips",
        "uspto_data/extracted", 
        "uspto_data/processed",
        "uspto_data/checkpoints",
        "uspto_data/batches"
    ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        print(f"✅ Created directory: {directory}")

def create_config_file():
    """Create default configuration file if it doesn't exist"""
    config_file = "uspto_config.json"
    
    if not Path(config_file).exists():
        # Load the comprehensive configuration
        try:
            with open("uspto_config.json", 'r') as f:
                default_config = json.load(f)
        except FileNotFoundError:
            # Fallback to basic configuration if comprehensive config doesn't exist
            default_config = {
                "project": {
                    "name": "USPTO Controller-Based Data Processing System",
                    "version": "1.0.0",
                    "description": "Modular system for downloading, processing, and storing USPTO trademark data"
                },
                "api": {
                    "base_url": "https://data.uspto.gov",
                    "search_endpoint": "/ui/datasets/products/search",
                    "full_url": "https://data.uspto.gov/ui/datasets/products/search?facets=true&latest=true&labels=Trademark",
                    "timeout": 30,
                    "retry_attempts": 3,
                    "headers": {
                        "User-Agent": "USPTO-Data-Processor/1.0.0",
                        "Accept": "application/json"
                    }
                },
                "download": {
                    "base_dir": "./uspto_data",
                    "force_redownload": False,
                    "verify_downloads": True,
                    "max_concurrent_downloads": 3
                },
                "processing": {
                    "batch_size": 10000,
                    "chunk_size": 50000,
                    "memory_limit_mb": 512,
                    "max_workers": 2
                },
                "database": {
                    "dbname": "trademarks",
                    "user": "postgres",
                    "password": "1234",
                    "host": "localhost",
                    "port": "5432",
                    "use_copy": True
                },
                "orchestrator": {
                    "max_files_per_product": 2,
                    "enable_parallel_processing": False,
                    "log_level": "INFO"
                },
                "logging": {
                    "level": "INFO",
                    "file_logging": True,
                    "log_file": "./logs/uspto_processor.log",
                    "console_logging": True
                }
            }
        
        with open(config_file, 'w') as f:
            json.dump(default_config, f, indent=2)
        
        print(f"✅ Created configuration file: {config_file}")
    else:
        print(f"ℹ️ Configuration file already exists: {config_file}")

def check_dependencies():
    """Check if required dependencies are installed"""
    required_packages = [
        'requests',
        'pandas', 
        'psycopg2',
        'lxml'
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
            print(f"✅ {package} is installed")
        except ImportError:
            missing_packages.append(package)
            print(f"❌ {package} is missing")
    
    if missing_packages:
        print(f"\n⚠️ Missing packages: {', '.join(missing_packages)}")
        print("Install them with: pip install -r requirements.txt")
        return False
    
    return True

def main():
    """Main setup function"""
    print("🚀 USPTO Controller System Setup")
    print("=" * 40)
    
    # Create directories
    print("\n📁 Creating directories...")
    create_directories()
    
    # Create config file
    print("\n⚙️ Setting up configuration...")
    create_config_file()
    
    # Check dependencies
    print("\n📦 Checking dependencies...")
    deps_ok = check_dependencies()
    
    print("\n" + "=" * 40)
    if deps_ok:
        print("✅ Setup completed successfully!")
        print("\nNext steps:")
        print("1. Configure database settings in uspto_config.json")
        print("2. Run: python run_uspto.py --test")
        print("3. Run: python run_uspto.py")
    else:
        print("⚠️ Setup completed with warnings!")
        print("Please install missing dependencies and run setup again.")

if __name__ == "__main__":
    main()
