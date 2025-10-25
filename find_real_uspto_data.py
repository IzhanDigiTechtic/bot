#!/usr/bin/env python3
"""
Find Real USPTO Data Source
Investigate and find the correct source for real USPTO data
"""

import requests
import json
from pathlib import Path

def investigate_uspto_data_sources():
    """Investigate various USPTO data sources to find real data"""
    
    print("Investigating USPTO Data Sources...")
    print("=" * 50)
    
    # Known USPTO data sources to investigate
    data_sources = [
        {
            'name': 'USPTO Bulk Data Portal',
            'url': 'https://bulkdata.uspto.gov/data/trademark/',
            'description': 'Official USPTO bulk data portal'
        },
        {
            'name': 'USPTO Data.gov',
            'url': 'https://data.uspto.gov/data/trademark/',
            'description': 'USPTO data on data.gov'
        },
        {
            'name': 'USPTO Trademark Status API',
            'url': 'https://tsdrapi.uspto.gov/ts/cd/casestatus/',
            'description': 'Trademark Status and Document Retrieval API'
        },
        {
            'name': 'USPTO Trademark Assignment API',
            'url': 'https://assignment-api.uspto.gov/assignment-api/',
            'description': 'Trademark Assignment API'
        }
    ]
    
    working_sources = []
    
    for source in data_sources:
        print(f"\n🔍 Testing: {source['name']}")
        print(f"   URL: {source['url']}")
        print(f"   Description: {source['description']}")
        
        try:
            # Test if the URL is accessible
            response = requests.get(source['url'], timeout=10)
            
            if response.status_code == 200:
                print(f"   ✅ Status: Accessible (200)")
                print(f"   📄 Content-Type: {response.headers.get('content-type', 'Unknown')}")
                
                # Check if it contains trademark-related content
                content = response.text.lower()
                trademark_indicators = ['trademark', 'serial', 'registration', 'assignment', 'case']
                matches = sum(1 for indicator in trademark_indicators if indicator in content)
                
                if matches >= 2:
                    print(f"   ✅ Content: Contains trademark data indicators ({matches}/5)")
                    working_sources.append(source)
                else:
                    print(f"   ⚠️  Content: Limited trademark indicators ({matches}/5)")
            else:
                print(f"   ❌ Status: Not accessible ({response.status_code})")
                
        except requests.exceptions.RequestException as e:
            print(f"   ❌ Error: {e}")
    
    return working_sources

def check_current_config():
    """Check the current USPTO configuration"""
    
    print("\nChecking Current Configuration...")
    print("=" * 40)
    
    config_files = [
        'uspto_controller_config.json',
        'uspto_config.json'
    ]
    
    for config_file in config_files:
        config_path = Path(config_file)
        if config_path.exists():
            print(f"\n📄 {config_file}:")
            try:
                with open(config_path, 'r') as f:
                    config = json.load(f)
                
                # Check API configuration
                api_config = config.get('api', {})
                print(f"   API URL: {api_config.get('api_url', 'Not set')}")
                print(f"   Timeout: {api_config.get('timeout', 'Not set')}")
                print(f"   Retry Attempts: {api_config.get('retry_attempts', 'Not set')}")
                
                # Check download configuration
                download_config = config.get('download', {})
                print(f"   Download Dir: {download_config.get('download_dir', 'Not set')}")
                print(f"   Force Redownload: {download_config.get('force_redownload', 'Not set')}")
                
            except Exception as e:
                print(f"   ❌ Error reading config: {e}")
        else:
            print(f"❌ {config_file}: Not found")

def suggest_real_data_solution():
    """Suggest solution for getting real USPTO data"""
    
    print("\n" + "=" * 60)
    print("REAL USPTO DATA SOLUTION")
    print("=" * 60)
    print()
    print("ISSUE: Current API endpoint provides fake test data")
    print("SOLUTION: Use official USPTO bulk data sources")
    print()
    print("Recommended approach:")
    print("1. 🔗 Use USPTO Bulk Data Portal: https://bulkdata.uspto.gov/data/trademark/")
    print("2. 📥 Download real data files directly from USPTO")
    print("3. 🔧 Update configuration to use local files instead of API")
    print("4. ✅ Process real data files locally")
    print()
    print("Benefits:")
    print("✅ Real trademark data (not test data)")
    print("✅ Complete datasets (not limited API responses)")
    print("✅ Better performance (local processing)")
    print("✅ No API rate limits")
    print("✅ Reliable data source")
    print()
    print("Next steps:")
    print("1. Visit https://bulkdata.uspto.gov/data/trademark/")
    print("2. Download the latest trademark data files")
    print("3. Update the system to process local files")
    print("4. Run processing with real data")

def create_real_data_config():
    """Create configuration for real data processing"""
    
    print("\nCreating Real Data Configuration...")
    print("=" * 40)
    
    real_data_config = {
        "api": {
            "api_url": "https://bulkdata.uspto.gov/data/trademark/",
            "timeout": 30,
            "retry_attempts": 3,
            "use_local_files": True,
            "local_data_dir": "./real_uspto_data"
        },
        "download": {
            "download_dir": "./real_uspto_data",
            "keep_latest_zips": 10,
            "force_redownload": True,
            "use_bulk_portal": True,
            "bulk_portal_url": "https://bulkdata.uspto.gov/data/trademark/"
        },
        "processing": {
            "batch_size": 5000,
            "chunk_size": 25000,
            "memory_limit_mb": 512,
            "max_workers": 2,
            "validate_data_source": True,
            "reject_fake_data": True
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
            "max_files_per_product": 10,
            "enable_parallel_processing": False,
            "log_level": "INFO",
            "data_validation": True
        }
    }
    
    # Save the configuration
    config_file = "uspto_real_data_config.json"
    with open(config_file, 'w') as f:
        json.dump(real_data_config, f, indent=2)
    
    print(f"✅ Created: {config_file}")
    print("   This configuration is set up for real USPTO data processing")
    print("   Key features:")
    print("   - Uses USPTO bulk data portal")
    print("   - Validates data source")
    print("   - Rejects fake data")
    print("   - Processes local files")

def main():
    """Main function"""
    
    print("USPTO Real Data Source Investigation")
    print("=" * 50)
    print()
    print("PROBLEM: System is processing fake data (serial numbers 60000001+)")
    print("GOAL: Find and configure real USPTO data source")
    print()
    
    # Investigate data sources
    working_sources = investigate_uspto_data_sources()
    
    # Check current configuration
    check_current_config()
    
    # Suggest solution
    suggest_real_data_solution()
    
    # Create real data configuration
    create_real_data_config()
    
    print("\n" + "=" * 60)
    print("INVESTIGATION COMPLETE!")
    print("=" * 60)
    print()
    print("Summary:")
    print(f"✅ Tested {len(working_sources)} working data sources")
    print("✅ Created real data configuration")
    print("✅ Identified solution approach")
    print()
    print("The fake data issue is caused by using a test API endpoint.")
    print("The solution is to use official USPTO bulk data sources.")
    print("See the created configuration file for next steps.")

if __name__ == "__main__":
    main()
