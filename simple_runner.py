#!/usr/bin/env python3
"""
Simple USPTO Runner
A simplified runner to test basic functionality without complex imports
"""

import sys
import os
import json
from pathlib import Path

def load_config():
    """Load configuration file"""
    config_file = "uspto_config.json"
    
    if not os.path.exists(config_file):
        print(f"❌ Configuration file not found: {config_file}")
        print("💡 Run 'python setup.py' to create it")
        return None
    
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
        print(f"✅ Configuration loaded from {config_file}")
        return config
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON in config file: {e}")
        return None
    except Exception as e:
        print(f"❌ Error loading config: {e}")
        return None

def test_database_connection(config):
    """Test database connection"""
    if not config or 'database' not in config:
        print("❌ No database configuration found")
        return False
    
    db_config = config['database']
    
    try:
        import psycopg2
        
        conn = psycopg2.connect(
            dbname=db_config.get('dbname', 'trademarks'),
            user=db_config.get('user', 'postgres'),
            password=db_config.get('password', ''),
            host=db_config.get('host', 'localhost'),
            port=db_config.get('port', '5432')
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        print("✅ Database connection successful")
        return True
        
    except ImportError:
        print("❌ psycopg2 not installed")
        print("💡 Run: pip install psycopg2-binary")
        return False
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        print("💡 Check your database configuration in uspto_config.json")
        return False

def test_api_connection(config):
    """Test API connection"""
    if not config or 'api' not in config:
        print("❌ No API configuration found")
        return False
    
    api_config = config['api']
    
    try:
        import requests
        
        url = api_config.get('full_url', api_config.get('api_url', ''))
        if not url:
            print("❌ No API URL configured")
            return False
        
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            print("✅ USPTO API connection successful")
            return True
        else:
            print(f"❌ USPTO API returned status {response.status_code}")
            return False
            
    except ImportError:
        print("❌ requests not installed")
        print("💡 Run: pip install requests")
        return False
    except Exception as e:
        print(f"❌ API connection failed: {e}")
        return False

def create_directories():
    """Create necessary directories"""
    directories = [
        "uspto_data",
        "uspto_data/zips",
        "uspto_data/extracted",
        "uspto_data/processed",
        "uspto_data/checkpoints",
        "uspto_data/batches",
        "logs"
    ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
    
    print("✅ Directories created")

def main():
    """Main function"""
    print("🚀 USPTO Simple Runner")
    print("=" * 40)
    
    # Load configuration
    config = load_config()
    if not config:
        return 1
    
    # Create directories
    create_directories()
    
    # Test database connection
    db_ok = test_database_connection(config)
    
    # Test API connection
    api_ok = test_api_connection(config)
    
    # Summary
    print("\n📊 Test Results:")
    print(f"   Configuration: {'✅' if config else '❌'}")
    print(f"   Database: {'✅' if db_ok else '❌'}")
    print(f"   API: {'✅' if api_ok else '❌'}")
    
    if config and db_ok and api_ok:
        print("\n🎉 All tests passed! System is ready.")
        print("\n💡 Next steps:")
        print("   1. Run: python setup.py (if not done)")
        print("   2. Run: python run_uspto.py --test")
        print("   3. Run: python run_uspto.py --max-files 1")
        return 0
    else:
        print("\n⚠️ Some tests failed. Please fix the issues above.")
        return 1

if __name__ == "__main__":
    exit(main())
