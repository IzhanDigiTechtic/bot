#!/usr/bin/env python3
"""
PostgreSQL Setup Guide for USPTO Project
Helps you set up PostgreSQL database correctly
"""

import os
import subprocess
import sys

def check_postgresql_installed():
    """Check if PostgreSQL is installed"""
    print("🔍 Checking PostgreSQL installation...")
    
    try:
        result = subprocess.run(['psql', '--version'], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ PostgreSQL installed: {result.stdout.strip()}")
            return True
        else:
            print("❌ PostgreSQL not found in PATH")
            return False
    except FileNotFoundError:
        print("❌ PostgreSQL not installed")
        return False

def test_postgresql_connection():
    """Test PostgreSQL connection with different configurations"""
    print("\n🔌 Testing PostgreSQL connections...")
    
    # Test configurations
    configs = [
        {"user": "postgres", "password": "", "description": "No password"},
        {"user": "postgres", "password": "1234", "description": "Password: 1234"},
        {"user": "postgres", "password": "postgres", "description": "Password: postgres"},
        {"user": "postgres", "password": "admin", "description": "Password: admin"},
        {"user": "postgres", "password": "password", "description": "Password: password"},
    ]
    
    for config in configs:
        print(f"\n🧪 Testing: {config['description']}")
        
        # Test connection
        cmd = [
            'psql',
            '-h', 'localhost',
            '-p', '5432',
            '-U', config['user'],
            '-d', 'postgres',
            '-c', 'SELECT 1;'
        ]
        
        env = os.environ.copy()
        if config['password']:
            env['PGPASSWORD'] = config['password']
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, env=env, timeout=5)
            if result.returncode == 0:
                print(f"   ✅ Connection successful!")
                return config
            else:
                print(f"   ❌ Connection failed: {result.stderr.strip()}")
        except subprocess.TimeoutExpired:
            print(f"   ❌ Connection timeout")
        except Exception as e:
            print(f"   ❌ Error: {e}")
    
    return None

def create_database():
    """Create the trademarks database"""
    print("\n🗄️ Creating trademarks database...")
    
    # Try to create database
    cmd = ['createdb', '-U', 'postgres', 'trademarks']
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ Database 'trademarks' created successfully")
            return True
        else:
            print(f"❌ Failed to create database: {result.stderr.strip()}")
            return False
    except Exception as e:
        print(f"❌ Error creating database: {e}")
        return False

def setup_postgresql_password():
    """Guide user to set up PostgreSQL password"""
    print("\n🔐 PostgreSQL Password Setup")
    print("=" * 40)
    
    print("To set up PostgreSQL password, you have several options:")
    print("\n1️⃣ Option 1: Set password for postgres user")
    print("   Run these commands:")
    print("   psql -U postgres")
    print("   ALTER USER postgres PASSWORD '1234';")
    print("   \\q")
    
    print("\n2️⃣ Option 2: Use pgAdmin (if installed)")
    print("   - Open pgAdmin")
    print("   - Right-click on 'Login/Group Roles'")
    print("   - Select 'postgres' user")
    print("   - Go to 'Definition' tab")
    print("   - Set password to '1234'")
    
    print("\n3️⃣ Option 3: Edit pg_hba.conf (Advanced)")
    print("   - Find pg_hba.conf file")
    print("   - Change 'md5' to 'trust' for local connections")
    print("   - Restart PostgreSQL service")
    
    print("\n4️⃣ Option 4: Use different user")
    print("   - Create new user with password")
    print("   - Update uspto_config.json with new credentials")

def update_config_file(password):
    """Update configuration file with working password"""
    print(f"\n⚙️ Updating configuration file...")
    
    config_file = "uspto_config.json"
    
    try:
        import json
        
        with open(config_file, 'r') as f:
            config = json.load(f)
        
        config['database']['password'] = password
        
        with open(config_file, 'w') as f:
            json.dump(config, f, indent=2)
        
        print(f"✅ Updated {config_file} with password: {password}")
        return True
        
    except Exception as e:
        print(f"❌ Error updating config: {e}")
        return False

def main():
    """Main function"""
    print("🐘 PostgreSQL Setup Guide for USPTO Project")
    print("=" * 50)
    
    # Check if PostgreSQL is installed
    if not check_postgresql_installed():
        print("\n📥 PostgreSQL Installation Required")
        print("=" * 30)
        print("Please install PostgreSQL:")
        print("1. Download from: https://www.postgresql.org/download/")
        print("2. Install with default settings")
        print("3. Remember the password you set during installation")
        print("4. Run this script again")
        return 1
    
    # Test connections
    working_config = test_postgresql_connection()
    
    if working_config:
        print(f"\n🎉 Found working configuration!")
        print(f"   User: {working_config['user']}")
        print(f"   Password: {working_config['password']}")
        
        # Update config file
        if update_config_file(working_config['password']):
            print("\n✅ Configuration updated successfully!")
            
            # Try to create database
            if create_database():
                print("\n🎉 PostgreSQL setup complete!")
                print("You can now run: python run_uspto.py --max-files 1")
                return 0
            else:
                print("\n⚠️ Database creation failed, but connection works")
                print("You may need to create the database manually")
                return 1
        else:
            print("\n❌ Failed to update configuration")
            return 1
    else:
        print("\n❌ No working PostgreSQL connection found")
        setup_postgresql_password()
        return 1

if __name__ == "__main__":
    exit(main())
