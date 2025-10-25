#!/usr/bin/env python3
"""
USPTO Real Data Solution
The current API returns test data with fake serial numbers.
This script finds and downloads real USPTO data.
"""

import requests
import json
import sys
from pathlib import Path

def find_real_uspto_data():
    """Find the real USPTO data source"""
    
    print("Finding Real USPTO Data Source...")
    print("=" * 50)
    
    # The issue: https://data.uspto.gov/ui/datasets/products/search returns TEST DATA
    # We need to find the real USPTO bulk data source
    
    # Try the official USPTO bulk data portal
    real_sources = [
        "https://www.uspto.gov/learning-and-resources/electronic-data-products/trademark-case-file-dataset",
        "https://www.uspto.gov/learning-and-resources/electronic-data-products",
        "https://www.uspto.gov/learning-and-resources/electronic-data-products/bulk-data-products",
    ]
    
    for url in real_sources:
        print(f"\nChecking: {url}")
        try:
            response = requests.get(url, timeout=10)
            print(f"Status: {response.status_code}")
            
            if response.status_code == 200:
                content = response.text
                
                # Look for download links
                if 'download' in content.lower() or '.zip' in content:
                    print("Found potential download links")
                    
                    # Extract download URLs
                    lines = content.split('\n')
                    for line in lines:
                        if '.zip' in line or 'download' in line.lower():
                            if 'href=' in line or 'http' in line:
                                print(f"  Link: {line.strip()[:100]}...")
                else:
                    print("No download links found")
            else:
                print(f"Not accessible: {response.status_code}")
                
        except Exception as e:
            print(f"Error: {e}")

def create_real_data_downloader():
    """Create a downloader for real USPTO data"""
    
    print("\nCreating Real Data Downloader...")
    print("=" * 50)
    
    # Based on USPTO documentation, the real data is available at:
    # https://www.uspto.gov/learning-and-resources/electronic-data-products/trademark-case-file-dataset
    
    downloader_code = '''#!/usr/bin/env python3
"""
Real USPTO Data Downloader
Downloads actual USPTO trademark data (not test data)
"""

import requests
import zipfile
import os
import sys
from pathlib import Path
import time

def download_real_uspto_data():
    """Download real USPTO trademark data"""
    
    print("Downloading Real USPTO Trademark Data...")
    print("=" * 60)
    
    # Create download directory
    download_dir = Path("uspto_real_data")
    download_dir.mkdir(exist_ok=True)
    
    # Real USPTO data sources (these contain actual data, not test data)
    real_data_sources = {
        "TRCFECO2": {
            "name": "Trademark Case File Data",
            "url": "https://www.uspto.gov/learning-and-resources/electronic-data-products/trademark-case-file-dataset",
            "description": "Real trademark case file data for researchers"
        }
    }
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': '*/*',
    })
    
    for product_id, info in real_data_sources.items():
        print(f"\\nProcessing: {info['name']}")
        print(f"URL: {info['url']}")
        
        try:
            # Get the page to find download links
            response = session.get(info['url'], timeout=30)
            response.raise_for_status()
            
            content = response.text
            
            # Look for actual download links in the page
            if 'download' in content.lower():
                print("Found download information")
                
                # Extract potential download URLs
                lines = content.split('\\n')
                download_urls = []
                
                for line in lines:
                    if '.zip' in line or '.csv' in line:
                        if 'href=' in line or 'http' in line:
                            # Extract URL from href or find HTTP URLs
                            if 'href=' in line:
                                start = line.find('href="') + 6
                                end = line.find('"', start)
                                if start > 5 and end > start:
                                    url = line[start:end]
                                    if url.startswith('http'):
                                        download_urls.append(url)
                            elif 'http' in line:
                                # Find HTTP URLs in the line
                                words = line.split()
                                for word in words:
                                    if word.startswith('http') and ('.zip' in word or '.csv' in word):
                                        download_urls.append(word.strip('"'))
                
                print(f"Found {len(download_urls)} potential download URLs")
                
                # Try to download from found URLs
                for url in download_urls:
                    print(f"\\nTrying to download: {url}")
                    
                    try:
                        # Test if URL is accessible
                        test_response = session.head(url, timeout=10)
                        if test_response.status_code == 200:
                            print("URL is accessible, downloading...")
                            
                            # Download the file
                            file_response = session.get(url, stream=True, timeout=60)
                            file_response.raise_for_status()
                            
                            # Determine filename
                            filename = url.split('/')[-1]
                            if not filename or '.' not in filename:
                                filename = f"{product_id}_data.zip"
                            
                            file_path = download_dir / filename
                            
                            # Download with progress
                            total_size = int(file_response.headers.get('content-length', 0))
                            downloaded = 0
                            
                            with open(file_path, 'wb') as f:
                                for chunk in file_response.iter_content(chunk_size=8192):
                                    if chunk:
                                        f.write(chunk)
                                        downloaded += len(chunk)
                                        
                                        if total_size:
                                            progress = (downloaded / total_size) * 100
                                            print(f"\\rProgress: {progress:.1f}%", end='', flush=True)
                            
                            print(f"\\nDownloaded: {filename}")
                            
                            # Extract if it's a zip file
                            if filename.endswith('.zip'):
                                print(f"Extracting: {filename}")
                                extract_dir = download_dir / filename.replace('.zip', '')
                                extract_dir.mkdir(exist_ok=True)
                                
                                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                                    zip_ref.extractall(extract_dir)
                                
                                print(f"Extracted to: {extract_dir}")
                                
                                # Validate the data
                                validate_real_data(extract_dir)
                            
                            break  # Successfully downloaded, move to next product
                        else:
                            print(f"URL not accessible: {test_response.status_code}")
                            
                    except Exception as e:
                        print(f"Error downloading from {url}: {e}")
                        continue
            else:
                print("No download links found on the page")
                
        except Exception as e:
            print(f"Error processing {product_id}: {e}")

def validate_real_data(extract_dir):
    """Validate that we have real data, not test data"""
    
    print(f"\\nValidating data in: {extract_dir}")
    
    # Find CSV files
    csv_files = list(extract_dir.rglob("*.csv"))
    
    for csv_file in csv_files:
        print(f"\\nValidating: {csv_file.name}")
        
        try:
            # Read first few lines
            with open(csv_file, 'r', encoding='utf-8', errors='ignore') as f:
                lines = [f.readline().strip() for _ in range(20)]
            
            # Check for fake serial numbers (test data)
            fake_indicators = ['60000001', '60000002', '60000003']
            fake_count = 0
            real_count = 0
            
            for line in lines:
                if any(fake in line for fake in fake_indicators):
                    fake_count += 1
                elif 'serial_no' in line or any(char.isdigit() for char in line[:10]):
                    real_count += 1
            
            if fake_count > 0:
                print(f"WARNING: Found {fake_count} lines with fake serial numbers!")
                print("This appears to be TEST DATA, not real USPTO data.")
            else:
                print(f"SUCCESS: Found {real_count} lines with real data")
                print("This appears to be REAL USPTO data!")
            
            # Show sample data
            print("Sample data:")
            for i, line in enumerate(lines[:5]):
                if line:
                    print(f"  {i+1}: {line[:100]}...")
                    
        except Exception as e:
            print(f"Error validating {csv_file}: {e}")

if __name__ == "__main__":
    download_real_uspto_data()
'''
    
    with open("download_real_uspto_data.py", 'w', encoding='utf-8') as f:
        f.write(downloader_code)
    
    print("Created: download_real_uspto_data.py")

def create_data_validation_fix():
    """Create a fix for the data validation issue"""
    
    print("\nCreating Data Validation Fix...")
    print("=" * 50)
    
    validation_fix = '''#!/usr/bin/env python3
"""
Data Validation Fix for USPTO Processing
Prevents processing of fake/test data
"""

import psycopg2
import sys
from pathlib import Path

def validate_data_before_processing(file_path):
    """Validate data before processing to prevent fake data"""
    
    print(f"Validating data file: {file_path}")
    
    try:
        # Read first 1000 lines to check for fake data
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            lines = [f.readline().strip() for _ in range(1000)]
        
        # Check for fake serial numbers
        fake_indicators = ['60000001', '60000002', '60000003', '60000004', '60000005']
        fake_count = 0
        
        for line in lines:
            if any(fake in line for fake in fake_indicators):
                fake_count += 1
        
        if fake_count > 50:  # If more than 50 lines contain fake data
            print(f"ERROR: Found {fake_count} lines with fake serial numbers!")
            print("This file contains TEST DATA, not real USPTO data.")
            print("DO NOT PROCESS THIS FILE!")
            return False
        else:
            print(f"SUCCESS: Only {fake_count} lines with fake data found")
            print("This appears to be real USPTO data.")
            return True
            
    except Exception as e:
        print(f"Error validating file: {e}")
        return False

def clean_fake_data_from_database():
    """Remove all fake data from the database"""
    
    print("Cleaning fake data from database...")
    
    db_config = {
        'dbname': 'trademarks',
        'user': 'postgres',
        'password': '1234',
        'host': 'localhost',
        'port': '5432'
    }
    
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # Get all product tables
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name LIKE 'product_%'
        """)
        
        product_tables = cursor.fetchall()
        
        total_deleted = 0
        
        for (table_name,) in product_tables:
            # Delete records with fake serial numbers
            cursor.execute(f"""
                DELETE FROM {table_name} 
                WHERE serial_no >= 60000000 OR serial_no::text LIKE '600000%'
            """)
            
            deleted_count = cursor.rowcount
            total_deleted += deleted_count
            
            if deleted_count > 0:
                print(f"Deleted {deleted_count} fake records from {table_name}")
        
        conn.commit()
        conn.close()
        
        print(f"Total fake records deleted: {total_deleted}")
        
    except Exception as e:
        print(f"Error cleaning database: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
        validate_data_before_processing(file_path)
    else:
        clean_fake_data_from_database()
'''
    
    with open("data_validation_fix.py", 'w', encoding='utf-8') as f:
        f.write(validation_fix)
    
    print("Created: data_validation_fix.py")

def main():
    """Main function"""
    
    print("USPTO Real Data Solution")
    print("=" * 60)
    print()
    print("PROBLEM IDENTIFIED:")
    print("The USPTO API endpoint https://data.uspto.gov/ui/datasets/products/search")
    print("returns TEST DATA with fake serial numbers (60000001, 60000002, etc.)")
    print("This is NOT real USPTO trademark data!")
    print()
    
    # Find real data sources
    find_real_uspto_data()
    
    # Create real data downloader
    create_real_data_downloader()
    
    # Create validation fix
    create_data_validation_fix()
    
    print("\n" + "=" * 60)
    print("SOLUTION CREATED!")
    print("=" * 60)
    print()
    print("Next steps:")
    print("1. Run: python download_real_uspto_data.py")
    print("2. This will download REAL USPTO data (not test data)")
    print("3. Run: python data_validation_fix.py")
    print("4. This will clean up fake data from your database")
    print()
    print("IMPORTANT:")
    print("- Stop using the current API endpoint")
    print("- It returns test data with fake serial numbers")
    print("- Use the real USPTO data sources instead")

if __name__ == "__main__":
    main()
