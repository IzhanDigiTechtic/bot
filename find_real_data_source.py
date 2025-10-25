#!/usr/bin/env python3
"""
Find the correct USPTO bulk data source
"""

import requests
import json
import sys
from pathlib import Path

def check_uspto_bulk_data():
    """Check the USPTO bulk data directory for real data"""
    
    print("Checking USPTO Bulk Data Sources...")
    print("=" * 50)
    
    # Try different USPTO bulk data URLs
    bulk_urls = [
        "https://data.uspto.gov/data/trademark/",
        "https://data.uspto.gov/data/trademark/casefile/",
        "https://data.uspto.gov/data/trademark/assignment/",
    ]
    
    for url in bulk_urls:
        print(f"\nChecking: {url}")
        try:
            response = requests.get(url, timeout=10)
            print(f"Status: {response.status_code}")
            
            if response.status_code == 200:
                content = response.text
                
                # Look for downloadable files
                if '.zip' in content or '.csv' in content:
                    print("✅ Found downloadable files")
                    
                    # Look for TRCFECO2 or case file references
                    if 'TRCFECO2' in content or 'case' in content.lower():
                        print("✅ Found TRCFECO2/case file references")
                        
                        # Extract potential download links
                        lines = content.split('\n')
                        for line in lines:
                            if '.zip' in line or '.csv' in line:
                                print(f"  Potential file: {line.strip()[:100]}...")
                else:
                    print("❌ No downloadable files found")
            else:
                print(f"❌ Not accessible: {response.status_code}")
                
        except Exception as e:
            print(f"❌ Error: {e}")

def check_direct_download():
    """Try to find direct download links"""
    
    print("\nChecking for Direct Download Links...")
    print("=" * 50)
    
    # Common USPTO file patterns
    file_patterns = [
        "https://data.uspto.gov/data/trademark/casefile/case_file.csv.zip",
        "https://data.uspto.gov/data/trademark/casefile/TRCFECO2/case_file.csv.zip",
        "https://bulkdata.uspto.gov/data/trademark/casefile/case_file.csv.zip",
    ]
    
    for url in file_patterns:
        print(f"\nTesting: {url}")
        try:
            # Use HEAD request to check if file exists
            response = requests.head(url, timeout=10)
            print(f"Status: {response.status_code}")
            
            if response.status_code == 200:
                print("✅ File exists!")
                content_length = response.headers.get('content-length')
                if content_length:
                    size_mb = int(content_length) / (1024 * 1024)
                    print(f"Size: {size_mb:.1f} MB")
                    
                    # Test a small download to check content
                    print("Testing content...")
                    test_response = requests.get(url, stream=True, timeout=10)
                    test_response.raise_for_status()
                    
                    # Read first 1000 bytes
                    content_sample = b''
                    for chunk in test_response.iter_content(chunk_size=1000):
                        content_sample += chunk
                        if len(content_sample) >= 1000:
                            break
                    
                    try:
                        content_text = content_sample.decode('utf-8', errors='ignore')
                        if '60000001' in content_text:
                            print("⚠️  Contains fake serial numbers!")
                        elif 'serial_no' in content_text or 'serial_number' in content_text:
                            print("✅ Contains real serial number data")
                        else:
                            print("❓ Unknown content type")
                            
                        # Show first few lines
                        lines = content_text.split('\n')[:3]
                        print("Sample content:")
                        for line in lines:
                            if line.strip():
                                print(f"  {line[:80]}...")
                                
                    except Exception as e:
                        print(f"Error reading content: {e}")
                        
            else:
                print(f"❌ File not found: {response.status_code}")
                
        except Exception as e:
            print(f"❌ Error: {e}")

def create_corrected_downloader():
    """Create a corrected downloader that uses the right data source"""
    
    downloader_script = '''#!/usr/bin/env python3
"""
Corrected USPTO Data Downloader
Downloads real data from the correct USPTO source
"""

import requests
import zipfile
import os
import sys
from pathlib import Path

def download_real_uspto_data():
    """Download real USPTO data from the correct source"""
    
    print("Downloading Real USPTO Data...")
    print("=" * 50)
    
    # Use the correct USPTO bulk data URL
    base_url = "https://data.uspto.gov/data/trademark/casefile/"
    
    # Create download directory
    download_dir = Path("uspto_data_real")
    download_dir.mkdir(exist_ok=True)
    
    # List of files to download
    files_to_download = [
        "case_file.csv.zip",
        # Add other files as needed
    ]
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': '*/*',
    })
    
    for filename in files_to_download:
        url = base_url + filename
        file_path = download_dir / filename
        
        print(f"\\nDownloading: {filename}")
        print(f"URL: {url}")
        
        try:
            response = session.get(url, stream=True, timeout=30)
            response.raise_for_status()
            
            # Get file size
            total_size = int(response.headers.get('content-length', 0))
            if total_size:
                print(f"Size: {total_size / (1024*1024):.1f} MB")
            
            # Download with progress
            downloaded = 0
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        
                        if total_size:
                            progress = (downloaded / total_size) * 100
                            print(f"\\rProgress: {progress:.1f}%", end='', flush=True)
            
            print(f"\\n✅ Downloaded: {filename}")
            
            # Extract if it's a zip file
            if filename.endswith('.zip'):
                print(f"Extracting: {filename}")
                extract_dir = download_dir / filename.replace('.zip', '')
                extract_dir.mkdir(exist_ok=True)
                
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    zip_ref.extractall(extract_dir)
                
                print(f"✅ Extracted to: {extract_dir}")
                
                # Validate the extracted data
                validate_extracted_data(extract_dir)
                
        except Exception as e:
            print(f"❌ Error downloading {filename}: {e}")

def validate_extracted_data(extract_dir):
    """Validate the extracted data for real serial numbers"""
    
    print(f"\\nValidating data in: {extract_dir}")
    
    # Find CSV files
    csv_files = list(extract_dir.rglob("*.csv"))
    
    for csv_file in csv_files:
        print(f"\\nValidating: {csv_file.name}")
        
        try:
            # Read first few lines
            with open(csv_file, 'r', encoding='utf-8', errors='ignore') as f:
                lines = [f.readline().strip() for _ in range(10)]
            
            # Check for fake serial numbers
            fake_count = 0
            real_count = 0
            
            for line in lines:
                if '60000001' in line or '60000002' in line:
                    fake_count += 1
                elif 'serial_no' in line or any(char.isdigit() for char in line[:10]):
                    real_count += 1
            
            if fake_count > 0:
                print(f"⚠️  Found {fake_count} lines with fake serial numbers")
            if real_count > 0:
                print(f"✅ Found {real_count} lines with real data")
            
            # Show sample
            print("Sample lines:")
            for i, line in enumerate(lines[:3]):
                if line:
                    print(f"  {i+1}: {line[:100]}...")
                    
        except Exception as e:
            print(f"Error validating {csv_file}: {e}")

if __name__ == "__main__":
    download_real_uspto_data()
'''
    
    with open("download_real_data.py", 'w') as f:
        f.write(downloader_script)
    
    print("✅ Created corrected downloader script")

if __name__ == "__main__":
    print("USPTO Data Source Investigation")
    print("=" * 60)
    
    # Check bulk data sources
    check_uspto_bulk_data()
    
    # Check direct downloads
    check_direct_download()
    
    # Create corrected downloader
    create_corrected_downloader()
    
    print("\n" + "=" * 60)
    print("Investigation completed!")
    print("\nNext steps:")
    print("1. Run: python download_real_data.py")
    print("2. This will download from the correct USPTO source")
    print("3. Validate the data before processing")
