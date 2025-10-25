#!/usr/bin/env python3
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
        print(f"\nProcessing: {info['name']}")
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
                lines = content.split('\n')
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
                    print(f"\nTrying to download: {url}")
                    
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
                                            print(f"\rProgress: {progress:.1f}%", end='', flush=True)
                            
                            print(f"\nDownloaded: {filename}")
                            
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
    
    print(f"\nValidating data in: {extract_dir}")
    
    # Find CSV files
    csv_files = list(extract_dir.rglob("*.csv"))
    
    for csv_file in csv_files:
        print(f"\nValidating: {csv_file.name}")
        
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
