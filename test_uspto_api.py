#!/usr/bin/env python3
"""
Test USPTO API to understand what data is being returned
"""

import requests
import json
import sys

def test_uspto_api():
    """Test the USPTO API to see what data it returns"""
    
    print("Testing USPTO API...")
    print("=" * 50)
    
    # Test the API endpoint
    api_url = "https://data.uspto.gov/ui/datasets/products/search"
    params = {
        'facets': 'true',
        'latest': 'true', 
        'labels': 'Trademark'
    }
    
    try:
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json, text/plain, */*',
        })
        
        print(f"Fetching from: {api_url}")
        print(f"Params: {params}")
        
        response = session.get(api_url, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        print(f"Response status: {response.status_code}")
        print(f"Found {data.get('count', 0)} trademark datasets")
        
        # Look for TRCFECO2 specifically
        products = data.get('bulkDataProductBag', [])
        trcfeco2_found = False
        
        for product in products:
            product_id = product.get('productIdentifier')
            title = product.get('productTitleText')
            
            print(f"\nProduct: {product_id}")
            print(f"Title: {title}")
            
            if product_id == 'TRCFECO2':
                trcfeco2_found = True
                print("*** TRCFECO2 FOUND ***")
                
                # Get file information
                file_bag = product.get('productFileBag', {})
                files = file_bag.get('fileDataBag', [])
                
                print(f"Files available: {len(files)}")
                
                for i, file_data in enumerate(files):
                    filename = file_data.get('fileName')
                    size = file_data.get('fileSize')
                    download_url = file_data.get('fileDownloadURI')
                    file_type = file_data.get('fileTypeText')
                    
                    print(f"  File {i+1}:")
                    print(f"    Name: {filename}")
                    print(f"    Size: {size:,} bytes" if size else "    Size: Unknown")
                    print(f"    Type: {file_type}")
                    print(f"    URL: {download_url}")
                    
                    # Test download a small sample
                    if download_url and filename:
                        print(f"    Testing download...")
                        try:
                            # Download just the first few bytes to check content
                            test_response = session.get(download_url, stream=True, timeout=10)
                            test_response.raise_for_status()
                            
                            # Read first 1000 bytes
                            content_sample = b''
                            for chunk in test_response.iter_content(chunk_size=1000):
                                content_sample += chunk
                                if len(content_sample) >= 1000:
                                    break
                            
                            # Decode and check for fake serial numbers
                            try:
                                content_text = content_sample.decode('utf-8', errors='ignore')
                                if '60000001' in content_text:
                                    print("    ⚠️  WARNING: File contains fake serial numbers!")
                                else:
                                    print("    ✅ File appears to have real data")
                                
                                # Show first few lines
                                lines = content_text.split('\n')[:5]
                                print("    First few lines:")
                                for line in lines:
                                    if line.strip():
                                        print(f"      {line[:100]}...")
                                        
                            except Exception as e:
                                print(f"    Error reading content: {e}")
                                
                        except Exception as e:
                            print(f"    Error testing download: {e}")
        
        if not trcfeco2_found:
            print("\n❌ TRCFECO2 not found in API response!")
        
        # Check if this might be a test/demo API
        if data.get('count', 0) < 5:
            print("\n⚠️  WARNING: Very few products found. This might be a test API.")
        
        print(f"\nTotal products found: {len(products)}")
        
    except Exception as e:
        print(f"❌ Error testing API: {e}")
        return False
    
    return True

def check_alternative_sources():
    """Check for alternative USPTO data sources"""
    
    print("\nChecking alternative USPTO data sources...")
    print("=" * 50)
    
    # Common USPTO data endpoints
    alternative_urls = [
        "https://bulkdata.uspto.gov/data/trademark/casefile/",
        "https://data.uspto.gov/data/trademark/",
        "https://www.uspto.gov/learning-and-resources/electronic-data-products/trademark-case-file-dataset",
    ]
    
    for url in alternative_urls:
        print(f"\nChecking: {url}")
        try:
            response = requests.get(url, timeout=10)
            print(f"  Status: {response.status_code}")
            if response.status_code == 200:
                print("  ✅ Accessible")
            else:
                print("  ❌ Not accessible")
        except Exception as e:
            print(f"  ❌ Error: {e}")

if __name__ == "__main__":
    print("USPTO API Investigation")
    print("=" * 60)
    
    # Test the main API
    api_success = test_uspto_api()
    
    # Check alternative sources
    check_alternative_sources()
    
    print("\n" + "=" * 60)
    if api_success:
        print("✅ API test completed")
    else:
        print("❌ API test failed")
    
    print("\nNext steps:")
    print("1. If fake data is found, we need to find the correct USPTO data source")
    print("2. If no TRCFECO2 is found, we need to check the correct API endpoint")
    print("3. Consider using direct USPTO bulk data downloads instead of API")
