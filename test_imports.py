#!/usr/bin/env python3
"""
Quick Import Test
Test all imports to identify missing dependencies
"""

def test_imports():
    """Test all required imports"""
    print("🧪 Testing imports...")
    
    try:
        import requests
        print("✅ requests")
    except ImportError as e:
        print(f"❌ requests: {e}")
    
    try:
        import pandas
        print("✅ pandas")
    except ImportError as e:
        print(f"❌ pandas: {e}")
    
    try:
        import psycopg2
        print("✅ psycopg2")
    except ImportError as e:
        print(f"❌ psycopg2: {e}")
    
    try:
        import lxml
        print("✅ lxml")
    except ImportError as e:
        print(f"❌ lxml: {e}")
    
    try:
        import zipfile
        print("✅ zipfile (built-in)")
    except ImportError as e:
        print(f"❌ zipfile: {e}")
    
    try:
        import xml.etree.ElementTree
        print("✅ xml.etree.ElementTree (built-in)")
    except ImportError as e:
        print(f"❌ xml.etree.ElementTree: {e}")
    
    try:
        import json
        print("✅ json (built-in)")
    except ImportError as e:
        print(f"❌ json: {e}")
    
    try:
        import hashlib
        print("✅ hashlib (built-in)")
    except ImportError as e:
        print(f"❌ hashlib: {e}")

if __name__ == "__main__":
    test_imports()
