#!/usr/bin/env python3
"""
Check first 10 fields of TRCFECO2 CSV file
Analyze the structure and content of the CSV file
"""

import pandas as pd
import csv
from pathlib import Path

def check_trcfeco2_csv_fields():
    """Check the first 10 fields of TRCFECO2 CSV file"""
    
    print("Checking TRCFECO2 CSV File Fields...")
    print("=" * 50)
    
    csv_file = Path("uspto_data/extracted/TRCFECO2/case_file.csv/case_file.csv")
    
    if not csv_file.exists():
        print(f"❌ CSV file not found: {csv_file}")
        return
    
    print(f"📄 File: {csv_file}")
    print(f"📊 File size: {csv_file.stat().st_size / (1024*1024):.1f}MB")
    print()
    
    try:
        # Method 1: Read just the header and first few rows
        print("Method 1: Reading header and first 5 rows...")
        print("-" * 40)
        
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)
            
            print(f"Total columns: {len(header)}")
            print(f"First 10 columns:")
            for i, col in enumerate(header[:10]):
                print(f"  {i+1:2d}. {col}")
            
            print(f"\nFirst 5 data rows (first 10 fields each):")
            for row_num, row in enumerate(reader):
                if row_num >= 5:
                    break
                
                print(f"\nRow {row_num + 1}:")
                for i, value in enumerate(row[:10]):
                    col_name = header[i] if i < len(header) else f"Col_{i+1}"
                    # Truncate long values
                    display_value = str(value)[:50] + "..." if len(str(value)) > 50 else str(value)
                    print(f"  {col_name}: {display_value}")
        
        print("\n" + "=" * 50)
        
        # Method 2: Use pandas to get more detailed info
        print("Method 2: Pandas analysis of first 10 fields...")
        print("-" * 40)
        
        # Read just first 1000 rows for analysis
        df_sample = pd.read_csv(csv_file, nrows=1000, low_memory=False)
        
        print(f"Sample size: {len(df_sample)} rows")
        print(f"Total columns: {len(df_sample.columns)}")
        
        # Analyze first 10 columns
        first_10_cols = df_sample.columns[:10]
        
        print(f"\nFirst 10 columns analysis:")
        for i, col in enumerate(first_10_cols):
            print(f"\n{i+1:2d}. {col}")
            
            # Get basic stats
            non_null_count = df_sample[col].notna().sum()
            null_count = df_sample[col].isna().sum()
            unique_count = df_sample[col].nunique()
            
            print(f"    Non-null values: {non_null_count}/{len(df_sample)} ({non_null_count/len(df_sample)*100:.1f}%)")
            print(f"    Null values: {null_count}/{len(df_sample)} ({null_count/len(df_sample)*100:.1f}%)")
            print(f"    Unique values: {unique_count}")
            
            # Show sample values
            sample_values = df_sample[col].dropna().head(3).tolist()
            print(f"    Sample values: {sample_values}")
            
            # Check for fake serial numbers in serial_no column
            if col == 'serial_no':
                fake_count = 0
                for val in df_sample[col].dropna():
                    try:
                        if int(val) >= 60000001:
                            fake_count += 1
                    except (ValueError, TypeError):
                        pass
                
                if fake_count > 0:
                    print(f"    ⚠️  FAKE DATA: {fake_count} fake serial numbers (60000001+)")
                else:
                    print(f"    ✅ Real data: No fake serial numbers detected")
        
        print("\n" + "=" * 50)
        
        # Method 3: Check for patterns in serial numbers
        print("Method 3: Serial number pattern analysis...")
        print("-" * 40)
        
        if 'serial_no' in df_sample.columns:
            serial_nos = df_sample['serial_no'].dropna()
            
            print(f"Serial number analysis (from {len(serial_nos)} records):")
            
            # Convert to numeric where possible
            numeric_serials = []
            for val in serial_nos:
                try:
                    numeric_serials.append(int(val))
                except (ValueError, TypeError):
                    pass
            
            if numeric_serials:
                numeric_serials.sort()
                
                print(f"  Min serial: {min(numeric_serials):,}")
                print(f"  Max serial: {max(numeric_serials):,}")
                print(f"  Range: {max(numeric_serials) - min(numeric_serials):,}")
                
                # Check for fake patterns
                fake_patterns = {
                    '60000001+': sum(1 for s in numeric_serials if s >= 60000001),
                    '70000000+': sum(1 for s in numeric_serials if s >= 70000000),
                    '80000000+': sum(1 for s in numeric_serials if s >= 80000000),
                }
                
                print(f"\n  Fake pattern detection:")
                for pattern, count in fake_patterns.items():
                    if count > 0:
                        print(f"    ❌ {pattern}: {count} records ({count/len(numeric_serials)*100:.1f}%)")
                    else:
                        print(f"    ✅ {pattern}: 0 records")
                
                # Show first and last few serial numbers
                print(f"\n  First 5 serial numbers: {numeric_serials[:5]}")
                print(f"  Last 5 serial numbers: {numeric_serials[-5:]}")
        
    except Exception as e:
        print(f"❌ Error analyzing CSV file: {e}")
        import traceback
        traceback.print_exc()

def main():
    """Main function"""
    
    print("TRCFECO2 CSV Field Analysis")
    print("=" * 50)
    print()
    print("This script will analyze the first 10 fields of the TRCFECO2 CSV file")
    print("to understand its structure and detect any fake data patterns.")
    print()
    
    check_trcfeco2_csv_fields()
    
    print("\n" + "=" * 50)
    print("ANALYSIS COMPLETE!")
    print("=" * 50)
    print()
    print("This analysis helps understand:")
    print("✅ CSV file structure and column names")
    print("✅ Data types and null value patterns")
    print("✅ Sample values for each field")
    print("✅ Detection of fake serial numbers")
    print("✅ Overall data quality assessment")

if __name__ == "__main__":
    main()
