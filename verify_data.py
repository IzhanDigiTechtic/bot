#!/usr/bin/env python3
"""
Verify that we're inserting the right data into the database
"""

import psycopg2
import pandas as pd
import os

def verify_database_data():
    """Verify the data in the database matches the source"""
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
        
        print("=== DATABASE VERIFICATION ===")
        
        # Check how many records we have
        cursor.execute("SELECT COUNT(*) FROM trademark_case_files")
        db_count = cursor.fetchone()[0]
        print(f"Records in database: {db_count}")
        
        # Get sample records from database
        cursor.execute("""
            SELECT serial_number, registration_number, filing_date, registration_date, 
                   status_code, status_date, mark_identification, mark_drawing_code,
                   cfh_status_cd, cfh_status_dt, data_source
            FROM trademark_case_files 
            ORDER BY serial_number 
            LIMIT 5
        """)
        
        db_records = cursor.fetchall()
        print(f"\nSample records from database:")
        for i, record in enumerate(db_records, 1):
            print(f"  Record {i}:")
            print(f"    Serial Number: {record[0]}")
            print(f"    Registration Number: {record[1]}")
            print(f"    Filing Date: {record[2]}")
            print(f"    Registration Date: {record[3]}")
            print(f"    Status Code: {record[4]}")
            print(f"    Status Date: {record[5]}")
            print(f"    Mark Identification: {record[6]}")
            print(f"    Mark Drawing Code: {record[7]}")
            print(f"    CFH Status Code: {record[8]}")
            print(f"    CFH Status Date: {record[9]}")
            print(f"    Data Source: {record[10]}")
            print()
        
        conn.close()
        
    except Exception as e:
        print(f"Error querying database: {e}")

def verify_source_data():
    """Verify the source CSV data"""
    csv_path = "uspto_data/extracted/TRCFECO2/case_file.csv/case_file.csv"
    
    if not os.path.exists(csv_path):
        print(f"Source CSV file not found: {csv_path}")
        return
    
    print("=== SOURCE DATA VERIFICATION ===")
    
    try:
        # Read the same sample of data that was processed
        df = pd.read_csv(csv_path, nrows=5)
        print(f"Source CSV has {len(df)} sample rows")
        print(f"Total columns: {len(df.columns)}")
        
        print(f"\nSample records from source CSV:")
        for i, (index, row) in enumerate(df.iterrows(), 1):
            print(f"  Record {i}:")
            print(f"    serial_no: {row.get('serial_no')}")
            print(f"    registration_no: {row.get('registration_no')}")
            print(f"    filing_dt: {row.get('filing_dt')}")
            print(f"    registration_dt: {row.get('registration_dt')}")
            print(f"    cfh_status_cd: {row.get('cfh_status_cd')}")
            print(f"    cfh_status_dt: {row.get('cfh_status_dt')}")
            print(f"    mark_id_char: {row.get('mark_id_char')}")
            print(f"    mark_draw_cd: {row.get('mark_draw_cd')}")
            print()
        
        # Check data types and null values
        print("Column analysis:")
        key_columns = ['serial_no', 'registration_no', 'filing_dt', 'registration_dt', 
                      'cfh_status_cd', 'cfh_status_dt', 'mark_id_char', 'mark_draw_cd']
        
        for col in key_columns:
            if col in df.columns:
                null_count = df[col].isnull().sum()
                dtype = df[col].dtype
                print(f"  {col}: {dtype}, {null_count}/{len(df)} nulls")
        
    except Exception as e:
        print(f"Error reading source CSV: {e}")

def compare_data():
    """Compare database data with source data"""
    print("\n=== DATA COMPARISON ===")
    
    # Get database data
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
        
        # Get first record from database
        cursor.execute("""
            SELECT serial_number, registration_number, filing_date, registration_date, 
                   status_code, status_date, mark_identification, mark_drawing_code,
                   cfh_status_cd, cfh_status_dt
            FROM trademark_case_files 
            ORDER BY serial_number 
            LIMIT 1
        """)
        
        db_record = cursor.fetchone()
        conn.close()
        
        # Get first record from source
        csv_path = "uspto_data/extracted/TRCFECO2/case_file.csv/case_file.csv"
        df = pd.read_csv(csv_path, nrows=1)
        
        print("Comparing first record:")
        print(f"Database serial_number: {db_record[0]}")
        print(f"Source serial_no: {df.iloc[0]['serial_no']}")
        print(f"Match: {str(db_record[0]) == str(df.iloc[0]['serial_no'])}")
        
        print(f"\nDatabase registration_number: {db_record[1]}")
        print(f"Source registration_no: {df.iloc[0]['registration_no']}")
        print(f"Match: {str(db_record[1]) == str(df.iloc[0]['registration_no'])}")
        
        print(f"\nDatabase cfh_status_cd: {db_record[8]}")
        print(f"Source cfh_status_cd: {df.iloc[0]['cfh_status_cd']}")
        print(f"Match: {str(db_record[8]) == str(df.iloc[0]['cfh_status_cd'])}")
        
        print(f"\nDatabase cfh_status_dt: {db_record[9]}")
        print(f"Source cfh_status_dt: {df.iloc[0]['cfh_status_dt']}")
        print(f"Match: {str(db_record[9]) == str(df.iloc[0]['cfh_status_dt'])}")
        
    except Exception as e:
        print(f"Error comparing data: {e}")

def check_data_quality():
    """Check data quality and completeness"""
    print("\n=== DATA QUALITY CHECK ===")
    
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
        
        # Check for null values in key fields
        cursor.execute("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(serial_number) as non_null_serial,
                COUNT(registration_number) as non_null_registration,
                COUNT(filing_date) as non_null_filing_date,
                COUNT(registration_date) as non_null_registration_date,
                COUNT(status_code) as non_null_status_code,
                COUNT(mark_identification) as non_null_mark_id
            FROM trademark_case_files
        """)
        
        stats = cursor.fetchone()
        print(f"Data completeness:")
        print(f"  Total records: {stats[0]}")
        print(f"  Non-null serial_number: {stats[1]} ({stats[1]/stats[0]*100:.1f}%)")
        print(f"  Non-null registration_number: {stats[2]} ({stats[2]/stats[0]*100:.1f}%)")
        print(f"  Non-null filing_date: {stats[3]} ({stats[3]/stats[0]*100:.1f}%)")
        print(f"  Non-null registration_date: {stats[4]} ({stats[4]/stats[0]*100:.1f}%)")
        print(f"  Non-null status_code: {stats[5]} ({stats[5]/stats[0]*100:.1f}%)")
        print(f"  Non-null mark_identification: {stats[6]} ({stats[6]/stats[0]*100:.1f}%)")
        
        # Check for duplicate serial numbers
        cursor.execute("""
            SELECT serial_number, COUNT(*) 
            FROM trademark_case_files 
            GROUP BY serial_number 
            HAVING COUNT(*) > 1
        """)
        
        duplicates = cursor.fetchall()
        if duplicates:
            print(f"\nDuplicate serial numbers found: {len(duplicates)}")
            for dup in duplicates[:5]:  # Show first 5
                print(f"  Serial {dup[0]}: {dup[1]} occurrences")
        else:
            print(f"\nNo duplicate serial numbers found ✓")
        
        conn.close()
        
    except Exception as e:
        print(f"Error checking data quality: {e}")

def main():
    verify_source_data()
    verify_database_data()
    compare_data()
    check_data_quality()

if __name__ == "__main__":
    main()
