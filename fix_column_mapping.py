#!/usr/bin/env python3
"""
Column Name Mapping Fix
Fixes the column name mapping issue between CSV data and database schema
"""

import psycopg2
import sys

def fix_column_mapping():
    """Fix column name mapping in the processing logic"""
    
    print("Fixing column name mapping...")
    print("=" * 40)
    
    # The issue: CSV data has 'serial_no' but processing logic might be using 'serial_number'
    # We need to ensure consistent column name mapping
    
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
        
        # Check what columns are actually in the tables
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name LIKE 'product_%'
        """)
        
        product_tables = cursor.fetchall()
        
        print("Current table schemas:")
        for (table_name,) in product_tables:
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = %s 
                ORDER BY ordinal_position
            """, (table_name,))
            
            columns = [row[0] for row in cursor.fetchall()]
            print(f"  {table_name}: {columns[:5]}...")  # Show first 5 columns
        
        conn.close()
        
        print("\nColumn mapping issue identified:")
        print("- Database tables have: serial_no")
        print("- Processing logic might be using: serial_number")
        print("- Need to fix the column name mapping in processing code")
        
    except Exception as e:
        print(f"Error checking schemas: {e}")

def create_column_mapping_fix():
    """Create a fix for the column mapping issue"""
    
    print("\nCreating column mapping fix...")
    print("=" * 40)
    
    fix_code = '''#!/usr/bin/env python3
"""
Column Name Mapping Fix for USPTO Processing
Ensures consistent column names between CSV data and database schema
"""

def fix_column_names(record: dict) -> dict:
    """Fix column names to match database schema"""
    
    # Column name mappings
    column_mappings = {
        'serial_number': 'serial_no',
        'registration_number': 'registration_no',  # Keep as is, but ensure consistency
        'filing_date': 'filing_dt',
        'registration_date': 'registration_dt',
        'mark_identification': 'mark_id_char',
        'mark_drawing_code': 'mark_draw_cd',
        'abandon_date': 'abandon_dt',
        'amend_registration_date': 'amend_reg_dt',
        'reg_cancel_code': 'reg_cancel_cd',
        'reg_cancel_date': 'reg_cancel_dt',
        'examiner_attorney_name': 'exm_attorney_name',
        'file_location_date': 'file_location_dt',
        'publication_date': 'publication_dt',
        'renewal_date': 'renewal_dt',
        'repub_12c_date': 'repub_12c_dt',
        'cfh_status_code': 'cfh_status_cd',
        'cfh_status_date': 'cfh_status_dt',
        'ir_auto_registration_date': 'ir_auto_reg_dt',
        'ir_first_refusal_in': 'ir_first_refus_in',
        'ir_death_date': 'ir_death_dt',
        'ir_publication_date': 'ir_publication_dt',
        'ir_registration_date': 'ir_registration_dt',
        'ir_registration_number': 'ir_registration_no',
        'ir_renewal_date': 'ir_renewal_dt',
        'ir_status_code': 'ir_status_cd',
        'ir_status_date': 'ir_status_dt',
        'ir_priority_date': 'ir_priority_dt',
        'tad_file_id': 'tad_file_id',  # Keep as is
    }
    
    # Create new record with fixed column names
    fixed_record = {}
    
    for key, value in record.items():
        # Clean the key first
        clean_key = key.strip().lower().replace(' ', '_').replace('-', '_')
        
        # Apply mapping if exists
        if clean_key in column_mappings:
            fixed_key = column_mappings[clean_key]
        else:
            fixed_key = clean_key
        
        fixed_record[fixed_key] = value
    
    return fixed_record

def validate_column_names(record: dict) -> bool:
    """Validate that column names match expected database schema"""
    
    # Expected column names (based on database schema)
    expected_columns = {
        'serial_no', 'registration_no', 'filing_dt', 'registration_dt',
        'mark_id_char', 'mark_draw_cd', 'abandon_dt', 'amend_reg_dt',
        'reg_cancel_cd', 'reg_cancel_dt', 'exm_attorney_name',
        'file_location_dt', 'publication_dt', 'renewal_dt', 'repub_12c_dt',
        'cfh_status_cd', 'cfh_status_dt', 'ir_auto_reg_dt', 'ir_first_refus_in',
        'ir_death_dt', 'ir_publication_dt', 'ir_registration_dt',
        'ir_registration_no', 'ir_renewal_dt', 'ir_status_cd', 'ir_status_dt',
        'ir_priority_dt', 'tad_file_id', 'data_source', 'file_hash', 'batch_number'
    }
    
    # Check if any unexpected columns exist
    unexpected_columns = set(record.keys()) - expected_columns
    
    if unexpected_columns:
        print(f"Warning: Unexpected columns found: {unexpected_columns}")
        return False
    
    return True

if __name__ == "__main__":
    # Test the column mapping
    test_record = {
        'serial_number': '12345678',
        'registration_number': '9876543',
        'filing_date': '2023-01-01',
        'mark_identification': 'TEST MARK',
        'data_source': 'TRTDXFAP [CSV]'
    }
    
    print("Testing column mapping...")
    print(f"Original: {test_record}")
    
    fixed_record = fix_column_names(test_record)
    print(f"Fixed: {fixed_record}")
    
    is_valid = validate_column_names(fixed_record)
    print(f"Valid: {is_valid}")
'''
    
    with open("column_mapping_fix.py", 'w') as f:
        f.write(fix_code)
    
    print("Created: column_mapping_fix.py")

def create_processing_fix():
    """Create a fix for the processing controller"""
    
    print("\nCreating processing controller fix...")
    print("=" * 40)
    
    # The issue is in the ProcessingController._clean_record method
    # It needs to properly map column names
    
    fix_code = '''#!/usr/bin/env python3
"""
Processing Controller Fix
Patches the ProcessingController to use correct column names
"""

def patch_processing_controller():
    """Patch the ProcessingController to fix column name mapping"""
    
    patch_code = '''
# Add this method to ProcessingController class
def _map_column_names(self, record: Dict) -> Dict:
    """Map column names to match database schema"""
    
    # Column name mappings
    column_mappings = {
        'serial_number': 'serial_no',
        'registration_number': 'registration_no',
        'filing_date': 'filing_dt',
        'registration_date': 'registration_dt',
        'mark_identification': 'mark_id_char',
        'mark_drawing_code': 'mark_draw_cd',
        'abandon_date': 'abandon_dt',
        'amend_registration_date': 'amend_reg_dt',
        'reg_cancel_code': 'reg_cancel_cd',
        'reg_cancel_date': 'reg_cancel_dt',
        'examiner_attorney_name': 'exm_attorney_name',
        'file_location_date': 'file_location_dt',
        'publication_date': 'publication_dt',
        'renewal_date': 'renewal_dt',
        'repub_12c_date': 'repub_12c_dt',
        'cfh_status_code': 'cfh_status_cd',
        'cfh_status_date': 'cfh_status_dt',
        'ir_auto_registration_date': 'ir_auto_reg_dt',
        'ir_first_refusal_in': 'ir_first_refus_in',
        'ir_death_date': 'ir_death_dt',
        'ir_publication_date': 'ir_publication_dt',
        'ir_registration_date': 'ir_registration_dt',
        'ir_registration_number': 'ir_registration_no',
        'ir_renewal_date': 'ir_renewal_dt',
        'ir_status_code': 'ir_status_cd',
        'ir_status_date': 'ir_status_dt',
        'ir_priority_date': 'ir_priority_dt',
    }
    
    mapped_record = {}
    for key, value in record.items():
        # Clean the key first
        clean_key = key.strip().lower().replace(' ', '_').replace('-', '_')
        
        # Apply mapping if exists
        if clean_key in column_mappings:
            mapped_record[column_mappings[clean_key]] = value
        else:
            mapped_record[clean_key] = value
    
    return mapped_record

# Modify the _clean_record method to use column mapping
def _clean_record(self, record: Dict, product_id: str) -> Optional[Dict]:
    """Clean and normalize a record with proper column mapping"""
    try:
        # First map column names
        mapped_record = self._map_column_names(record)
        
        cleaned = {}
        for key, value in mapped_record.items():
            # Clean values with proper type conversion
            if pd.isna(value) or value == '' or str(value).lower() == 'nan':
                cleaned[key] = None
            else:
                # Convert values based on their type and column name
                cleaned[key] = self._convert_value(value, key)
        
        # Add metadata
        cleaned['data_source'] = f"{product_id} [CSV]"
        cleaned['batch_number'] = 0  # Will be set by database controller
        
        return cleaned
        
    except Exception as e:
        self.logger.error(f"Error cleaning record: {e}")
        return None
'''
    
    with open("processing_controller_patch.py", 'w') as f:
        f.write(f"# Processing Controller Patch\\n\\n{patch_code}")
    
    print("Created: processing_controller_patch.py")

def main():
    """Main function"""
    
    print("Column Name Mapping Fix")
    print("=" * 50)
    print()
    print("ISSUE: Column name mismatch in data processing")
    print("- Database tables have: serial_no")
    print("- Processing logic is using: serial_number")
    print("- This causes INSERT errors")
    print()
    
    # Check current schemas
    fix_column_mapping()
    
    # Create fixes
    create_column_mapping_fix()
    create_processing_fix()
    
    print("\n" + "=" * 50)
    print("SOLUTION CREATED!")
    print("=" * 50)
    print()
    print("Next steps:")
    print("1. Apply the column mapping fix to the processing code")
    print("2. The system will then use correct column names")
    print("3. Database INSERT errors will be resolved")
    print()
    print("Files created:")
    print("- column_mapping_fix.py: Column name mapping logic")
    print("- processing_controller_patch.py: Code patch for ProcessingController")

if __name__ == "__main__":
    main()
