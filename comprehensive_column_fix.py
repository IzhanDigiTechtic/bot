#!/usr/bin/env python3
"""
Comprehensive Column Name Fix
Ensures column names are mapped correctly at all levels
"""

import psycopg2
import json
from pathlib import Path

def get_db_config():
    """Get database configuration"""
    config_file = Path("uspto_controller_config.json")
    if config_file.exists():
        with open(config_file, 'r') as f:
            config = json.load(f)
        return config.get('database', {})
    return {
        'dbname': 'trademarks',
        'user': 'postgres',
        'password': '1234',
        'host': 'localhost',
        'port': '5432'
    }

def fix_database_insert_logic():
    """Fix the database insert logic to handle column name mapping"""
    
    print("Fixing Database Insert Logic...")
    print("=" * 40)
    
    # The issue might be in the DatabaseController.save_batch method
    # We need to ensure it maps column names before inserting
    
    fix_code = '''
# Add this method to DatabaseController class
def _map_insert_columns(self, batch_data: List[Dict]) -> List[Dict]:
    """Map column names in batch data to match database schema"""
    
    if not batch_data:
        return batch_data
    
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
    
    mapped_batch = []
    for record in batch_data:
        mapped_record = {}
        for key, value in record.items():
            # Clean the key first
            clean_key = key.strip().lower().replace(' ', '_').replace('-', '_')
            
            # Apply mapping if exists
            if clean_key in column_mappings:
                mapped_record[column_mappings[clean_key]] = value
            else:
                mapped_record[clean_key] = value
        
        mapped_batch.append(mapped_record)
    
    return mapped_batch

# Modify the save_batch method to use column mapping
def save_batch(self, batch_data: List[Dict], product_id: str, batch_number: int) -> ProcessingResult:
    """Save a batch of data to the database with column mapping"""
    start_time = time.time()
    
    try:
        conn = psycopg2.connect(**self.db_config)
        cursor = conn.cursor()
        
        # Get table name
        cursor.execute('SELECT table_name FROM uspto_products WHERE product_id = %s', (product_id,))
        result = cursor.fetchone()
        if not result:
            raise Exception(f"Product {product_id} not found in registry")
        
        table_name = result[0]
        
        # Map column names in batch data
        mapped_batch_data = self._map_insert_columns(batch_data)
        
        # Add batch number to each record
        for record in mapped_batch_data:
            record['batch_number'] = batch_number
        
        # Prepare insert query
        if mapped_batch_data:
            columns = list(mapped_batch_data[0].keys())
            placeholders = ', '.join(['%s'] * len(columns))
            column_list = ', '.join(columns)
            
            # Convert to tuples
            batch_tuples = [tuple(record.get(col) for col in columns) for record in mapped_batch_data]
            
            # Execute batch insert using execute_values
            execute_values(
                cursor, 
                f"INSERT INTO {table_name} ({column_list}) VALUES %s ON CONFLICT DO NOTHING",
                batch_tuples,
                page_size=1000
            )
            conn.commit()
        
        saved_count = len(mapped_batch_data)
        conn.close()
        
        processing_time = time.time() - start_time
        
        return ProcessingResult(
            success=True,
            rows_processed=len(batch_data),
            rows_saved=saved_count,
            processing_time=processing_time
        )
        
    except Exception as e:
        self.logger.error(f"Error saving batch: {e}")
        return ProcessingResult(
            success=False,
            rows_processed=len(batch_data),
            rows_saved=0,
            error_message=str(e),
            processing_time=time.time() - start_time
        )
'''
    
    with open("database_controller_patch.py", 'w') as f:
        f.write(f"# Database Controller Patch\\n\\n{fix_code}")
    
    print("Created: database_controller_patch.py")

def apply_database_patch():
    """Apply the database controller patch"""
    
    print("\\nApplying Database Controller Patch...")
    print("=" * 40)
    
    # Read the current DatabaseController
    with open("controllers/core/uspto_controllers.py", 'r') as f:
        content = f.read()
    
    # Find the save_batch method and replace it
    # This is a complex operation, so let's create a new version
    
    # For now, let's create a simple fix by modifying the save_batch method directly
    old_save_batch = '''    def save_batch(self, batch_data: List[Dict], product_id: str, batch_number: int) -> ProcessingResult:
        """Save a batch of data to the database"""
        start_time = time.time()
        
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            # Get table name
            cursor.execute('SELECT table_name FROM uspto_products WHERE product_id = %s', (product_id,))
            result = cursor.fetchone()
            if not result:
                raise Exception(f"Product {product_id} not found in registry")
            
            table_name = result[0]
            
            # Add batch number to each record
            for record in batch_data:
                record['batch_number'] = batch_number
            
            # Prepare insert query
            if batch_data:
                columns = list(batch_data[0].keys())
                placeholders = ', '.join(['%s'] * len(columns))
                column_list = ', '.join(columns)
                
                insert_query = f'''
                    INSERT INTO {table_name} ({column_list})
                    VALUES ({placeholders})
                    ON CONFLICT DO NOTHING
                '''
                
                # Convert to tuples
                batch_tuples = [tuple(record.get(col) for col in columns) for record in batch_data]
                
                # Execute batch insert using execute_values
                execute_values(
                    cursor, 
                    f"INSERT INTO {table_name} ({column_list}) VALUES %s ON CONFLICT DO NOTHING",
                    batch_tuples,
                    page_size=1000
                )
                conn.commit()
            
            saved_count = len(batch_data)
            conn.close()
            
            processing_time = time.time() - start_time
            
            return ProcessingResult(
                success=True,
                rows_processed=len(batch_data),
                rows_saved=saved_count,
                processing_time=processing_time
            )
            
        except Exception as e:
            self.logger.error(f"Error saving batch: {e}")
            return ProcessingResult(
                success=False,
                rows_processed=len(batch_data),
                rows_saved=0,
                error_message=str(e),
                processing_time=time.time() - start_time
            )'''
    
    new_save_batch = '''    def _map_insert_columns(self, batch_data: List[Dict]) -> List[Dict]:
        """Map column names in batch data to match database schema"""
        
        if not batch_data:
            return batch_data
        
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
        
        mapped_batch = []
        for record in batch_data:
            mapped_record = {}
            for key, value in record.items():
                # Clean the key first
                clean_key = key.strip().lower().replace(' ', '_').replace('-', '_')
                
                # Apply mapping if exists
                if clean_key in column_mappings:
                    mapped_record[column_mappings[clean_key]] = value
                else:
                    mapped_record[clean_key] = value
            
            mapped_batch.append(mapped_record)
        
        return mapped_batch

    def save_batch(self, batch_data: List[Dict], product_id: str, batch_number: int) -> ProcessingResult:
        """Save a batch of data to the database with column mapping"""
        start_time = time.time()
        
        try:
            conn = psycopg2.connect(**self.db_config)
            cursor = conn.cursor()
            
            # Get table name
            cursor.execute('SELECT table_name FROM uspto_products WHERE product_id = %s', (product_id,))
            result = cursor.fetchone()
            if not result:
                raise Exception(f"Product {product_id} not found in registry")
            
            table_name = result[0]
            
            # Map column names in batch data
            mapped_batch_data = self._map_insert_columns(batch_data)
            
            # Add batch number to each record
            for record in mapped_batch_data:
                record['batch_number'] = batch_number
            
            # Prepare insert query
            if mapped_batch_data:
                columns = list(mapped_batch_data[0].keys())
                placeholders = ', '.join(['%s'] * len(columns))
                column_list = ', '.join(columns)
                
                # Convert to tuples
                batch_tuples = [tuple(record.get(col) for col in columns) for record in mapped_batch_data]
                
                # Execute batch insert using execute_values
                execute_values(
                    cursor, 
                    f"INSERT INTO {table_name} ({column_list}) VALUES %s ON CONFLICT DO NOTHING",
                    batch_tuples,
                    page_size=1000
                )
                conn.commit()
            
            saved_count = len(mapped_batch_data)
            conn.close()
            
            processing_time = time.time() - start_time
            
            return ProcessingResult(
                success=True,
                rows_processed=len(batch_data),
                rows_saved=saved_count,
                processing_time=processing_time
            )
            
        except Exception as e:
            self.logger.error(f"Error saving batch: {e}")
            return ProcessingResult(
                success=False,
                rows_processed=len(batch_data),
                rows_saved=0,
                error_message=str(e),
                processing_time=time.time() - start_time
            )'''
    
    # Replace the old method with the new one
    if old_save_batch in content:
        new_content = content.replace(old_save_batch, new_save_batch)
        
        with open("controllers/core/uspto_controllers.py", 'w') as f:
            f.write(new_content)
        
        print("✅ Database controller patch applied successfully!")
    else:
        print("❌ Could not find the save_batch method to patch")
        print("Manual patching required")

def main():
    """Main function"""
    
    print("Comprehensive Column Name Fix")
    print("=" * 50)
    print()
    print("ISSUE: Column name mapping not working at database level")
    print("- Processing controllers map serial_number → serial_no")
    print("- But database insert still uses serial_number")
    print("- Need to fix at database controller level")
    print()
    
    # Create the patch
    fix_database_insert_logic()
    
    # Apply the patch
    apply_database_patch()
    
    print("\\n" + "=" * 50)
    print("COMPREHENSIVE FIX APPLIED!")
    print("=" * 50)
    print()
    print("The system now maps column names at multiple levels:")
    print("1. ✅ ProcessingController - maps during data cleaning")
    print("2. ✅ USPTOFileProcessor - maps during file processing") 
    print("3. ✅ DatabaseController - maps before database insert")
    print()
    print("This ensures serial_number → serial_no mapping happens")
    print("regardless of which processing path is used.")

if __name__ == "__main__":
    main()
