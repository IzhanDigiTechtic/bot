-- USPTO Multi-Product Database Schema
-- Updated schema for the new multi-processor system with product-specific tables

-- Drop existing tables if they exist (for clean setup)
DROP TABLE IF EXISTS batch_processing CASCADE;
DROP TABLE IF EXISTS file_processing_history CASCADE;
DROP TABLE IF EXISTS uspto_products CASCADE;

-- Drop all existing product tables (they will be recreated dynamically)
DO $$ 
DECLARE 
    r RECORD;
BEGIN
    FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE 'product_%') 
    LOOP
        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE';
    END LOOP;
END $$;

-- Product registry table - tracks all USPTO products
CREATE TABLE uspto_products (
    id SERIAL PRIMARY KEY,
    product_id VARCHAR(50) UNIQUE NOT NULL,
    title TEXT,
    description TEXT,
    frequency VARCHAR(20),
    from_date DATE,
    to_date DATE,
    total_size BIGINT,
    file_count INTEGER,
    last_modified TIMESTAMP,
    formats TEXT[],
    table_name VARCHAR(50) UNIQUE,
    schema_created BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- File processing history - tracks download and processing status
CREATE TABLE file_processing_history (
    id SERIAL PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL,
    file_name VARCHAR(255) NOT NULL,
    file_url TEXT,
    file_size BIGINT,
    file_hash VARCHAR(64),
    download_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processing_started TIMESTAMP,
    processing_completed TIMESTAMP,
    rows_processed INTEGER DEFAULT 0,
    rows_saved INTEGER DEFAULT 0,
    status VARCHAR(20) DEFAULT 'pending',
    error_message TEXT,
    processing_attempts INTEGER DEFAULT 0,
    batch_count INTEGER DEFAULT 0,
    last_batch_processed INTEGER DEFAULT 0,
    UNIQUE(product_id, file_name),
    FOREIGN KEY (product_id) REFERENCES uspto_products(product_id) ON DELETE CASCADE
);

-- Batch processing tracking - tracks individual batch processing
CREATE TABLE batch_processing (
    id SERIAL PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL,
    file_name VARCHAR(255) NOT NULL,
    batch_number INTEGER NOT NULL,
    batch_file_path TEXT,
    rows_in_batch INTEGER,
    processed BOOLEAN DEFAULT FALSE,
    saved_to_db BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP,
    UNIQUE(product_id, file_name, batch_number),
    FOREIGN KEY (product_id) REFERENCES uspto_products(product_id) ON DELETE CASCADE
);

-- Create indexes for better performance
CREATE INDEX idx_products_id ON uspto_products(product_id);
CREATE INDEX idx_products_table ON uspto_products(table_name);
CREATE INDEX idx_file_history_product ON file_processing_history(product_id);
CREATE INDEX idx_file_history_status ON file_processing_history(status);
CREATE INDEX idx_file_history_file ON file_processing_history(file_name);
CREATE INDEX idx_batch_product ON batch_processing(product_id, file_name);
CREATE INDEX idx_batch_status ON batch_processing(processed, saved_to_db);

-- Create a function to automatically create product tables
CREATE OR REPLACE FUNCTION create_product_table(
    p_product_id VARCHAR(50),
    p_table_name VARCHAR(50),
    p_product_type VARCHAR(20) DEFAULT 'generic'
) RETURNS BOOLEAN AS $$
DECLARE
    table_exists BOOLEAN;
BEGIN
    -- Check if table already exists
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = p_table_name
    ) INTO table_exists;
    
    IF table_exists THEN
        RETURN TRUE;
    END IF;
    
    -- Create table based on product type
    IF p_product_type = 'case_file' OR p_product_id LIKE 'TRCFECO%' THEN
        -- Case file data table
        EXECUTE format('
            CREATE TABLE %I (
                id SERIAL PRIMARY KEY,
                serial_number VARCHAR(20) UNIQUE,
                registration_number VARCHAR(20),
                filing_date DATE,
                registration_date DATE,
                status_code VARCHAR(10),
                status_date DATE,
                mark_identification TEXT,
                mark_drawing_code VARCHAR(10),
                abandon_dt DATE,
                amend_reg_dt DATE,
                amend_lb_for_app_in BOOLEAN DEFAULT FALSE,
                amend_lb_for_reg_in BOOLEAN DEFAULT FALSE,
                amend_lb_itu_in BOOLEAN DEFAULT FALSE,
                amend_lb_use_in BOOLEAN DEFAULT FALSE,
                reg_cancel_cd VARCHAR(10),
                reg_cancel_dt DATE,
                cancel_pend_in BOOLEAN DEFAULT FALSE,
                cert_mark_in BOOLEAN DEFAULT FALSE,
                chg_reg_in BOOLEAN DEFAULT FALSE,
                coll_memb_mark_in BOOLEAN DEFAULT FALSE,
                coll_serv_mark_in BOOLEAN DEFAULT FALSE,
                coll_trade_mark_in BOOLEAN DEFAULT FALSE,
                draw_color_cur_in BOOLEAN DEFAULT FALSE,
                draw_color_file_in BOOLEAN DEFAULT FALSE,
                concur_use_in BOOLEAN DEFAULT FALSE,
                concur_use_pend_in BOOLEAN DEFAULT FALSE,
                draw_3d_cur_in BOOLEAN DEFAULT FALSE,
                draw_3d_file_in BOOLEAN DEFAULT FALSE,
                exm_attorney_name TEXT,
                lb_use_file_in BOOLEAN DEFAULT FALSE,
                lb_for_app_cur_in BOOLEAN DEFAULT FALSE,
                lb_for_reg_cur_in BOOLEAN DEFAULT FALSE,
                lb_intl_reg_cur_in BOOLEAN DEFAULT FALSE,
                lb_for_app_file_in BOOLEAN DEFAULT FALSE,
                lb_for_reg_file_in BOOLEAN DEFAULT FALSE,
                lb_intl_reg_file_in BOOLEAN DEFAULT FALSE,
                lb_none_cur_in BOOLEAN DEFAULT FALSE,
                for_priority_in BOOLEAN DEFAULT FALSE,
                lb_itu_cur_in BOOLEAN DEFAULT FALSE,
                lb_itu_file_in BOOLEAN DEFAULT FALSE,
                interfer_pend_in BOOLEAN DEFAULT FALSE,
                exm_office_cd VARCHAR(10),
                opposit_pend_in BOOLEAN DEFAULT FALSE,
                amend_principal_in BOOLEAN DEFAULT FALSE,
                concur_use_pub_in BOOLEAN DEFAULT FALSE,
                publication_dt DATE,
                renewal_dt DATE,
                renewal_file_in BOOLEAN DEFAULT FALSE,
                repub_12c_dt DATE,
                repub_12c_in BOOLEAN DEFAULT FALSE,
                incontest_ack_in BOOLEAN DEFAULT FALSE,
                incontest_file_in BOOLEAN DEFAULT FALSE,
                acq_dist_in BOOLEAN DEFAULT FALSE,
                acq_dist_part_in BOOLEAN DEFAULT FALSE,
                use_afdv_acc_in BOOLEAN DEFAULT FALSE,
                use_afdv_file_in BOOLEAN DEFAULT FALSE,
                use_afdv_par_acc_in BOOLEAN DEFAULT FALSE,
                serv_mark_in BOOLEAN DEFAULT FALSE,
                std_char_claim_in BOOLEAN DEFAULT FALSE,
                cfh_status_cd INTEGER,
                cfh_status_dt DATE,
                amend_supp_reg_in BOOLEAN DEFAULT FALSE,
                supp_reg_in BOOLEAN DEFAULT FALSE,
                trade_mark_in BOOLEAN DEFAULT FALSE,
                lb_use_cur_in BOOLEAN DEFAULT FALSE,
                lb_none_file_in BOOLEAN DEFAULT FALSE,
                ir_auto_reg_dt DATE,
                ir_first_refus_in BOOLEAN DEFAULT FALSE,
                ir_death_dt DATE,
                ir_publication_dt DATE,
                ir_registration_dt DATE,
                ir_registration_no VARCHAR(20),
                ir_renewal_dt DATE,
                ir_status_cd VARCHAR(10),
                ir_status_dt DATE,
                ir_priority_dt DATE,
                ir_priority_in BOOLEAN DEFAULT FALSE,
                related_other_in BOOLEAN DEFAULT FALSE,
                tad_file_id INTEGER,
                data_source VARCHAR(100),
                file_hash VARCHAR(64),
                batch_number INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )', p_table_name);
            
        -- Create indexes for case file table
        EXECUTE format('CREATE INDEX idx_%I_serial ON %I(serial_number)', p_table_name, p_table_name);
        EXECUTE format('CREATE INDEX idx_%I_registration ON %I(registration_number)', p_table_name, p_table_name);
        EXECUTE format('CREATE INDEX idx_%I_filing_date ON %I(filing_date)', p_table_name, p_table_name);
        EXECUTE format('CREATE INDEX idx_%I_batch ON %I(batch_number)', p_table_name, p_table_name);
        
    ELSIF p_product_type = 'assignment' OR p_product_id LIKE 'TRASECO%' OR p_product_id LIKE 'TRTDXFAG%' OR p_product_id LIKE 'TRTYRAG%' THEN
        -- Assignment data table
        EXECUTE format('
            CREATE TABLE %I (
                id SERIAL PRIMARY KEY,
                assignment_id VARCHAR(50) UNIQUE,
                serial_number VARCHAR(20),
                registration_number VARCHAR(20),
                date_recorded DATE,
                conveyance_text TEXT,
                frame_no VARCHAR(10),
                reel_no VARCHAR(10),
                page_count INTEGER,
                last_update_date DATE,
                purge_indicator CHAR(1),
                correspondent_name TEXT,
                correspondent_address_1 TEXT,
                correspondent_address_2 TEXT,
                correspondent_address_3 TEXT,
                assignor_name TEXT,
                assignee_name TEXT,
                assignor_address TEXT,
                assignee_address TEXT,
                data_source VARCHAR(100),
                file_hash VARCHAR(64),
                batch_number INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )', p_table_name);
            
        -- Create indexes for assignment table
        EXECUTE format('CREATE INDEX idx_%I_assignment ON %I(assignment_id)', p_table_name, p_table_name);
        EXECUTE format('CREATE INDEX idx_%I_serial ON %I(serial_number)', p_table_name, p_table_name);
        EXECUTE format('CREATE INDEX idx_%I_date ON %I(date_recorded)', p_table_name, p_table_name);
        EXECUTE format('CREATE INDEX idx_%I_batch ON %I(batch_number)', p_table_name, p_table_name);
        
    ELSIF p_product_type = 'ttab' OR p_product_id LIKE 'TTAB%' OR p_product_type = 'trademark_application' OR p_product_id LIKE 'TRTDXFAP%' OR p_product_id LIKE 'TRTYRAP%' THEN
        -- TTAB proceedings table
        EXECUTE format('
            CREATE TABLE %I (
                id SERIAL PRIMARY KEY,
                proceeding_number VARCHAR(20) UNIQUE,
                proceeding_type VARCHAR(50),
                status VARCHAR(50),
                filing_date DATE,
                applicant_name TEXT,
                opposer_name TEXT,
                mark_description TEXT,
                goods_services TEXT,
                data_source VARCHAR(100),
                file_hash VARCHAR(64),
                batch_number INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )', p_table_name);
            
        -- Create indexes for TTAB table
        EXECUTE format('CREATE INDEX idx_%I_proceeding ON %I(proceeding_number)', p_table_name, p_table_name);
        EXECUTE format('CREATE INDEX idx_%I_filing_date ON %I(filing_date)', p_table_name, p_table_name);
        EXECUTE format('CREATE INDEX idx_%I_batch ON %I(batch_number)', p_table_name, p_table_name);
        
    ELSE
        -- Generic table for unknown product types
        EXECUTE format('
            CREATE TABLE %I (
                id SERIAL PRIMARY KEY,
                raw_data JSONB,
                data_source VARCHAR(100),
                file_hash VARCHAR(64),
                batch_number INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )', p_table_name);
            
        -- Create indexes for generic table
        EXECUTE format('CREATE INDEX idx_%I_batch ON %I(batch_number)', p_table_name, p_table_name);
        EXECUTE format('CREATE INDEX idx_%I_data_source ON %I(data_source)', p_table_name, p_table_name);
        EXECUTE format('CREATE INDEX idx_%I_raw_data ON %I USING GIN(raw_data)', p_table_name, p_table_name);
    END IF;
    
    -- Update product registry
    UPDATE uspto_products 
    SET schema_created = TRUE 
    WHERE product_id = p_product_id;
    
    RETURN TRUE;
    
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Error creating table %: %', p_table_name, SQLERRM;
    RETURN FALSE;
END;
$$ LANGUAGE plpgsql;

-- Create a function to get processing statistics
CREATE OR REPLACE FUNCTION get_processing_stats()
RETURNS TABLE (
    product_id VARCHAR(50),
    product_title TEXT,
    total_files INTEGER,
    completed_files INTEGER,
    processing_files INTEGER,
    error_files INTEGER,
    total_rows BIGINT,
    total_saved BIGINT,
    last_processed TIMESTAMP
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        p.product_id,
        p.title,
        COUNT(f.id)::INTEGER as total_files,
        COUNT(CASE WHEN f.status = 'completed' THEN 1 END)::INTEGER as completed_files,
        COUNT(CASE WHEN f.status = 'processing' THEN 1 END)::INTEGER as processing_files,
        COUNT(CASE WHEN f.status = 'error' THEN 1 END)::INTEGER as error_files,
        COALESCE(SUM(f.rows_processed), 0) as total_rows,
        COALESCE(SUM(f.rows_saved), 0) as total_saved,
        MAX(f.processing_completed) as last_processed
    FROM uspto_products p
    LEFT JOIN file_processing_history f ON p.product_id = f.product_id
    GROUP BY p.product_id, p.title
    ORDER BY p.product_id;
END;
$$ LANGUAGE plpgsql;

-- Create a function to clean up old batch files
CREATE OR REPLACE FUNCTION cleanup_old_batches(keep_days INTEGER DEFAULT 7)
RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER := 0;
    batch_record RECORD;
BEGIN
    -- Find batches older than keep_days that are fully processed
    FOR batch_record IN 
        SELECT bp.batch_file_path 
        FROM batch_processing bp
        WHERE bp.processed = TRUE 
        AND bp.saved_to_db = TRUE
        AND bp.created_at < NOW() - INTERVAL '%s days'
        AND bp.batch_file_path IS NOT NULL
    LOOP
        -- Delete the batch file
        PERFORM pg_file_unlink(batch_record.batch_file_path);
        deleted_count := deleted_count + 1;
    END LOOP;
    
    -- Delete old batch records
    DELETE FROM batch_processing 
    WHERE processed = TRUE 
    AND saved_to_db = TRUE
    AND created_at < NOW() - INTERVAL '%s days';
    
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Create a view for monitoring processing status
CREATE VIEW processing_status AS
SELECT 
    p.product_id,
    p.title as product_title,
    p.table_name,
    p.schema_created,
    COUNT(f.id) as total_files,
    COUNT(CASE WHEN f.status = 'completed' THEN 1 END) as completed_files,
    COUNT(CASE WHEN f.status = 'processing' THEN 1 END) as processing_files,
    COUNT(CASE WHEN f.status = 'error' THEN 1 END) as error_files,
    COUNT(CASE WHEN f.status = 'pending' THEN 1 END) as pending_files,
    SUM(f.rows_processed) as total_rows_processed,
    SUM(f.rows_saved) as total_rows_saved,
    MAX(f.processing_completed) as last_completed,
    p.created_at as product_created,
    p.updated_at as product_updated
FROM uspto_products p
LEFT JOIN file_processing_history f ON p.product_id = f.product_id
GROUP BY p.product_id, p.title, p.table_name, p.schema_created, p.created_at, p.updated_at
ORDER BY p.product_id;

-- Grant permissions (adjust as needed for your setup)
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO your_username;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO your_username;
-- GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO your_username;

-- Insert all USPTO product configurations from API response
INSERT INTO uspto_products (product_id, title, description, frequency, table_name, formats) VALUES
('TRCFECO2', 'Trademark Case File Data for Academia and Researchers', 'Contains detailed information on 12.1 million trademark applications filed with or registrations issued by the USPTO between 1870 and January 2023', 'YEARLY', 'product_trcfeco2', ARRAY['CSV', 'DTA']),
('TRTDXFAP', 'Trademark Full Text XML Data (No Images) – Daily Applications', 'Pending and registered trademark text data (no images) to include word mark, serial number, registration number, filing date, registration date, goods and services, classification number(s), status code(s), design search code(s), pseudo mark(s) in CY2025', 'DAILY', 'product_trtdxfap', ARRAY['XML']),
('TTABTDXF', 'Trademark Full Text XML Data (No Images) – Daily TTAB', 'TTAB text data (no images) in CY2025. The file format is eXtensible Markup Language (XML) in accordance with the U.S. Trademark Trial and Appeal Board Version 1.0 Document Type Definition (DTD)', 'DAILY', 'product_ttabtdxf', ARRAY['XML']),
('TRTDXFAG', 'Trademark Full Text XML Data (No Images) – Daily Assignments', 'Assignment text data (no images) in CY2025. The file format is eXtensible Markup Language (XML) in accordance with the U.S. Trademark Assignments Version 0.4 Document Type Definition (DTD)', 'DAILY', 'product_trtdxfag', ARRAY['XML']),
('TRTYRAP', 'Trademark Full Text XML Data (No Images) – Annual Applications', 'Contains (backfile) pending and registered trademark text data (no images) to include word mark, serial number, registration number, filing date, registration date, goods and services, classification number(s), status code(s), design search code(s), pseudo mark(s) from (APR 1884 - DEC 2024)', 'YEARLY', 'product_trtyrap', ARRAY['XML']),
('TRTYRAG', 'Trademark Full Text XML Data (No Images) – Annual Assignments', 'Contains (backfile) Trademark Assignment text data (JAN 3, 1955 - DEC 31, 2024). The file format is eXtensible Markup Language (XML) in accordance with the U.S. Trademark Assignments Version 0.4 Document Type Definition (DTD)', 'YEARLY', 'product_trtyrag', ARRAY['XML']),
('TTABYR', 'Trademark Full Text XML Data (No Images) – Annual TTAB', 'Contains (backfile) Trademark Trial and Appeal Board text data (OCT 2, 1951 - DEC 31, 2024). The file format is eXtensible Markup Language (XML) in accordance with the U.S. Trademark Trial and Appeal Board Version 1.0 Document Type Definition (DTD)', 'YEARLY', 'product_ttabyr', ARRAY['XML']),
('TRASECO', 'Trademark Assignment Data for Academia and Researchers', 'Contains detailed information on 1.29 million assignments and other transactions recorded at the USPTO between 1952 and 2023 and involving 2.28 million unique trademark properties', 'YEARLY', 'product_traseco', ARRAY['CSV', 'DTA'])
ON CONFLICT (product_id) DO NOTHING;

-- Create tables for all products
SELECT create_product_table('TRCFECO2', 'product_trcfeco2', 'case_file');
SELECT create_product_table('TRTDXFAP', 'product_trtdxfap', 'trademark_application');
SELECT create_product_table('TTABTDXF', 'product_ttabtdxf', 'ttab');
SELECT create_product_table('TRTDXFAG', 'product_trtdxfag', 'trademark_assignment');
SELECT create_product_table('TRTYRAP', 'product_trtyrap', 'trademark_application');
SELECT create_product_table('TRTYRAG', 'product_trtyrag', 'trademark_assignment');
SELECT create_product_table('TTABYR', 'product_ttabyr', 'ttab');
SELECT create_product_table('TRASECO', 'product_traseco', 'assignment');

COMMIT;
