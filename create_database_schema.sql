-- USPTO Trademark Database Schema
-- Separate tables for each file type based on actual structure analysis

-- Drop existing tables if they exist (for clean setup)
DROP TABLE IF EXISTS download_history CASCADE;
DROP TABLE IF EXISTS trademark_case_files CASCADE;
DROP TABLE IF EXISTS trademark_assignments CASCADE;
DROP TABLE IF EXISTS trademark_ttab_proceedings CASCADE;
DROP TABLE IF EXISTS trademark_classifications CASCADE;
DROP TABLE IF EXISTS trademark_owners CASCADE;
DROP TABLE IF EXISTS trademark_correspondents CASCADE;
DROP TABLE IF EXISTS trademark_events CASCADE;
DROP TABLE IF EXISTS trademark_statements CASCADE;
DROP TABLE IF EXISTS trademark_design_searches CASCADE;
DROP TABLE IF EXISTS trademark_foreign_applications CASCADE;
DROP TABLE IF EXISTS trademark_international_registrations CASCADE;

-- Download history table (unchanged)
CREATE TABLE download_history (
    id SERIAL PRIMARY KEY,
    file_name VARCHAR(255) UNIQUE,
    file_url TEXT,
    file_size BIGINT,
    download_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    file_hash VARCHAR(64),
    product_id VARCHAR(50),
    processed BOOLEAN DEFAULT FALSE,
    last_processed TIMESTAMP,
    processing_attempts INTEGER DEFAULT 0
);

-- Main trademark case files table (from CSV and XML case-file elements)
CREATE TABLE trademark_case_files (
    id SERIAL PRIMARY KEY,
    serial_number VARCHAR(20) UNIQUE,
    registration_number VARCHAR(20),
    
    -- Basic case file information
    transaction_date DATE,
    filing_date DATE,
    registration_date DATE,
    status_code VARCHAR(10),
    status_date DATE,
    
    -- Mark information
    mark_identification TEXT,
    mark_drawing_code VARCHAR(10),
    
    -- Case file header flags (from CSV)
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
    
    -- International registration fields
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
    
    -- Metadata
    tad_file_id INTEGER,
    data_source VARCHAR(100),
    file_hash VARCHAR(64),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Trademark assignments table (from assignment XML files)
CREATE TABLE trademark_assignments (
    id SERIAL PRIMARY KEY,
    assignment_id VARCHAR(50) UNIQUE, -- Generated from reel-no + frame-no
    
    -- Assignment details
    date_recorded DATE,
    conveyance_text TEXT,
    frame_no VARCHAR(10),
    reel_no VARCHAR(10),
    page_count INTEGER,
    last_update_date DATE,
    purge_indicator CHAR(1),
    
    -- Correspondent information
    correspondent_name TEXT,
    correspondent_address_1 TEXT,
    correspondent_address_2 TEXT,
    correspondent_address_3 TEXT,
    
    -- Metadata
    data_source VARCHAR(100),
    file_hash VARCHAR(64),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Trademark owners table (from case-file-owners and assignees)
CREATE TABLE trademark_owners (
    id SERIAL PRIMARY KEY,
    serial_number VARCHAR(20),
    registration_number VARCHAR(20),
    assignment_id VARCHAR(50), -- Links to trademark_assignments if from assignment
    
    -- Owner details
    party_name TEXT,
    party_type VARCHAR(10),
    legal_entity_type_code VARCHAR(10),
    legal_entity_text TEXT,
    entry_number VARCHAR(10),
    
    -- Address information
    address_1 TEXT,
    address_2 TEXT,
    city VARCHAR(100),
    state VARCHAR(50),
    postcode VARCHAR(20),
    country VARCHAR(10),
    
    -- Additional details
    nationality TEXT,
    name_change_explanation TEXT,
    execution_date DATE, -- For assignors
    
    -- Owner type
    owner_type VARCHAR(20), -- 'APPLICANT', 'ASSIGNEE', 'ASSIGNOR'
    
    -- Metadata
    data_source VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (serial_number) REFERENCES trademark_case_files(serial_number) ON DELETE CASCADE,
    FOREIGN KEY (assignment_id) REFERENCES trademark_assignments(assignment_id) ON DELETE CASCADE
);

-- Trademark correspondents table
CREATE TABLE trademark_correspondents (
    id SERIAL PRIMARY KEY,
    serial_number VARCHAR(20),
    
    -- Correspondent details
    person_or_organization_name TEXT,
    address_1 TEXT,
    address_2 TEXT,
    address_3 TEXT,
    address_4 TEXT,
    
    -- Metadata
    data_source VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (serial_number) REFERENCES trademark_case_files(serial_number) ON DELETE CASCADE
);

-- Trademark classifications table
CREATE TABLE trademark_classifications (
    id SERIAL PRIMARY KEY,
    serial_number VARCHAR(20),
    
    -- Classification details
    international_code VARCHAR(10),
    us_code VARCHAR(10),
    primary_code VARCHAR(10),
    international_code_total_no INTEGER,
    us_code_total_no INTEGER,
    status_code VARCHAR(10),
    status_date DATE,
    
    -- Metadata
    data_source VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (serial_number) REFERENCES trademark_case_files(serial_number) ON DELETE CASCADE
);

-- Trademark events table (from case-file-event-statements)
CREATE TABLE trademark_events (
    id SERIAL PRIMARY KEY,
    serial_number VARCHAR(20),
    
    -- Event details
    code VARCHAR(10),
    date DATE,
    description_text TEXT,
    number VARCHAR(10),
    type VARCHAR(10),
    
    -- Metadata
    data_source VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (serial_number) REFERENCES trademark_case_files(serial_number) ON DELETE CASCADE
);

-- Trademark statements table (from case-file-statements)
CREATE TABLE trademark_statements (
    id SERIAL PRIMARY KEY,
    serial_number VARCHAR(20),
    
    -- Statement details
    type_code VARCHAR(20),
    text TEXT,
    
    -- Metadata
    data_source VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (serial_number) REFERENCES trademark_case_files(serial_number) ON DELETE CASCADE
);

-- Trademark design searches table
CREATE TABLE trademark_design_searches (
    id SERIAL PRIMARY KEY,
    serial_number VARCHAR(20),
    
    -- Design search details
    code VARCHAR(20),
    
    -- Metadata
    data_source VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (serial_number) REFERENCES trademark_case_files(serial_number) ON DELETE CASCADE
);

-- Trademark foreign applications table
CREATE TABLE trademark_foreign_applications (
    id SERIAL PRIMARY KEY,
    serial_number VARCHAR(20),
    
    -- Foreign application details
    country VARCHAR(10),
    entry_number VARCHAR(10),
    foreign_priority_claim_in BOOLEAN DEFAULT FALSE,
    registration_date DATE,
    registration_expiration_date DATE,
    registration_number VARCHAR(20),
    
    -- Metadata
    data_source VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (serial_number) REFERENCES trademark_case_files(serial_number) ON DELETE CASCADE
);

-- Trademark international registrations table
CREATE TABLE trademark_international_registrations (
    id SERIAL PRIMARY KEY,
    serial_number VARCHAR(20),
    
    -- International registration details
    international_registration_number VARCHAR(20),
    international_registration_date DATE,
    international_publication_date DATE,
    international_renewal_date DATE,
    auto_protection_date DATE,
    first_refusal_in BOOLEAN DEFAULT FALSE,
    international_status_code VARCHAR(10),
    international_status_date DATE,
    priority_claimed_date DATE,
    priority_claimed_in BOOLEAN DEFAULT FALSE,
    
    -- Metadata
    data_source VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (serial_number) REFERENCES trademark_case_files(serial_number) ON DELETE CASCADE
);

-- TTAB proceedings table (for TTAB XML files)
CREATE TABLE trademark_ttab_proceedings (
    id SERIAL PRIMARY KEY,
    proceeding_number VARCHAR(20) UNIQUE,
    
    -- Proceeding details (structure to be determined from actual TTAB data)
    proceeding_type VARCHAR(50),
    status VARCHAR(50),
    filing_date DATE,
    
    -- Metadata
    data_source VARCHAR(100),
    file_hash VARCHAR(64),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for better performance
CREATE INDEX idx_trademark_case_files_serial ON trademark_case_files(serial_number);
CREATE INDEX idx_trademark_case_files_registration ON trademark_case_files(registration_number);
CREATE INDEX idx_trademark_case_files_filing_date ON trademark_case_files(filing_date);
CREATE INDEX idx_trademark_case_files_status ON trademark_case_files(status_code);
-- mark_identification can be very large (TEXT). A btree index on the raw column
-- can exceed PostgreSQL index row size limits. Use an expression index on the
-- MD5 hash of the value instead (compact, fixed-size) to support lookups.
CREATE INDEX idx_trademark_case_files_mark_id ON trademark_case_files (md5(mark_identification));

CREATE INDEX idx_trademark_assignments_date ON trademark_assignments(date_recorded);
CREATE INDEX idx_trademark_assignments_reel_frame ON trademark_assignments(reel_no, frame_no);

CREATE INDEX idx_trademark_owners_serial ON trademark_owners(serial_number);
CREATE INDEX idx_trademark_owners_name ON trademark_owners(party_name);
CREATE INDEX idx_trademark_owners_type ON trademark_owners(owner_type);

CREATE INDEX idx_trademark_correspondents_serial ON trademark_correspondents(serial_number);

CREATE INDEX idx_trademark_classifications_serial ON trademark_classifications(serial_number);
CREATE INDEX idx_trademark_classifications_code ON trademark_classifications(international_code);

CREATE INDEX idx_trademark_events_serial ON trademark_events(serial_number);
CREATE INDEX idx_trademark_events_date ON trademark_events(date);

CREATE INDEX idx_trademark_statements_serial ON trademark_statements(serial_number);

CREATE INDEX idx_trademark_design_searches_serial ON trademark_design_searches(serial_number);

CREATE INDEX idx_trademark_foreign_apps_serial ON trademark_foreign_applications(serial_number);

CREATE INDEX idx_trademark_intl_reg_serial ON trademark_international_registrations(serial_number);

CREATE INDEX idx_trademark_ttab_proceeding ON trademark_ttab_proceedings(proceeding_number);

-- Create a view for easy querying of complete trademark information
CREATE VIEW trademark_complete AS
SELECT 
    tcf.id,
    tcf.serial_number,
    tcf.registration_number,
    tcf.filing_date,
    tcf.registration_date,
    tcf.status_code,
    tcf.status_date,
    tcf.mark_identification,
    tcf.mark_drawing_code,
    tcf.data_source,
    tcf.created_at,
    tcf.updated_at,
    
    -- Owner information
    o.party_name as owner_name,
    o.address_1 as owner_address_1,
    o.city as owner_city,
    o.state as owner_state,
    o.country as owner_country,
    
    -- Correspondent information
    c.person_or_organization_name as correspondent_name,
    c.address_1 as correspondent_address_1,
    c.address_2 as correspondent_address_2,
    c.address_3 as correspondent_address_3,
    
    -- Classification information
    cl.international_code,
    cl.us_code,
    
    -- Goods and services
    s.text as goods_services
    
FROM trademark_case_files tcf
LEFT JOIN trademark_owners o ON tcf.serial_number = o.serial_number AND o.owner_type = 'APPLICANT'
LEFT JOIN trademark_correspondents c ON tcf.serial_number = c.serial_number
LEFT JOIN trademark_classifications cl ON tcf.serial_number = cl.serial_number
LEFT JOIN trademark_statements s ON tcf.serial_number = s.serial_number AND s.type_code = 'GS0201';

-- Grant permissions (adjust as needed for your setup)
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO your_username;
-- GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO your_username;
