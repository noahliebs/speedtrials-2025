-- SDWIS Database Schema Creation Script - FIXED VERSION
-- Georgia Q1 2025 Safe Drinking Water Information System Data

-- Create database (run separately if needed)
-- CREATE DATABASE sdwis_georgia;
-- \c sdwis_georgia;

-- Drop tables if they exist (in dependency order)
DROP TABLE IF EXISTS sdwa_violations_enforcement CASCADE;
DROP TABLE IF EXISTS sdwa_pn_violation_assoc CASCADE;
DROP TABLE IF EXISTS sdwa_lcr_samples CASCADE;
DROP TABLE IF EXISTS sdwa_site_visits CASCADE;
DROP TABLE IF EXISTS sdwa_events_milestones CASCADE;
DROP TABLE IF EXISTS sdwa_service_areas CASCADE;
DROP TABLE IF EXISTS sdwa_geographic_areas CASCADE;
DROP TABLE IF EXISTS sdwa_facilities CASCADE;
DROP TABLE IF EXISTS sdwa_pub_water_systems CASCADE;
DROP TABLE IF EXISTS sdwa_ref_code_values CASCADE;
DROP TABLE IF EXISTS sdwa_ref_ansi_areas CASCADE;

-- Reference Tables First

-- ANSI Areas Reference
CREATE TABLE sdwa_ref_ansi_areas (
    ansi_state_code CHAR(2) NOT NULL,
    ansi_entity_code CHAR(3) NOT NULL,
    ansi_name VARCHAR(40),
    state_code CHAR(2),
    PRIMARY KEY (ansi_state_code, ansi_entity_code)
);

-- Code Values Reference
CREATE TABLE sdwa_ref_code_values (
    value_type VARCHAR(40) NOT NULL,
    value_code VARCHAR(40) NOT NULL,
    value_description VARCHAR(250),
    PRIMARY KEY (value_type, value_code)
);

-- Public Water Systems (Main entity table)
CREATE TABLE sdwa_pub_water_systems (
    submissionyearquarter CHAR(7) NOT NULL,
    pwsid CHAR(9) NOT NULL,
    pws_name VARCHAR(100),
    primacy_agency_code CHAR(2),
    epa_region CHAR(2),
    season_begin_date CHAR(5),
    season_end_date CHAR(5),
    pws_activity_code CHAR(1),
    pws_deactivation_date DATE,
    pws_type_code CHAR(6),
    dbpr_schedule_cat_code CHAR(6),
    cds_id VARCHAR(100),
    gw_sw_code CHAR(2),
    lt2_schedule_cat_code CHAR(6),
    owner_type_code CHAR(1),
    population_served_count INTEGER,
    pop_cat_2_code CHAR(2),
    pop_cat_3_code CHAR(2),
    pop_cat_4_code CHAR(2),
    pop_cat_5_code CHAR(2),
    pop_cat_11_code CHAR(2),
    primacy_type VARCHAR(20),
    primary_source_code CHAR(4),
    is_grant_eligible_ind CHAR(1),
    is_wholesaler_ind CHAR(1),
    is_school_or_daycare_ind CHAR(1),
    service_connections_count INTEGER,
    submission_status_code CHAR(1),
    org_name VARCHAR(100),
    admin_name VARCHAR(100),
    email_addr VARCHAR(100),
    phone_number CHAR(15),
    phone_ext_number CHAR(5),
    fax_number CHAR(15),
    alt_phone_number CHAR(15),
    address_line1 VARCHAR(200),
    address_line2 VARCHAR(200),
    city_name VARCHAR(40),
    zip_code CHAR(14),
    country_code CHAR(2),
    first_reported_date DATE,
    last_reported_date DATE,
    state_code CHAR(2),
    source_water_protection_code CHAR(2),
    source_protection_begin_date DATE,
    outstanding_performer CHAR(2),
    outstanding_perform_begin_date DATE,
    reduced_rtcr_monitoring VARCHAR(20),
    reduced_monitoring_begin_date DATE,
    reduced_monitoring_end_date DATE,
    seasonal_startup_system VARCHAR(40),
    PRIMARY KEY (submissionyearquarter, pwsid)
);

-- Facilities
CREATE TABLE sdwa_facilities (
    submissionyearquarter CHAR(7) NOT NULL,
    pwsid CHAR(9) NOT NULL,
    facility_id CHAR(12) NOT NULL,
    facility_name VARCHAR(100),
    state_facility_id VARCHAR(40),
    facility_activity_code CHAR(1),
    facility_deactivation_date DATE,
    facility_type_code CHAR(4),
    submission_status_code CHAR(4),
    is_source_ind CHAR(1),
    water_type_code CHAR(4),
    availability_code CHAR(4),
    seller_treatment_code CHAR(4),
    seller_pwsid CHAR(9),
    seller_pws_name VARCHAR(100),
    filtration_status_code CHAR(4),
    is_source_treated_ind CHAR(1),
    first_reported_date DATE,
    last_reported_date DATE,
    PRIMARY KEY (submissionyearquarter, pwsid, facility_id),
    FOREIGN KEY (submissionyearquarter, pwsid) REFERENCES sdwa_pub_water_systems(submissionyearquarter, pwsid)
);

-- Geographic Areas
CREATE TABLE sdwa_geographic_areas (
    submissionyearquarter CHAR(7) NOT NULL,
    pwsid CHAR(9) NOT NULL,
    geo_id CHAR(20) NOT NULL,
    area_type_code CHAR(4),
    tribal_code CHAR(10),
    state_served CHAR(4),
    ansi_entity_code CHAR(4),
    zip_code_served CHAR(5),
    city_served VARCHAR(40),
    county_served VARCHAR(40),
    last_reported_date DATE,
    PRIMARY KEY (submissionyearquarter, pwsid, geo_id),
    FOREIGN KEY (submissionyearquarter, pwsid) REFERENCES sdwa_pub_water_systems(submissionyearquarter, pwsid)
);

-- Service Areas
CREATE TABLE sdwa_service_areas (
    submissionyearquarter CHAR(7) NOT NULL,
    pwsid CHAR(9) NOT NULL,
    service_area_type_code CHAR(4),
    is_primary_service_area_code CHAR(1),
    first_reported_date DATE,
    last_reported_date DATE,
    FOREIGN KEY (submissionyearquarter, pwsid) REFERENCES sdwa_pub_water_systems(submissionyearquarter, pwsid)
);

-- Events and Milestones
CREATE TABLE sdwa_events_milestones (
    submissionyearquarter CHAR(7) NOT NULL,
    pwsid CHAR(9) NOT NULL,
    event_schedule_id CHAR(20) NOT NULL,
    event_end_date DATE,
    event_actual_date DATE,
    event_comments_text VARCHAR(2000),
    event_milestone_code CHAR(4),
    event_reason_code CHAR(4),
    first_reported_date DATE,
    last_reported_date DATE,
    PRIMARY KEY (submissionyearquarter, pwsid, event_schedule_id),
    FOREIGN KEY (submissionyearquarter, pwsid) REFERENCES sdwa_pub_water_systems(submissionyearquarter, pwsid)
);

-- Site Visits
CREATE TABLE sdwa_site_visits (
    submissionyearquarter CHAR(7) NOT NULL,
    pwsid CHAR(9) NOT NULL,
    visit_id CHAR(20) NOT NULL,
    visit_date DATE,
    agency_type_code CHAR(2),
    visit_reason_code CHAR(4),
    management_ops_eval_code CHAR(1),
    source_water_eval_code CHAR(1),
    security_eval_code CHAR(1),
    pumps_eval_code CHAR(1),
    other_eval_code CHAR(1),
    compliance_eval_code CHAR(1),
    data_verification_eval_code CHAR(1),
    treatment_eval_code CHAR(1),
    finished_water_stor_eval_code CHAR(1),
    distribution_eval_code CHAR(1),
    financial_eval_code CHAR(1),
    visit_comments VARCHAR(2000),
    first_reported_date DATE,
    last_reported_date DATE,
    PRIMARY KEY (submissionyearquarter, pwsid, visit_id),
    FOREIGN KEY (submissionyearquarter, pwsid) REFERENCES sdwa_pub_water_systems(submissionyearquarter, pwsid)
);

-- Lead and Copper Rule Samples
CREATE TABLE sdwa_lcr_samples (
    submissionyearquarter CHAR(7) NOT NULL,
    pwsid CHAR(9) NOT NULL,
    sample_id CHAR(20) NOT NULL,
    sampling_end_date DATE,
    sampling_start_date DATE,
    reconciliation_id VARCHAR(40),
    sample_first_reported_date DATE,
    sample_last_reported_date DATE,
    sar_id INTEGER NOT NULL,
    contaminant_code CHAR(4),
    result_sign_code CHAR(1),
    sample_measure NUMERIC,
    unit_of_measure CHAR(4),
    sar_first_reported_date DATE,
    sar_last_reported_date DATE,
    PRIMARY KEY (submissionyearquarter, pwsid, sample_id, sar_id),
    FOREIGN KEY (submissionyearquarter, pwsid) REFERENCES sdwa_pub_water_systems(submissionyearquarter, pwsid)
);

-- Public Notice Violations Association (FIXED - Bug 1 & 3)
-- Added compl_per_begin_date and compl_per_end_date columns
-- Used SERIAL id instead of composite primary key to avoid duplicate issues
CREATE TABLE sdwa_pn_violation_assoc (
    id SERIAL PRIMARY KEY,
    submissionyearquarter CHAR(7) NOT NULL,
    pwsid CHAR(9) NOT NULL,
    pn_violation_id CHAR(20) NOT NULL,
    related_violation_id CHAR(20),
    compl_per_begin_date DATE,          -- Bug 1 Fix: Added column
    compl_per_end_date DATE,            -- Bug 1 Fix: Added column  
    non_compl_per_begin_date DATE,
    non_compl_per_end_date DATE,
    violation_code CHAR(4),
    contamination_code CHAR(4),
    first_reported_date DATE,
    last_reported_date DATE,
    FOREIGN KEY (submissionyearquarter, pwsid) REFERENCES sdwa_pub_water_systems(submissionyearquarter, pwsid)
);

-- Violations and Enforcement (largest table)
CREATE TABLE sdwa_violations_enforcement (
    id SERIAL PRIMARY KEY,
    submissionyearquarter CHAR(7) NOT NULL,
    pwsid CHAR(9) NOT NULL,
    violation_id CHAR(20) NOT NULL,
    facility_id CHAR(12),
    compl_per_begin_date DATE,
    compl_per_end_date DATE,
    non_compl_per_begin_date DATE,
    non_compl_per_end_date DATE,
    pws_deactivation_date DATE,
    violation_code CHAR(4),
    violation_category_code CHAR(5),
    is_health_based_ind CHAR(1),
    contaminant_code CHAR(4),
    viol_measure NUMERIC,
    unit_of_measure CHAR(9),
    federal_mcl VARCHAR(31),                -- Bug 4: Increased size to handle complex values
    state_mcl NUMERIC,
    is_major_viol_ind CHAR(1),
    severity_ind_cnt INTEGER,
    calculated_rtc_date DATE,
    violation_status VARCHAR(11),
    public_notification_tier INTEGER,
    calculated_pub_notif_tier INTEGER,
    viol_originator_code CHAR(4),
    sample_result_id VARCHAR(40),
    corrective_action_id VARCHAR(40),
    rule_code CHAR(3),
    rule_group_code CHAR(3),
    rule_family_code CHAR(3),
    viol_first_reported_date DATE,
    viol_last_reported_date DATE,
    enforcement_id CHAR(20),
    enforcement_date DATE,
    enforcement_action_type_code CHAR(4),
    enf_action_category VARCHAR(4000),      -- Bug 4: Can contain commas and long text
    enf_originator_code CHAR(4),
    enf_first_reported_date DATE,
    enf_last_reported_date DATE,
    FOREIGN KEY (submissionyearquarter, pwsid) REFERENCES sdwa_pub_water_systems(submissionyearquarter, pwsid)
);

-- Create indexes for better performance
CREATE INDEX idx_pws_pws_type ON sdwa_pub_water_systems(pws_type_code);
CREATE INDEX idx_pws_activity ON sdwa_pub_water_systems(pws_activity_code);
CREATE INDEX idx_pws_population ON sdwa_pub_water_systems(population_served_count);
CREATE INDEX idx_pws_state ON sdwa_pub_water_systems(state_code);

CREATE INDEX idx_facilities_type ON sdwa_facilities(facility_type_code);
CREATE INDEX idx_facilities_activity ON sdwa_facilities(facility_activity_code);

CREATE INDEX idx_violations_category ON sdwa_violations_enforcement(violation_category_code);
CREATE INDEX idx_violations_health ON sdwa_violations_enforcement(is_health_based_ind);
CREATE INDEX idx_violations_status ON sdwa_violations_enforcement(violation_status);
CREATE INDEX idx_violations_dates ON sdwa_violations_enforcement(non_compl_per_begin_date, non_compl_per_end_date);

CREATE INDEX idx_sites_date ON sdwa_site_visits(visit_date);
CREATE INDEX idx_sites_reason ON sdwa_site_visits(visit_reason_code);

-- Indexes for PN violation associations (Bug 3 fix)
CREATE INDEX idx_pn_violations_pwsid ON sdwa_pn_violation_assoc(submissionyearquarter, pwsid);
CREATE INDEX idx_pn_violations_id ON sdwa_pn_violation_assoc(pn_violation_id);

-- Add comments to tables
COMMENT ON TABLE sdwa_pub_water_systems IS 'Public Water Systems - Main entity table containing system information';
COMMENT ON TABLE sdwa_facilities IS 'Water system facilities (wells, treatment plants, distribution systems, etc.)';
COMMENT ON TABLE sdwa_violations_enforcement IS 'Violations of drinking water regulations and enforcement actions';
COMMENT ON TABLE sdwa_site_visits IS 'Regulatory site visits and inspection results';
COMMENT ON TABLE sdwa_lcr_samples IS 'Lead and Copper Rule sampling results';
COMMENT ON TABLE sdwa_events_milestones IS 'Regulatory milestones and events';
COMMENT ON TABLE sdwa_geographic_areas IS 'Geographic areas served by water systems';
COMMENT ON TABLE sdwa_service_areas IS 'Service area classifications';
COMMENT ON TABLE sdwa_pn_violation_assoc IS 'Public notification violations (FIXED: added compliance period dates, uses SERIAL id)';
COMMENT ON TABLE sdwa_ref_code_values IS 'Reference table for code descriptions';
COMMENT ON TABLE sdwa_ref_ansi_areas IS 'ANSI geographic area codes';

COMMENT ON COLUMN sdwa_pub_water_systems.pwsid IS 'Public Water System ID - unique identifier (state code + 7 digits)';
COMMENT ON COLUMN sdwa_pub_water_systems.pws_type_code IS 'CWS=Community, TNCWS=Transient Non-Community, NTNCWS=Non-Transient Non-Community';
COMMENT ON COLUMN sdwa_violations_enforcement.is_health_based_ind IS 'Y/N - MCL, MRDL, or treatment technique violations';
COMMENT ON COLUMN sdwa_violations_enforcement.violation_status IS 'Resolved/Archived/Addressed/Unaddressed';

-- Additional comments for fixed fields
COMMENT ON COLUMN sdwa_pn_violation_assoc.compl_per_begin_date IS 'Compliance period begin date (Bug 1 fix)';
COMMENT ON COLUMN sdwa_pn_violation_assoc.compl_per_end_date IS 'Compliance period end date (Bug 1 fix)';
COMMENT ON COLUMN sdwa_pn_violation_assoc.id IS 'Auto-generated ID to handle duplicate keys (Bug 3 fix)';

-- Data quality note
COMMENT ON COLUMN sdwa_violations_enforcement.federal_mcl IS 'Federal MCL value - can contain complex formats (Bug 4 fix)';
COMMENT ON COLUMN sdwa_violations_enforcement.enf_action_category IS 'Enforcement action category - can contain long text with commas (Bug 4 fix)';