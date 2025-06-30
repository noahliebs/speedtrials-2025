#!/bin/bash

# SDWIS Data Loading Script - FINAL FIXED VERSION
# Load Georgia Q1 2025 Safe Drinking Water Information System Data

# Configuration
DB_NAME="sdwis_georgia"
DB_USER="noahlieberman"
DB_HOST="localhost"
DB_PORT="5432"
DATA_DIR="$HOME/github/speedtrials-2025/data"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Starting SDWIS Data Loading Process${NC}"

check_result() {
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ $1 completed successfully${NC}"
    else
        echo -e "${RED}✗ $1 failed${NC}"
        exit 1
    fi
}

# Step 1: Create schema
echo -e "${YELLOW}Creating database schema...${NC}"
psql -U $DB_USER -h $DB_HOST -p $DB_PORT -d $DB_NAME -f etl/create_sdwis_schema.sql
check_result "Schema creation"

# Step 2: Clean data
echo -e "${YELLOW}Cleaning data files...${NC}"

# PN Violation Assoc Clean
echo "  - Cleaning PN violation associations file..."
sed 's/--->//g' "$DATA_DIR/SDWA_PN_VIOLATION_ASSOC.csv" > "$DATA_DIR/SDWA_PN_VIOLATION_ASSOC_CLEANED.csv"

# Violations Enforcement Clean
echo "  - Cleaning violations enforcement file..."

cat <<EOF | python3
import csv
from pathlib import Path

input_path = Path("${DATA_DIR}/SDWA_VIOLATIONS_ENFORCEMENT.csv")
output_path = Path("${DATA_DIR}/SDWA_VIOLATIONS_ENFORCEMENT_FINAL.csv")
log_path = Path("${DATA_DIR}/violations_skipped.log")

with input_path.open('r', encoding='utf-8') as infile, \
     output_path.open('w', encoding='utf-8', newline='') as outfile, \
     log_path.open('w', encoding='utf-8') as logfile:

    reader = csv.reader(infile)
    writer = csv.writer(outfile, quoting=csv.QUOTE_NONE, escapechar='\\\\')

    header = next(reader)
    writer.writerow(header)
    expected_cols = len(header)

    for row_num, row in enumerate(reader, start=2):
        while len(row) > expected_cols and row[-1].strip() == '':
            row = row[:-1]

        if len(row) != expected_cols:
            logfile.write(f"SKIP line {row_num}: column count = {len(row)} instead of {expected_cols}\n")
            continue

        violation_id = row[2].strip()
        if not violation_id:
            logfile.write(f"SKIP line {row_num}: missing violation_id\n")
            continue

        cleaned = [cell.strip().replace('--->', '') for cell in row]
        writer.writerow(cleaned)
EOF

if [ ! -f "$DATA_DIR/SDWA_VIOLATIONS_ENFORCEMENT_FINAL.csv" ]; then
  echo -e "${RED}✗ Cleaner failed: FINAL.csv not created${NC}"
  exit 1
fi
echo -e "${GREEN}✓ Data cleaning completed${NC}"

# Step 3: Load data into DB
echo -e "${YELLOW}Loading tables...${NC}"

echo -e "${YELLOW}Loading reference code values...${NC}"
psql -U $DB_USER -h $DB_HOST -p $DB_PORT -d $DB_NAME -c "\COPY sdwa_ref_code_values FROM '$DATA_DIR/SDWA_REF_CODE_VALUES.csv' WITH CSV HEADER NULL AS '';"
check_result "Reference code values loading"

echo -e "${YELLOW}Loading public water systems...${NC}"
psql -U $DB_USER -h $DB_HOST -p $DB_PORT -d $DB_NAME -c "\COPY sdwa_pub_water_systems FROM '$DATA_DIR/SDWA_PUB_WATER_SYSTEMS.csv' WITH CSV HEADER NULL AS '';"
check_result "Public water systems loading"

echo -e "${YELLOW}Loading facilities...${NC}"
psql -U $DB_USER -h $DB_HOST -p $DB_PORT -d $DB_NAME -c "\COPY sdwa_facilities FROM '$DATA_DIR/SDWA_FACILITIES.csv' WITH CSV HEADER NULL AS '';"
check_result "Facilities loading"

echo -e "${YELLOW}Loading geographic areas...${NC}"
psql -U $DB_USER -h $DB_HOST -p $DB_PORT -d $DB_NAME -c "\COPY sdwa_geographic_areas FROM '$DATA_DIR/SDWA_GEOGRAPHIC_AREAS.csv' WITH CSV HEADER NULL AS '';"
check_result "Geographic areas loading"

echo -e "${YELLOW}Loading service areas...${NC}"
psql -U $DB_USER -h $DB_HOST -p $DB_PORT -d $DB_NAME -c "\COPY sdwa_service_areas FROM '$DATA_DIR/SDWA_SERVICE_AREAS.csv' WITH CSV HEADER NULL AS '';"
check_result "Service areas loading"

echo -e "${YELLOW}Loading events and milestones...${NC}"
psql -U $DB_USER -h $DB_HOST -p $DB_PORT -d $DB_NAME -c "\COPY sdwa_events_milestones FROM '$DATA_DIR/SDWA_EVENTS_MILESTONES.csv' WITH CSV HEADER NULL AS '';"
check_result "Events and milestones loading"

echo -e "${YELLOW}Loading site visits...${NC}"
psql -U $DB_USER -h $DB_HOST -p $DB_PORT -d $DB_NAME -c "\COPY sdwa_site_visits FROM '$DATA_DIR/SDWA_SITE_VISITS.csv' WITH CSV HEADER NULL AS '';"
check_result "Site visits loading"

echo -e "${YELLOW}Loading LCR samples...${NC}"
psql -U $DB_USER -h $DB_HOST -p $DB_PORT -d $DB_NAME -c "\COPY sdwa_lcr_samples FROM '$DATA_DIR/SDWA_LCR_SAMPLES.csv' WITH CSV HEADER NULL AS '';"
check_result "LCR samples loading"

echo -e "${YELLOW}Loading PN violation associations...${NC}"
psql -U $DB_USER -h $DB_HOST -p $DB_PORT -d $DB_NAME -c "\COPY sdwa_pn_violation_assoc(
    submissionyearquarter, pwsid, pn_violation_id, related_violation_id,
    compl_per_begin_date, compl_per_end_date,
    non_compl_per_begin_date, non_compl_per_end_date,
    violation_code, contamination_code,
    first_reported_date, last_reported_date
) FROM '$DATA_DIR/SDWA_PN_VIOLATION_ASSOC_CLEANED.csv' WITH CSV HEADER NULL AS '';"
check_result "PN violation associations loading"

echo -e "${YELLOW}Loading violations and enforcement...${NC}"
psql -U $DB_USER -h $DB_HOST -p $DB_PORT -d $DB_NAME -c "\COPY sdwa_violations_enforcement(
    submissionyearquarter, pwsid, violation_id, facility_id, compl_per_begin_date, compl_per_end_date,
    non_compl_per_begin_date, non_compl_per_end_date, pws_deactivation_date, violation_code,
    violation_category_code, is_health_based_ind, contaminant_code, viol_measure, unit_of_measure,
    federal_mcl, state_mcl, is_major_viol_ind, severity_ind_cnt, calculated_rtc_date,
    violation_status, public_notification_tier, calculated_pub_notif_tier, viol_originator_code,
    sample_result_id, corrective_action_id, rule_code, rule_group_code, rule_family_code,
    viol_first_reported_date, viol_last_reported_date, enforcement_id, enforcement_date,
    enforcement_action_type_code, enf_action_category, enf_originator_code,
    enf_first_reported_date, enf_last_reported_date
) FROM '$DATA_DIR/SDWA_VIOLATIONS_ENFORCEMENT_FINAL.csv' WITH CSV HEADER NULL AS '';"
check_result "Violations and enforcement loading"
