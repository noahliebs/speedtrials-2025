"""Enhanced UI Components for Georgia Water Quality System with CCR and Schedule pages."""

import re

import pandas as pd
import streamlit as st

from constants import GEORGIA_COUNTIES
from constants import POINT_OF_CONTACT_TYPES
from constants import PRIMARY_SOURCE_WATER_TYPES
from constants import SAMPLE_CLASSES
from constants import WATER_SYSTEM_TYPES


def render_enhanced_search_interface():
    """Render the enhanced search interface matching Georgia's official site."""
    st.subheader("🔍 Water System Search")
    st.markdown('<div class="search-box">', unsafe_allow_html=True)

    # Public Water Supply Systems Search Parameters
    st.markdown("### 🏢 Public Water Supply Systems Search Parameters")
    col1, col2 = st.columns(2)

    with col1:
        system_id = st.text_input(
            "📋 Water System No.",
            placeholder="e.g., GA1234567",
            help="Enter the Georgia water system ID (will auto-add 'GA' prefix if missing)",
        )
        county = st.selectbox(
            "🏛️ Principal County Served",
            options=GEORGIA_COUNTIES,
            index=0,
            help="Select the primary county served by the water system",
        )
        source_water_type = st.selectbox(
            "🌊 Primary Source Water Type",
            options=[item["label"] for item in PRIMARY_SOURCE_WATER_TYPES],
            index=0,
            help="Select the primary source of water for the system",
        )

    with col2:
        system_name = st.text_input(
            "🏢 Water System Name",
            placeholder="Enter system name",
            help="Enter part or all of the water system name",
        )
        water_system_type = st.selectbox(
            "📊 Water System Type",
            options=[item["label"] for item in WATER_SYSTEM_TYPES],
            index=0,
            help="Community systems serve residents year-round, Non-Community serve transient populations",
        )
        contact_type = st.selectbox(
            "📞 Point of Contact Type",
            options=[item["label"] for item in POINT_OF_CONTACT_TYPES],
            index=0,
            help="Select the type of contact person for the water system",
        )

    # Sample Search Parameters
    st.markdown("---")
    st.markdown("### 🧪 Sample Search Parameters")
    sample_class = st.selectbox(
        "🧪 Sample Class",
        options=[item["label"] for item in SAMPLE_CLASSES],
        index=0,
        help="Select the type of water quality samples to search for",
    )

    start_date, end_date = None, None
    if sample_class != "Click to select a value...":
        st.markdown("📅 **Sample Collection Date Range**")
        st.caption("(Defaults to last 2 years if unchanged.)")
        date_col1, date_col2 = st.columns(2)
        with date_col1:
            start_date = st.date_input(
                "From",
                value=pd.Timestamp.now() - pd.DateOffset(years=2),
                help="Start date for sample collection period",
            )
        with date_col2:
            end_date = st.date_input(
                "To",
                value=pd.Timestamp.now(),
                help="End date for sample collection period",
            )

    # Search Actions
    st.markdown("---")
    st.markdown("### 🎯 Search Actions")
    sc1, sc2, sc3, sc4 = st.columns(4)
    with sc1:
        search_systems = st.button(
            "🔍 Search For Water Systems", type="primary", use_container_width=True
        )
    with sc2:
        disabled = sample_class == "Click to select a value..."
        search_samples = st.button(
            "🧪 Search For Samples", use_container_width=True, disabled=disabled
        )
    with sc3:
        schedule_lookup = st.button("📅 Schedule Lookup", use_container_width=True)
    with sc4:
        consumer_confidence = st.button(
            "📄 Consumer Confidence", use_container_width=True
        )

    # Utility
    u1, u2, u3 = st.columns(3)
    with u1:
        if st.button("🔄 Clear", use_container_width=True):
            st.rerun()
    with u2:
        if st.button("📖 Glossary", use_container_width=True):
            show_glossary()
    with u3:
        if st.button("🗺️ County Map", use_container_width=True):
            show_county_map_info()

    st.markdown("</div>", unsafe_allow_html=True)

    return {
        "system_id": system_id or None,
        "system_name": system_name or None,
        "county": county if county != "All" else None,
        "water_system_type": water_system_type if water_system_type != "All" else None,
        "source_water_type": source_water_type if source_water_type != "All" else None,
        "contact_type": contact_type if contact_type != "None" else None,
        "sample_class": sample_class
        if sample_class != "Click to select a value..."
        else None,
        "start_date": start_date,
        "end_date": end_date,
        "actions": {
            "search_systems": search_systems,
            "search_samples": search_samples,
            "schedule_lookup": schedule_lookup,
            "consumer_confidence": consumer_confidence,
        },
    }


def render_ccr_interface(api_manager):
    """Render the Consumer Confidence Report interface."""
    st.subheader("📄 Review Consumer Confidence Data")
    st.markdown('<div class="search-box">', unsafe_allow_html=True)
    c1, c2 = st.columns([1, 2])
    with c1:
        st.markdown("**Enter or Select Water System**")
        manual_entry = st.text_input(
            label="Manual water system entry",
            placeholder="Enter system name or ID",
            key="ccr_manual_entry",
            label_visibility="collapsed",
        )
    with c2:
        st.markdown("**Or**")
        water_systems = get_all_water_systems(api_manager)
        if water_systems:
            selected_system = render_searchable_dropdown(
                "Select Water System",
                water_systems,
                key="ccr_system_select",
                placeholder="Select Water System",
            )
        else:
            selected_system = st.selectbox(
                label="Loading systems...",
                options=["Loading systems..."],
                disabled=True,
                label_visibility="collapsed",
            )
    st.markdown("**Select CCR Year**")
    ccr_year = st.selectbox(
        label="CCR Year",
        options=api_manager.sql_manager.get_ccr_years_available(),
        index=0,
        key="ccr_year",
        label_visibility="collapsed",
    )
    st.markdown("**Select Report Format**")
    report_format = st.selectbox(
        label="Report format",
        options=["PDF", "HTML", "Excel"],
        index=0,
        key="ccr_format",
        label_visibility="collapsed",
    )
    st.markdown("</div>", unsafe_allow_html=True)

    btn_col1, btn_col2, btn_col3 = st.columns(3)
    with btn_col2:
        if st.button("Generate Report", type="primary", use_container_width=True):
            handle_ccr_generation(
                api_manager, manual_entry, selected_system, ccr_year, report_format
            )

    return {
        "manual_entry": manual_entry,
        "selected_system": selected_system,
        "ccr_year": ccr_year,
        "report_format": report_format,
    }


def render_schedule_interface(api_manager):
    """Render the SDWIS Monitoring Schedule Lookup interface."""
    st.subheader("📅 SDWIS Monitoring Schedule Lookup")
    st.markdown('<div class="search-box">', unsafe_allow_html=True)

    water_systems = get_all_water_systems(api_manager)
    st.markdown("**Select Water System**")
    if water_systems:
        selected_system = render_searchable_dropdown(
            "",
            water_systems,
            key="schedule_system_select",
            placeholder="Select Water System",
        )
        if selected_system:
            st.info(f"Selected: {selected_system}")
        s1, s2, s3 = st.columns([1, 2, 1])
        with s2:
            if st.button(
                "Get Schedules for this PWS",
                use_container_width=True,
                disabled=not selected_system,
            ):
                handle_schedule_generation(api_manager, selected_system)
    else:
        selected_system = None
        st.warning("Loading water systems...")

    st.markdown("**Select WSF**")
    facilities = (
        get_facilities_for_system(api_manager, selected_system)
        if selected_system
        else []
    )
    if facilities:
        selected_facility = st.selectbox(
            label="Select WSF",
            options=["Select Facility"] + facilities,
            key="schedule_facility_select",
            label_visibility="collapsed",  # or "visible"
        )
        f1, f2, f3 = st.columns([1, 2, 1])
        with f2:
            if st.button(
                "Get Schedules for this WSF",
                use_container_width=True,
                disabled=(
                    not selected_facility or selected_facility == "Select Facility"
                ),
            ):
                handle_schedule_generation(
                    api_manager, selected_system, selected_facility
                )
    else:
        selected_facility = None

    st.markdown("</div>", unsafe_allow_html=True)
    st.markdown("---")
    b1, b2 = st.columns(2)
    with b1:
        if st.button("Schedule Data for all PWSs", use_container_width=True):
            handle_bulk_schedule_download("schedules")
        st.markdown("**Download full schedule export (tilde-delimited).**")
    with b2:
        if st.button(
            "Download Sampling Points Data for all PWSs", use_container_width=True
        ):
            handle_bulk_schedule_download("sampling_points")
        st.markdown("**Download full sampling-points export (tilde-delimited).**")

    return {
        "selected_system": selected_system,
        "selected_facility": selected_facility,
    }


def render_searchable_dropdown(label, options, key, placeholder="Select an option"):
    """Render a searchable dropdown with type-ahead functionality."""
    term = st.text_input(
        f"{label} (type to search)",
        placeholder=f"Type to search {placeholder.lower()}...",
        key=f"{key}_search",
    )
    if term:
        filtered = [opt for opt in options if term.lower() in opt.lower()][:50]
        if filtered:
            sel = st.selectbox(
                "Select from results:",
                [""] + filtered,
                key=f"{key}_filtered",
                format_func=lambda x: "Select..." if x == "" else x,
            )
            return sel or None
        else:
            st.warning("No results found.")
            return None
    st.info("Start typing to search...")
    return None


def get_all_water_systems(api_manager):
    """Get all water systems for dropdown with caching."""
    try:
        query = """
        SELECT DISTINCT pwsid,pws_name,city_name,
          CONCAT(pws_name,' (',pwsid,')') as display_name
        FROM sdwa_pub_water_systems
        WHERE pws_activity_code='A' AND pws_name<>''
        ORDER BY pws_name LIMIT 1000"""
        results = api_manager.sql_manager.execute_query(query)
        return [r["display_name"] for r in results]
    except Exception as e:
        st.error(f"Error loading water systems: {e}")
        return []


def get_facilities_for_system(api_manager, display_name):
    """Get facilities for a specific water system."""
    if not display_name:
        return []
    m = re.search(r"\(([^)]+)\)$", display_name)
    pwsid = m.group(1) if m else display_name
    try:
        query = f"""
        SELECT DISTINCT facility_id,facility_name,
          CONCAT(COALESCE(facility_name,'Facility'),' (',facility_id,')') as display_name
        FROM sdwa_facilities
        WHERE pwsid='{pwsid}' AND facility_activity_code='A'
        ORDER BY facility_name"""
        results = api_manager.sql_manager.execute_query(query)
        return [r["display_name"] for r in results]
    except Exception as e:
        st.error(f"Error loading facilities: {e}")
        return []


def handle_ccr_generation(api_manager, manual_entry, selected_system, year, fmt):
    """Fetch CCR data and render tables inline."""
    target = (manual_entry or selected_system or "").strip()
    if not target:
        st.error("Please select or enter a water system.")
        return
    m = re.search(r"\(([^)]+)\)$", target)
    pwsid = m.group(1) if m else target
    st.info(f"Loading CCR data for {pwsid} ({year})…")
    ccr = api_manager.sql_manager.get_ccr_data_for_system(pwsid, year)
    if ccr.get("error"):
        st.error("Failed to load CCR.")
        return
    sys = ccr["system_info"]
    st.subheader(f"{sys.get('pws_name','')} ({sys.get('pwsid','')})")
    st.markdown(
        f"**Type:** {sys.get('pws_type_code','N/A')} • **Pop:** {sys.get('population_served_count','N/A')}"
    )
    st.markdown(
        f"**Contact:** {sys.get('admin_name','')} | {sys.get('phone_number','')} | {sys.get('email_addr','')}"
    )
    viols = ccr.get("violations", [])
    if viols:
        st.markdown("### Violations")
        st.table(viols)
    else:
        st.success("✅ No violations for this year.")
    tests = ccr.get("test_results", [])
    if tests:
        st.markdown("### Test Results")
        st.table(tests)
    else:
        st.info("ℹ️ No test results for this year.")
    st.markdown(f"_Data generated on {ccr.get('generated_date','unknown')}_")


def handle_schedule_generation(api_manager, selected_system, selected_facility=None):
    """Fetch and render monitoring schedules."""
    if not selected_system:
        st.error("Please select a water system.")
        return
    m = re.search(r"\(([^)]+)\)$", selected_system)
    pwsid = m.group(1) if m else selected_system
    st.info(f"Loading schedules for {pwsid}…")
    schedules = api_manager.sql_manager.get_monitoring_schedules_for_system(pwsid)
    if selected_facility:
        fm = re.search(r"\(([^)]+)\)$", selected_facility)
        fac = fm.group(1) if fm else None
        schedules = [r for r in schedules if r.get("facility_id") == fac]
    if not schedules:
        st.success("✅ No schedule records found.")
        return
    st.subheader(f"Monitoring Schedules for {pwsid}")
    st.table(schedules)
    df = pd.DataFrame(schedules)
    csv = df.to_csv(index=False)
    st.download_button(
        "Download as CSV", data=csv, file_name=f"{pwsid}_schedules.csv", mime="text/csv"
    )


def handle_bulk_schedule_download(data_type):
    """Handle bulk data downloads."""
    if data_type == "schedules":
        st.success("📋 Preparing schedule data download...")
        st.info("Full tilde-delimited schedule export.")
    else:
        st.success("📍 Preparing sampling points download...")
        st.info("Full tilde-delimited sampling-points export.")


def show_glossary():
    """Show glossary of terms."""
    st.markdown("---")
    st.markdown("### 📖 Water Quality Glossary")
    terms = {
        "Community Water System (CWS)": "Serves same people year-round.",
        "Non-Community Water System (NCWS)": "Serves transient populations.",
        "Non-Transient Non-Community (NTNC)": "Serves same people regularly but not year-round.",
        "MCL": "Maximum Contaminant Level.",
        "Health-Based Violation": "Violation affecting public health.",
        "Lead and Copper Rule": "EPA regulation for lead/copper testing.",
        "Total Coliform Rule": "EPA regulation for coliform testing.",
        "Surface Water": "Rivers, lakes, reservoirs.",
        "Groundwater": "Wells and aquifers.",
        "Consumer Confidence Report (CCR)": "Annual water quality report.",
        "WSF": "Water System Facility (wells, tanks, plants).",
        "SDWIS": "Safe Drinking Water Information System.",
    }
    for term, definition in terms.items():
        with st.expander(f"**{term}**"):
            st.write(definition)


def show_county_map_info():
    """Show information about Georgia county map."""
    st.markdown("---")
    st.markdown("### 🗺️ Georgia County Map")
    st.info(
        "📍 **Tip:** Georgia has 159 counties. Use the dropdown above to filter by specific counties, or select 'All' to search statewide."
    )

    # Show some major counties for reference
    major_counties = [
        "FULTON (Atlanta area)",
        "GWINNETT (Lawrenceville area)",
        "COBB (Marietta area)",
        "DEKALB (Decatur area)",
        "CHATHAM (Savannah area)",
        "RICHMOND (Augusta area)",
        "MUSCOGEE (Columbus area)",
        "BIBB (Macon area)",
    ]

    st.markdown("**Major Georgia Counties:**")
    for county in major_counties:
        st.markdown(f"• {county}")


def show_system_card_enhanced(system: dict):
    """Show an enhanced water system card with more details."""
    st.markdown('<div class="result-card">', unsafe_allow_html=True)

    # System name and ID
    name = system.get("pws_name", "Unknown System")
    system_id = system.get("pwsid", "Unknown")

    col1, col2 = st.columns([3, 1])

    with col1:
        st.markdown(f"**🏢 {name}**")
        st.markdown(f"*System ID: {system_id}*")

    with col2:
        # System status indicator
        activity_code = system.get("pws_activity_code", "")
        if activity_code == "A":
            st.markdown("✅ **Active**")
        else:
            st.markdown("❌ **Inactive**")

    # System details
    details_col1, details_col2 = st.columns(2)

    with details_col1:
        # System type
        system_type = system.get("pws_type_code", "Unknown")
        friendly_type = {
            "CWS": "Community Water System",
            "TNCWS": "Transient Non-Community",
            "NTNCWS": "Non-Transient Non-Community",
        }.get(system_type, system_type)
        st.markdown(f"**Type:** {friendly_type}")

        # Location
        city = system.get("city_name", "Unknown")
        county = system.get("county_served", "Unknown")
        if city != "Unknown" or county != "Unknown":
            st.markdown(f"**Location:** {city}, {county} County")

        # Population served
        population = system.get("population_served_count")
        if population and population > 0:
            st.markdown(f"**Population Served:** {population:,}")

    with details_col2:
        # Source water type
        source_code = system.get("primary_source_code") or system.get("gw_sw_code")
        if source_code:
            source_friendly = {
                "GW": "Groundwater",
                "SW": "Surface Water",
                "GU": "Mixed Sources",
                "GWP": "Groundwater (Purchased)",
                "SWP": "Surface Water (Purchased)",
            }.get(source_code, source_code)
            st.markdown(f"**Water Source:** {source_friendly}")

        # Violation status
        violations = system.get("active_violations", 0)
        if violations == 0:
            st.markdown(
                '**Status:** <span class="status-good">✅ No current violations</span>',
                unsafe_allow_html=True,
            )
        elif violations <= 2:
            st.markdown(
                f'**Status:** <span class="status-warning">⚠️ {violations} violation(s)</span>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                f'**Status:** <span class="status-alert">🚨 {violations} violations</span>',
                unsafe_allow_html=True,
            )

    st.markdown("</div>", unsafe_allow_html=True)


def show_sample_result_card(sample: dict):
    """Show a sample result card."""
    st.markdown('<div class="result-card">', unsafe_allow_html=True)

    # Sample header
    sample_id = sample.get("sample_id", "Unknown")
    contaminant = sample.get("contaminant_code", "Unknown")

    st.markdown(f"**🧪 Sample ID:** {sample_id}")
    st.markdown(f"**🧬 Contaminant:** {contaminant}")

    # Sample details
    sample_col1, sample_col2 = st.columns(2)

    with sample_col1:
        # Sample date
        sample_date = sample.get("sampling_end_date") or sample.get(
            "non_compl_per_begin_date"
        )
        if sample_date:
            st.markdown(f"**Date:** {sample_date}")

        # Violation category
        violation_category = sample.get("violation_category_code")
        if violation_category:
            st.markdown(f"**Category:** {violation_category}")

    with sample_col2:
        # Health-based indicator
        is_health_based = sample.get("is_health_based_ind")
        if is_health_based == "Y":
            st.markdown("**⚠️ Health-Based Violation**")

        # Violation status
        status = sample.get("violation_status", "Unknown")
        status_color = {
            "Resolved": "status-good",
            "Unaddressed": "status-alert",
            "Addressed": "status-warning",
        }.get(status, "")

        if status_color:
            st.markdown(
                f'**Status:** <span class="{status_color}">{status}</span>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(f"**Status:** {status}")

    st.markdown("</div>", unsafe_allow_html=True)
