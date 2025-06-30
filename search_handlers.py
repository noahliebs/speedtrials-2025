"""Search handlers for Georgia Water Quality System."""

import streamlit as st


def handle_water_system_search(search_params, api_manager, ask_question_func):
    """Handle water system search with enhanced parameters."""

    # Try direct SQL search using existing search_systems method
    try:
        # Use existing search_systems method from api_manager
        results = api_manager.search_systems(
            city=search_params.get("city")
            if search_params.get("city") != "All"
            else None,
            county=search_params.get("county")
            if search_params.get("county") != "All"
            else None,
            system_name=search_params.get("system_name"),
            limit=50,
        )

        # Filter results by system_id if provided
        if search_params.get("system_id") and results:
            system_id = search_params["system_id"].upper()
            if not system_id.startswith("GA"):
                system_id = "GA" + system_id
            results = [r for r in results if r.get("pwsid") == system_id]

        # Display results directly without switching to chat
        if results and not results[0].get("error"):
            st.success(f"✅ Found {len(results)} water systems")

            # Import here to avoid circular import
            import ui_components

            for system in results:
                ui_components.show_system_card_enhanced(system)

        elif results and results[0].get("error"):
            # SQL failed, fall back to LLM
            st.warning("Direct search failed, using AI assistant...")
            query = build_natural_language_query(search_params)
            st.session_state.current_view = "chat"
            ask_question_func(query)
            st.rerun()
        else:
            st.warning("No water systems found matching your criteria.")

            # Show helpful suggestions
            st.markdown("### 💡 Try these suggestions:")
            st.markdown(
                "• **Major Georgia cities**: Atlanta, Augusta, Columbus, Savannah, Macon"
            )
            st.markdown(
                "• **Major counties**: Fulton, Gwinnett, Cobb, DeKalb, Cherokee"
            )
            st.markdown("• **Check spelling** and try partial names")

    except Exception as e:
        # SQL failed, fall back to LLM
        st.warning(f"Direct search failed ({str(e)}), using AI assistant...")
        query = build_natural_language_query(search_params)
        st.session_state.current_view = "chat"
        ask_question_func(query)
        st.rerun()


def handle_sample_search(search_params, api_manager, ask_question_func):
    """Handle sample search - use LLM for now since it's complex."""

    st.info("🧪 Sample searches use our AI assistant for best results...")

    # Build sample-specific query and go to chat
    query = build_sample_query(search_params)
    st.session_state.current_view = "chat"
    ask_question_func(query)
    st.rerun()


def handle_schedule_lookup(search_params, api_manager, ask_question_func):
    """Handle schedule lookup and sampling points."""

    query = "Show me sampling schedules and monitoring requirements"

    if search_params.get("county"):
        query += f" for {search_params['county']} County"

    if search_params.get("system_name"):
        query += f" for systems containing '{search_params['system_name']}'"

    # Switch to chat view
    st.session_state.current_view = "chat"
    ask_question_func(query)
    st.rerun()


def handle_consumer_confidence(search_params, api_manager, ask_question_func):
    """Handle consumer confidence report data."""

    query = "Show me consumer confidence report information and annual water quality summaries"

    if search_params.get("county"):
        query += f" for {search_params['county']} County"

    if search_params.get("system_name"):
        query += f" for systems containing '{search_params['system_name']}'"

    # Switch to chat view
    st.session_state.current_view = "chat"
    ask_question_func(query)
    st.rerun()


def convert_search_params_to_db(search_params):
    """Convert UI search parameters to database parameters."""

    db_params = {}

    # System ID - add GA prefix if missing
    if search_params.get("system_id"):
        system_id = search_params["system_id"].upper()
        if not system_id.startswith("GA"):
            system_id = "GA" + system_id
        db_params["system_id"] = system_id

    # Direct mappings
    direct_mappings = ["system_name", "county"]
    for param in direct_mappings:
        if search_params.get(param):
            db_params[param] = search_params[param]

    # Water system type mapping
    if search_params.get("water_system_type"):
        system_type_map = {
            "Community": "CWS",
            "Non-Community": "TNCWS",
            "Non-Transient Non-Community": "NTNCWS",
        }
        db_params["system_type"] = system_type_map.get(
            search_params["water_system_type"], search_params["water_system_type"]
        )

    # Source water type mapping
    if search_params.get("source_water_type"):
        source_map = {
            "GroundWater": "GW",
            "GroundWater Purchased": "GWP",
            "GroundWater UDI SurfaceWater": "GU",
            "GroundWater UDI SurfaceWater Purchased": "GUP",
            "SurfaceWater": "SW",
            "SurfaceWater Purchased": "SWP",
        }
        db_params["source_type"] = source_map.get(
            search_params["source_water_type"], search_params["source_water_type"]
        )

    return db_params


def build_natural_language_query(search_params):
    """Build natural language query from search parameters."""

    query_parts = ["Find water systems"]

    # System ID
    if search_params.get("system_id"):
        system_id = search_params["system_id"].upper()
        if not system_id.startswith("GA"):
            system_id = "GA" + system_id
        query_parts.append(f"with system ID {system_id}")

    # System name
    if search_params.get("system_name"):
        query_parts.append(f"with name containing '{search_params['system_name']}'")

    # Location
    if search_params.get("county"):
        query_parts.append(f"in {search_params['county']} County")

    # System type
    if (
        search_params.get("water_system_type")
        and search_params["water_system_type"] != "All"
    ):
        type_desc = {
            "Community": "community water systems (CWS)",
            "Non-Community": "transient non-community systems (TNCWS)",
            "Non-Transient Non-Community": "non-transient non-community systems (NTNCWS)",
        }.get(search_params["water_system_type"], search_params["water_system_type"])
        query_parts.append(f"that are {type_desc}")

    # Source water type
    if (
        search_params.get("source_water_type")
        and search_params["source_water_type"] != "All"
    ):
        source_desc = {
            "GroundWater": "using groundwater",
            "GroundWater Purchased": "using purchased groundwater",
            "GroundWater UDI SurfaceWater": "using mixed groundwater and surface water",
            "GroundWater UDI SurfaceWater Purchased": "using purchased mixed water sources",
            "SurfaceWater": "using surface water",
            "SurfaceWater Purchased": "using purchased surface water",
        }.get(
            search_params["source_water_type"],
            f"using {search_params['source_water_type'].lower()}",
        )
        query_parts.append(source_desc)

    # Contact type
    if search_params.get("contact_type") and search_params["contact_type"] != "None":
        contact_desc = {
            "Administrative Contact": "with administrative contacts",
            "Emergency Contact": "with emergency contacts",
            "Owner": "with owner contacts",
            "Lead Engineer": "with engineering contacts",
            "Operator": "with operator contacts",
            "Financial Contact": "with financial contacts",
            "Legal Contact": "with legal contacts",
            "Designated Op in Charge": "with designated operators",
            "Sampling": "with sampling contacts",
        }.get(
            search_params["contact_type"],
            f"with {search_params['contact_type'].lower()} contacts",
        )
        query_parts.append(contact_desc)

    return " ".join(query_parts)


def build_sample_query(search_params):
    """Build sample-specific query from search parameters."""

    query_parts = ["Find water quality samples"]

    # Sample class
    if search_params.get("sample_class"):
        sample_desc = {
            "Coliform/Microbial Samples": "coliform and microbial samples",
            "Coliform/Microbial Samples Detection Only": "coliform detection results",
            "Coliform Summaries": "coliform summary data",
            "Lead and Copper Summaries": "lead and copper samples",
            "Summarized Field Sample Results": "field sample summaries",
            "Chem/Rad Samples All": "chemical and radiological samples",
            "Samples by Analyte": "samples grouped by analyte",
            "Chem/Rad Samples Detection Only": "chemical and radiological detections",
        }.get(search_params["sample_class"], search_params["sample_class"].lower())
        query_parts.append(f"for {sample_desc}")

    # Date range
    if search_params.get("start_date") and search_params.get("end_date"):
        query_parts.append(
            f"between {search_params['start_date']} and {search_params['end_date']}"
        )

    # Location filters
    if search_params.get("county"):
        query_parts.append(f"in {search_params['county']} County")

    if search_params.get("system_name"):
        query_parts.append(f"from systems containing '{search_params['system_name']}'")

    # System type filter
    if (
        search_params.get("water_system_type")
        and search_params["water_system_type"] != "All"
    ):
        type_desc = {
            "Community": "community water systems",
            "Non-Community": "non-community systems",
            "Non-Transient Non-Community": "non-transient non-community systems",
        }.get(search_params["water_system_type"], search_params["water_system_type"])
        query_parts.append(f"from {type_desc}")

    return " ".join(query_parts)


def validate_search_params(search_params):
    """Validate search parameters before processing."""

    errors = []
    warnings = []

    # Check if at least one search criteria is provided
    search_fields = [
        "system_id",
        "system_name",
        "county",
        "water_system_type",
        "source_water_type",
        "contact_type",
        "sample_class",
    ]

    has_criteria = any(
        search_params.get(field)
        and search_params[field] not in ["All", "None", "Click to select a value..."]
        for field in search_fields
    )

    if not has_criteria:
        warnings.append(
            "No search criteria specified. This will return all active water systems."
        )

    # Validate system ID format
    if search_params.get("system_id"):
        system_id = search_params["system_id"].upper()
        if not system_id.startswith("GA"):
            system_id = "GA" + system_id

        # Check if it's a valid format (GA + 7 digits)
        if len(system_id) != 9 or not system_id[2:].isdigit():
            warnings.append(
                "Water System ID should be in format GA1234567 (GA + 7 digits)"
            )

    # Validate date range
    if search_params.get("start_date") and search_params.get("end_date"):
        if search_params["start_date"] > search_params["end_date"]:
            errors.append("Start date must be before end date")

    # Check for sample search without sample class
    if (
        search_params.get("start_date") or search_params.get("end_date")
    ) and not search_params.get("sample_class"):
        warnings.append("Date range specified but no sample class selected")

    return errors, warnings


def format_search_summary(search_params):
    """Format a human-readable summary of search parameters."""

    summary_parts = []

    if search_params.get("system_id"):
        summary_parts.append(f"System ID: {search_params['system_id']}")

    if search_params.get("system_name"):
        summary_parts.append(f"System Name: {search_params['system_name']}")

    if search_params.get("county"):
        summary_parts.append(f"County: {search_params['county']}")

    if (
        search_params.get("water_system_type")
        and search_params["water_system_type"] != "All"
    ):
        summary_parts.append(f"Type: {search_params['water_system_type']}")

    if (
        search_params.get("source_water_type")
        and search_params["source_water_type"] != "All"
    ):
        summary_parts.append(f"Source: {search_params['source_water_type']}")

    if search_params.get("sample_class"):
        summary_parts.append(f"Samples: {search_params['sample_class']}")

    if search_params.get("start_date") and search_params.get("end_date"):
        summary_parts.append(
            f"Dates: {search_params['start_date']} to {search_params['end_date']}"
        )

    if not summary_parts:
        return "All active water systems"

    return " | ".join(summary_parts)
