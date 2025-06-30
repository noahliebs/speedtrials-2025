"""Georgia Water Quality Assistant - Enhanced Conversational Interface."""

import asyncio
import os

import nest_asyncio
import pandas as pd
import streamlit as st

import search_handlers
import ui_components
from backend.api_manager import ApiManager


def setup_asyncio():
    """Setup asyncio for Streamlit compatibility."""
    try:
        # Try to get the current event loop
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            # Create a new event loop if closed
            asyncio.set_event_loop(asyncio.new_event_loop())
    except RuntimeError:
        # No event loop exists, create one
        asyncio.set_event_loop(asyncio.new_event_loop())

    # Allow nested event loops (needed for Streamlit)
    nest_asyncio.apply()


# Call this before any async operations
setup_asyncio()

# Configuration
DATABASE_URL = "postgresql://noahlieberman@localhost:5432/sdwis_georgia"

# Page configuration
st.set_page_config(
    page_title="🌊 Georgia Water Quality",
    page_icon="🌊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Check for dev mode (after page config) - Fixed duplicate element ID
DEV_MODE = os.getenv("DEV_MODE", "false").lower() == "true"
if not DEV_MODE:
    # Only show checkbox if not already set via environment variable
    DEV_MODE = st.sidebar.checkbox(
        "🛠 Developer Mode", value=False, key="dev_mode_toggle"
    )

# Custom CSS for cleaner look
st.markdown(
    """
<style>
    .main-header {
        text-align: center;
        padding: 2rem 0;
        background: linear-gradient(90deg, #1e3a8a 0%, #3b82f6 100%);
        color: white;
        border-radius: 10px;
        margin-bottom: 2rem;
    }

    .search-box {
        background: #f8fafc;
        padding: 1.5rem;
        border-radius: 10px;
        border: 1px solid #e2e8f0;
        margin-bottom: 1rem;
    }

    .quick-action {
        background: white;
        padding: 1rem;
        border-radius: 8px;
        border: 1px solid #e2e8f0;
        text-align: center;
        cursor: pointer;
        transition: all 0.2s;
    }

    .quick-action:hover {
        border-color: #3b82f6;
        box-shadow: 0 2px 8px rgba(59, 130, 246, 0.1);
    }

    .result-card {
        background: white;
        padding: 1.5rem;
        border-radius: 10px;
        border: 1px solid #e2e8f0;
        margin-bottom: 1rem;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
    }

    .chat-message {
        padding: 1rem;
        margin: 0.5rem 0;
        border-radius: 10px;
        max-width: 100%;
    }

    .user-message {
        background: #e3f2fd;
        border-left: 4px solid #1976d2;
    }

    .assistant-message {
        background: #f3e5f5;
        border-left: 4px solid #7b1fa2;
    }

    .chat-container {
        max-height: 600px;
        overflow-y: auto;
        border: 1px solid #e2e8f0;
        border-radius: 10px;
        padding: 1rem;
        background: #fafafa;
    }

    .status-good { color: #059669; font-weight: bold; }
    .status-warning { color: #d97706; font-weight: bold; }
    .status-alert { color: #dc2626; font-weight: bold; }

    /* Hide technical elements in consumer mode */
    .dev-only {
        display: none;
    }

    .stButton > button {
        width: 100%;
    }
</style>
""",
    unsafe_allow_html=True,
)


@st.cache_resource
def get_api_manager():
    """Initialize and cache API manager."""
    return ApiManager(DATABASE_URL)


# Initialize session state
if "session_id" not in st.session_state:
    api_manager = get_api_manager()
    st.session_state.session_id = api_manager.create_session()

if "messages" not in st.session_state:
    st.session_state.messages = []

if "current_view" not in st.session_state:
    st.session_state.current_view = "chat"

if "show_quick_search" not in st.session_state:
    st.session_state.show_quick_search = True


def show_header():
    """Show main header."""
    st.markdown(
        """
    <div class="main-header">
        <h1>🌊 Georgia Water Quality</h1>
        <p style="margin: 0; opacity: 0.9;">Find information about your drinking water system</p>
    </div>
    """,
        unsafe_allow_html=True,
    )


def show_view_toggle():
    """Show view toggle buttons."""
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        if st.button("🔍 Quick Search", use_container_width=True, key="nav_search"):
            st.session_state.current_view = "search"
            st.rerun()

    with col2:
        if st.button("💬 Chat", use_container_width=True, key="nav_chat"):
            st.session_state.current_view = "chat"
            st.rerun()

    with col3:
        if st.button("📊 Stats", use_container_width=True, key="nav_stats"):
            st.session_state.current_view = "stats"
            st.rerun()

    with col4:
        if st.button("📄 CCR Reports", use_container_width=True, key="nav_ccr"):
            st.session_state.current_view = "ccr"
            st.rerun()

    with col5:
        if st.button("📅 Schedules", use_container_width=True, key="nav_schedule"):
            st.session_state.current_view = "schedule"
            st.rerun()


def show_search_interface():
    """Show the enhanced search interface using modular components."""

    # Render the search interface and get parameters
    search_params = ui_components.render_enhanced_search_interface()

    # Get API manager
    api_manager = get_api_manager()

    # Handle search actions
    if search_params["actions"]["search_systems"]:
        # Validate parameters
        errors, warnings = search_handlers.validate_search_params(search_params)

        if errors:
            for error in errors:
                st.error(f"❌ {error}")
            return

        if warnings:
            for warning in warnings:
                st.warning(f"⚠️ {warning}")

        # Show search summary
        summary = search_handlers.format_search_summary(search_params)
        st.info(f"🔍 Searching for: {summary}")

        # Handle the search
        search_handlers.handle_water_system_search(
            search_params, api_manager, ask_question
        )

    elif search_params["actions"]["search_samples"]:
        # Validate sample search
        if not search_params.get("sample_class"):
            st.error("❌ Please select a sample class before searching for samples")
            return

        errors, warnings = search_handlers.validate_search_params(search_params)

        if errors:
            for error in errors:
                st.error(f"❌ {error}")
            return

        if warnings:
            for warning in warnings:
                st.warning(f"⚠️ {warning}")

        # Show search summary
        summary = search_handlers.format_search_summary(search_params)
        st.info(f"🧪 Searching for samples: {summary}")

        # Handle the search
        search_handlers.handle_sample_search(search_params, api_manager, ask_question)

    elif search_params["actions"]["schedule_lookup"]:
        st.info("📅 Looking up sampling schedules...")
        search_handlers.handle_schedule_lookup(search_params, api_manager, ask_question)

    elif search_params["actions"]["consumer_confidence"]:
        st.info("📄 Retrieving consumer confidence data...")
        search_handlers.handle_consumer_confidence(
            search_params, api_manager, ask_question
        )


def show_ccr_interface():
    """Show the Consumer Confidence Report interface."""
    api_manager = get_api_manager()
    ui_components.render_ccr_interface(api_manager)


def show_schedule_interface():
    """Show the Schedule Lookup interface."""
    api_manager = get_api_manager()
    ui_components.render_schedule_interface(api_manager)


def show_chat_interface():
    """Show the main chat interface."""
    st.subheader("💬 Chat with Georgia Water Quality Assistant")

    # Initialize input key counter for clearing
    if "input_key_counter" not in st.session_state:
        st.session_state.input_key_counter = 0

    # Chat input at the top for better UX
    question = st.text_input(
        "Ask me anything about Georgia's water quality:",
        placeholder="e.g., 'What water systems serve Atlanta?' or 'Show me recent violations'",
        key=f"chat_input_{st.session_state.input_key_counter}",  # Dynamic key for clearing
    )

    col1, col2 = st.columns([3, 1])
    with col1:
        if st.button(
            "Send", type="primary", use_container_width=True, key="send_message"
        ):
            if question.strip():
                ask_question(question)
                # Increment key counter to create new input field (clears the old one)
                st.session_state.input_key_counter += 1
                st.rerun()
            else:
                st.warning("Please enter a question.")

    with col2:
        if st.button("Clear Chat", use_container_width=True, key="clear_chat"):
            clear_conversation()
            st.rerun()

    # Show conversation history
    show_conversation_history()

    # Quick action suggestions if no conversation yet
    if not st.session_state.messages:
        st.markdown("---")
        show_quick_actions()


def show_stats_view():
    """Show statistics and quick facts."""
    st.subheader("📊 Georgia Water Quality Statistics")
    show_quick_stats()

    # Add some helpful context
    st.markdown("---")
    st.markdown("### 📚 Understanding Water System Types")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("""
        **🏘️ Community Water Systems (CWS)**
        - Serve the same people year-round
        - Includes residential areas, towns, cities
        - Subject to all drinking water regulations
        """)

    with col2:
        st.markdown("""
        **🚛 Transient Non-Community (TNCWS)**
        - Serve different people temporarily
        - Includes gas stations, restaurants, campgrounds
        - Limited monitoring requirements
        """)

    with col3:
        st.markdown("""
        **🏢 Non-Transient Non-Community (NTNCWS)**
        - Serve same people regularly but not year-round
        - Includes schools, offices, factories
        - More monitoring than transient systems
        """)

    st.markdown("---")
    st.markdown("### ⚠️ Understanding Violations")
    st.markdown("""
    - **Health-based violations**: Exceed maximum contaminant levels (MCLs) or treatment technique requirements
    - **Monitoring violations**: Failed to test water as required
    - **Reporting violations**: Failed to report test results to regulators
    - **Public notification violations**: Failed to notify customers of problems
    """)


def ask_question(question: str):
    """Ask a question with simplified handling and retry capability."""
    api_manager = get_api_manager()

    # Add to chat history
    st.session_state.messages.append({"role": "user", "content": question})

    with st.spinner("🤖"):
        try:
            loop = asyncio.get_event_loop()
            response = loop.run_until_complete(
                api_manager.chat(
                    question,
                    st.session_state.session_id,
                    dev_mode=DEV_MODE,
                )
            )
        except Exception as e:
            error_msg = f"Error processing question: {str(e)}"
            st.session_state.messages.append(
                {"role": "assistant", "content": error_msg}
            )
            return

    # Add response to chat (no cleaning needed!)
    st.session_state.messages.append({"role": "assistant", "content": response.message})

    # Store SQL results for table display
    if response.sql_results:
        st.session_state.last_sql_results = response.sql_results
    elif hasattr(st.session_state, "last_sql_results"):
        delattr(st.session_state, "last_sql_results")

    # Dev details if needed
    if DEV_MODE:
        show_dev_details(response)


def clear_conversation():
    """Clear the conversation history."""
    api_manager = get_api_manager()
    api_manager.clear_session(st.session_state.session_id)
    st.session_state.messages = []
    if hasattr(st.session_state, "last_sql_results"):
        delattr(st.session_state, "last_sql_results")
    # Increment input key counter to clear the input field
    st.session_state.input_key_counter += 1
    st.success("🔄 Conversation cleared!")


def show_quick_actions():
    """Show quick action buttons."""
    st.subheader("🎯 Quick Questions")

    col1, col2 = st.columns(2)

    with col1:
        if st.button(
            "🏙️ Major Cities Water Systems", use_container_width=True, key="quick_cities"
        ):
            ask_question(
                "Show me water systems in major Georgia cities like Atlanta, Augusta, Columbus, and Savannah"
            )
            st.rerun()

        if st.button("🔍 Lead Issues", use_container_width=True, key="quick_lead"):
            ask_question("Show me water systems with lead violations")
            st.rerun()

    with col2:
        if st.button("⚠️ Health Alerts", use_container_width=True, key="quick_health"):
            ask_question("What are the current health violations in Georgia?")
            st.rerun()

        if st.button(
            "📊 System Overview", use_container_width=True, key="quick_overview"
        ):
            ask_question(
                "Give me an overview of Georgia's water systems and their status"
            )
            st.rerun()


def show_quick_stats():
    """Show quick statistics."""
    api_manager = get_api_manager()

    try:
        # Use the improved database stats method
        stats = api_manager.get_database_stats()

        if stats.get("health_check", False):
            st.markdown("### 📊 Georgia Water Quality at a Glance")

            col1, col2, col3, col4 = st.columns(4)

            with col1:
                total_systems = stats.get("total_systems", 0)
                st.metric("Water Systems", f"{total_systems:,}")

            with col2:
                total_pop = stats.get("total_population_served", 0)
                if total_pop and total_pop >= 1000000:
                    pop_display = f"{total_pop/1000000:.1f}M"
                elif total_pop:
                    pop_display = f"{total_pop:,}"
                else:
                    pop_display = "N/A"
                st.metric("People Served", pop_display)

            with col3:
                health_violations = stats.get("health_violations", 0)
                st.metric("Health Violations", f"{health_violations:,}")

            with col4:
                unaddressed = stats.get("unaddressed_violations", 0)
                st.metric("Unresolved Issues", f"{unaddressed:,}")

    except Exception as e:
        if DEV_MODE:
            st.error(f"Error loading stats: {e}")
        # Silently fail in consumer mode


def show_results_table(data: list[dict]):
    """Display SQL results as a clean table."""
    if not data:
        return

    # Create DataFrame
    df = pd.DataFrame(data)

    # Clean up column names for display
    df.columns = [
        col.replace("_", " ").replace("pws ", "").title() for col in df.columns
    ]

    # Show as interactive table
    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        height=min(400, len(df) * 35 + 50),  # Dynamic height
    )

    # Show count
    st.caption(f"Showing {len(data)} results")


def show_conversation_history():
    """Show conversation with separate data display."""
    if not st.session_state.messages:
        st.info(
            "👋 Welcome! Ask me anything about Georgia's water quality data. "
            "I can help you find water systems, check for violations, or answer questions about drinking water safety."
        )
        return

    st.markdown("### Conversation")

    for i, message in enumerate(st.session_state.messages):
        if message["role"] == "user":
            st.markdown(
                f'<div class="chat-message user-message">'
                f'<strong>👤 You:</strong><br/>{message["content"]}'
                f'</div>',
                unsafe_allow_html=True,
            )
        else:
            # Show the LLM response as-is (no post-processing needed!)
            st.markdown(
                f'<div class="chat-message assistant-message">'
                f"<strong>🤖 Assistant:</strong><br/>{message['content']}"
                f"</div>",
                unsafe_allow_html=True,
            )

            # Show data table if this is the most recent response with results
            if (
                i == len(st.session_state.messages) - 1
                and hasattr(st.session_state, "last_sql_results")
                and st.session_state.last_sql_results
            ):
                st.markdown("---")
                st.markdown("**📊 Data Results:**")
                show_results_table(st.session_state.last_sql_results)


def show_system_card(system: dict):
    """Show a water system card using enhanced UI components."""
    ui_components.show_system_card_enhanced(system)


def show_search_results(results: list[dict], result_type: str = "systems"):
    """Show search results with enhanced formatting."""

    if not results:
        st.warning("No results found for your search criteria.")
        return

    if results[0].get("error"):
        st.error(f"Search error: {results[0]['error']}")
        return

    # Show results count
    st.success(f"✅ Found {len(results)} {result_type}")

    # Display results based on type
    if result_type == "systems":
        for system in results:
            show_system_card(system)
    elif result_type == "samples":
        for sample in results:
            ui_components.show_sample_result_card(sample)
    else:
        # Generic table display
        show_results_table(results)


def show_dev_details(response):
    """Show developer details (only in dev mode)."""
    if not DEV_MODE:
        return

    st.markdown("---")
    st.markdown("### 🛠 Developer Details")

    # SQL Query
    if response.sql_query:
        with st.expander("🔍 SQL Query"):
            st.code(response.sql_query, language="sql")

    # Raw Results
    if response.sql_results:
        with st.expander("📊 Raw Data"):
            st.json(response.sql_results[:3])  # Limit to first 3 rows

    # Error details
    if response.sql_error:
        with st.expander("❌ SQL Error"):
            st.error(response.sql_error)

    # Show conversation flow (tool calls, errors, retries)
    api_manager = get_api_manager()
    if hasattr(api_manager.chat_manager, "_build_conversation_contents"):
        try:
            # Get the conversation contents to show tool flow
            session_messages = api_manager.chat_manager.get_session(
                st.session_state.session_id
            )

            if len(session_messages) > 2:  # More than just initial schema exchange
                with st.expander("🔄 Conversation Flow (Tool Calls & Retries)"):
                    st.markdown("**Recent conversation with LLM:**")

                    # Show last few messages to see tool calls and errors
                    for i, msg in enumerate(
                        session_messages[-6:], 1
                    ):  # Last 6 messages
                        role_icon = {"user": "👤", "model": "🤖", "system": "⚙️"}.get(
                            msg.role.value, "❓"
                        )
                        st.markdown(f"**{role_icon} {msg.role.value.title()}:**")

                        # Truncate long content for readability
                        content = msg.content
                        if len(content) > 500:
                            content = content[:500] + "..."

                        st.code(content, language="text")

                        if i < 6:  # Don't add separator after last message
                            st.markdown("---")
        except Exception as e:
            st.error(f"Error showing conversation flow: {e}")


def show_sidebar():
    """Show sidebar with dev mode toggle always visible."""
    with st.sidebar:
        st.header("🛠 Controls")

        # Dev mode status
        if DEV_MODE:
            st.success("🛠 Developer Mode: ON")
            st.caption("Showing SQL queries and raw data")
        else:
            st.info("👤 Consumer Mode: ON")
            st.caption("Showing user-friendly responses")

        st.markdown("---")

        # View controls
        st.subheader("Navigation")
        current_view_display = {
            "search": "🔍 Quick Search",
            "chat": "💬 Chat",
            "stats": "📊 Statistics",
            "ccr": "📄 CCR Reports",
            "schedule": "📅 Schedules",
        }

        st.info(
            f"Current: {current_view_display.get(st.session_state.current_view, 'Unknown')}"
        )

        if not DEV_MODE:
            st.markdown("---")
            st.markdown("**Quick Help:**")
            st.markdown("• Try searching by city name")
            st.markdown("• Use the chat for complex questions")
            st.markdown("• Check major cities like 'Atlanta'")
            return

        st.markdown("---")

        # Database status (dev mode only)
        st.subheader("Database Status")
        api_manager = get_api_manager()
        stats = api_manager.get_database_stats()

        if stats.get("health_check", False):
            st.success("✅ Connected")
            if "table_counts" in stats:
                for table, count in list(stats["table_counts"].items())[:3]:
                    clean_name = table.replace("sdwa_", "").replace("_", " ").title()
                    st.metric(
                        clean_name, f"{count:,}" if isinstance(count, int) else count
                    )
        else:
            st.error("❌ Connection Failed")

        # Session controls
        st.subheader("Session")
        if st.button("Clear Chat", key="sidebar_clear_chat"):
            clear_conversation()
            st.rerun()

        # Debug info
        st.subheader("Debug Info")
        st.code(f"Session ID: {st.session_state.session_id[:8]}...")
        st.code(f"Messages: {len(st.session_state.messages)}")


def main():
    """Main application."""
    show_header()
    show_sidebar()

    # View toggle
    show_view_toggle()

    st.markdown("---")

    # Main content based on current view
    if st.session_state.current_view == "search":
        show_search_interface()
    elif st.session_state.current_view == "chat":
        show_chat_interface()
    elif st.session_state.current_view == "stats":
        show_stats_view()
    elif st.session_state.current_view == "ccr":
        show_ccr_interface()
    elif st.session_state.current_view == "schedule":
        show_schedule_interface()

    # Footer
    st.markdown("---")
    st.markdown(
        """
    <div style="text-align: center; color: #64748b; padding: 1rem;">
        <small>
            Data from Georgia Environmental Protection Division • Updated Q1 2025<br>
            For emergencies, contact your local water utility immediately
        </small>
    </div>
    """,
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
