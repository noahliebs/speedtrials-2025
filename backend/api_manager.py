"""API Manager with Tool Calling for Georgia Water Quality Assistant."""

import logging
from dataclasses import dataclass
from typing import Any

from backend.chat_manager import ChatManager
from backend.sql_manager import SqlManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class ChatResponse:
    """Response from chat API with tool calling."""

    message: str
    sql_query: str | None = None
    sql_results: list[dict[str, Any]] | None = None
    sql_error: str | None = None
    session_id: str | None = None
    has_sql: bool = False


class ApiManager:
    """API Manager with tool calling for SQL execution."""

    def __init__(self, database_url: str, model_name: str = "gemini-1.5-flash"):
        """Initialize API Manager."""
        self.sql_manager = SqlManager(database_url)
        self.chat_manager = ChatManager(model_name)
        self._schema_cache: dict[str, Any] | None = None

    def get_schema(self) -> dict[str, Any]:
        """Get database schema with caching."""
        if self._schema_cache is None:
            self._schema_cache = self.sql_manager.get_schema_description()
        return self._schema_cache

    def create_session(self) -> str:
        """Create a new chat session."""
        return self.chat_manager.create_session()

    def _execute_sql_safely(self, query: str) -> list[dict[str, Any]]:
        """Execute SQL with error handling."""
        try:
            return self.sql_manager.execute_query(query)
        except Exception as e:
            # Log the error but don't expose technical details to users.
            print(f"SQL Error: {e}")
            raise Exception("Error retrieving data from database") from e

    async def chat(
        self,
        message: str,
        session_id: str | None = None,
        dev_mode: bool = False,
    ) -> ChatResponse:
        """Process chat message with tool calling."""

        # Create session if needed.
        if session_id is None:
            session_id = self.create_session()

        # Get schema.
        schema = self.get_schema()

        try:
            # Generate response using tool calling.
            (
                response_text,
                sql_query,
                sql_results,
            ) = await self.chat_manager.generate_response_with_tools(
                session_id=session_id,
                user_message=message,
                schema=schema,
                sql_executor=self._execute_sql_safely,
            )

            return ChatResponse(
                message=response_text,
                sql_query=sql_query,
                sql_results=sql_results,
                session_id=session_id,
                has_sql=sql_query is not None,
            )

        except Exception as e:
            logger.exception("Unexpected error in ApiManager.chat")
            error_msg = (
                "I encountered an error processing your request. Please try rephrasing"
                " your question."
            )

            # In dev mode, show more details.
            if dev_mode:
                error_msg += f" (Error: {str(e)})"

            return ChatResponse(
                message=error_msg,
                sql_error=str(e),
                session_id=session_id,
                has_sql=False,
            )

    def execute_sql(self, query: str) -> dict[str, Any]:
        """Execute a SQL query directly."""
        try:
            results = self.sql_manager.execute_query(query)
            return {"query": query, "result": results}
        except Exception as e:
            return {"query": query, "result": [], "error": str(e)}

    def get_session_history(self, session_id: str) -> list[dict[str, Any]]:
        """Get chat history for a session."""
        try:
            messages = self.chat_manager.get_session(session_id)
            return [
                {
                    "role": msg.role.value,
                    "content": msg.content,
                    "timestamp": msg.timestamp,
                }
                for msg in messages
            ]
        except ValueError:
            return []

    def clear_session(self, session_id: str) -> bool:
        """Clear a chat session."""
        try:
            self.chat_manager.clear_session(session_id)
            return True
        except Exception:
            return False

    def get_database_stats(self) -> dict[str, Any]:
        """Get database statistics."""
        try:
            schema = self.get_schema()
            stats = {
                "total_schemas": len(schema),
                "total_tables": sum(len(tables) for tables in schema.values()),
                "health_check": self.sql_manager.health_check(),
            }

            # Get table row counts for main tables.
            main_tables = [
                "sdwa_pub_water_systems",
                "sdwa_violations_enforcement",
                "sdwa_facilities",
                "sdwa_geographic_areas",
                "sdwa_site_visits",
            ]

            table_counts = {}
            for table in main_tables:
                try:
                    count = self.sql_manager.get_table_row_count(table)
                    table_counts[table] = count
                except Exception:
                    table_counts[table] = "Error"

            stats["table_counts"] = table_counts
            return stats

        except Exception as e:
            return {"error": str(e), "health_check": False}

    def search_systems(
        self,
        city: str | None = None,
        county: str | None = None,
        system_name: str | None = None,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """Search for water systems."""
        try:
            # Build query with direct string substitution to avoid parameter issues.
            query_parts = [
                "SELECT DISTINCT pws.pwsid, pws.pws_name, pws.city_name,",
                "geo.county_served, pws.population_served_count, pws.pws_type_code,",
                "COALESCE(v.violation_count, 0) as active_violations",
                "FROM sdwa_pub_water_systems pws",
                "LEFT JOIN sdwa_geographic_areas geo ON pws.pwsid = geo.pwsid",
                "LEFT JOIN (",
                "  SELECT pwsid, COUNT(*) as violation_count",
                "  FROM sdwa_violations_enforcement",
                "  WHERE violation_status = 'Unaddressed'",
                "  GROUP BY pwsid",
                ") v ON pws.pwsid = v.pwsid",
                "WHERE pws.pws_activity_code = 'A'",
            ]

            if city:
                # Escape single quotes to prevent SQL injection.
                escaped_city = city.replace("'", "''")
                query_parts.append(f"AND pws.city_name ILIKE '%{escaped_city}%'")

            if county:
                escaped_county = county.replace("'", "''")
                query_parts.append(f"AND geo.county_served ILIKE '%{escaped_county}%'")

            if system_name:
                escaped_name = system_name.replace("'", "''")
                query_parts.append(f"AND pws.pws_name ILIKE '%{escaped_name}%'")

            query_parts.append(f"ORDER BY pws.pws_name LIMIT {limit}")

            query = " ".join(query_parts)
            return self.sql_manager.execute_query(query)

        except Exception as e:
            return [{"error": str(e)}]

    def get_search_metadata(self) -> dict[str, Any]:
        """Get metadata for search form dropdowns."""
        try:
            metadata = {
                "system_types": [
                    {
                        "code": "CWS",
                        "name": "Community Water System",
                        "description": "Serves the same people year-round",
                    },
                    {
                        "code": "TNCWS",
                        "name": "Transient Non-Community",
                        "description": "Serves different people temporarily",
                    },
                    {
                        "code": "NTNCWS",
                        "name": "Non-Transient Non-Community",
                        "description": "Serves same people regularly but not year-round",
                    },
                ],
                "source_types": self.sql_manager.get_source_water_types(),
                "sample_classes": [
                    {
                        "name": "Lead & Copper",
                        "description": "Lead and Copper Rule violations",
                    },
                    {
                        "name": "Bacteria/Coliform",
                        "description": "Total Coliform Rule violations",
                    },
                    {
                        "name": "Chemical",
                        "description": "Maximum Contaminant Level violations",
                    },
                    {
                        "name": "Radiological",
                        "description": "Radioactive contamination",
                    },
                ],
                "contaminants": self.sql_manager.get_contaminant_types()[:20],  # Top 20
            }

            return metadata
        except Exception as e:
            return {
                "error": str(e),
                "system_types": [],
                "source_types": [],
                "sample_classes": [],
                "contaminants": [],
            }

    def search_systems_enhanced(
        self,
        system_id: str | None = None,
        city: str | None = None,
        county: str | None = None,
        system_name: str | None = None,
        system_type: str | None = None,
        source_type: str | None = None,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """Enhanced search for water systems with multiple criteria."""
        try:
            # If we have the enhanced method in sql_manager, use it
            if hasattr(self.sql_manager, "search_systems_enhanced"):
                return self.sql_manager.search_systems_enhanced(
                    system_id=system_id,
                    city=city,
                    county=county,
                    system_name=system_name,
                    system_type=system_type,
                    source_type=source_type,
                    limit=limit,
                )
            else:
                # Fall back to existing search_systems method
                return self.search_systems(
                    city=city,
                    county=county,
                    system_name=system_name,
                    limit=limit,
                )
        except Exception as e:
            return [{"error": str(e)}]

    def search_systems_with_samples(
        self,
        sample_class: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        city: str | None = None,
        county: str | None = None,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """Search for water systems with sample/violation data."""
        try:
            # If we have the enhanced method in sql_manager, use it
            if hasattr(self.sql_manager, "search_systems_with_samples"):
                return self.sql_manager.search_systems_with_samples(
                    sample_class=sample_class,
                    start_date=start_date,
                    end_date=end_date,
                    city=city,
                    county=county,
                    limit=limit,
                )
            else:
                # Fall back to chat-based search
                return [{"error": "Sample search not implemented, use chat interface"}]
        except Exception as e:
            return [{"error": str(e)}]
