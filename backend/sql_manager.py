"""SQL Manager for Georgia Water Quality Database with Logging and Predefined Queries."""

import logging
import re
from typing import Any

from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MAX_ROWS = 100


class SqlManager:
    """SQL Manager for database operations with detailed logging and predefined queries."""

    def __init__(self, database_url: str):
        """Initialize SQL Manager.

        Args:
            database_url: PostgreSQL connection string.
        """
        logger.info("🔌 Initializing SqlManager")
        logger.info(f"Database URL: {database_url}")

        self.database_url = database_url
        self.engine = create_engine(database_url)
        self.Session = sessionmaker(bind=self.engine)
        self._schema_cache: dict[str, Any] | None = None

        logger.info("✅ SqlManager initialized successfully")

    def _validate_query(self, query: str) -> str:
        """Validate and modify query for safety.

        Args:
            query: SQL query to validate.

        Returns:
            Modified query with safety checks.

        Raises:
            ValueError: If query contains forbidden operations.
        """
        # Check for operations that are not allowed.
        forbidden_ops = [
            "DELETE",
            "DROP",
            "TRUNCATE",
            "UPDATE",
            "INSERT",
            "ALTER",
            "CREATE",
        ]

        for op in forbidden_ops:
            if re.search(rf"\b{op}\b", query, re.IGNORECASE):
                logger.error(f"❌ Forbidden operation detected: {op}")
                raise ValueError(f"{op} queries are not allowed.")

        return query

    def execute_query(self, query: str) -> list[dict[str, Any]]:
        """Execute a SQL query and return results.

        Args:
            query: SQL query to execute.

        Returns:
            List of dictionaries representing query results.

        Raises:
            ValueError: If query validation fails.
            SQLAlchemyError: If query execution fails.
        """
        logger.info(f"Input query: {query}")

        try:
            validated_query = self._validate_query(query)
        except ValueError as e:
            logger.error(f"❌ Query validation failed: {e}")
            raise

        with self.Session() as session:
            try:
                result = session.execute(text(validated_query))

                # Convert result to list of dictionaries.
                rows = []
                for row in result:
                    rows.append(dict(row._mapping))

                logger.info(
                    f"✅ Query executed successfully - {len(rows)} rows returned"
                )

                # Log sample of results (first row only for privacy)
                if rows:
                    sample_row = {k: str(v)[:50] for k, v in list(rows[0].items())[:5]}
                    logger.info(f"Sample result: {sample_row}")
                else:
                    logger.info("📊 No rows returned")

                return rows

            except SQLAlchemyError as e:
                logger.error(f"❌ Database error during query execution: {str(e)}")
                raise SQLAlchemyError(f"Error executing query: {str(e)}") from e

    def get_schema_description(self) -> dict[str, Any]:
        """Get database schema description.

        Returns:
            Dictionary containing schema information.
        """
        logger.info("📋 Getting database schema description")

        if self._schema_cache is not None:
            logger.info("♻️ Using cached schema")
            return self._schema_cache

        with self.Session() as session:
            # Get all tables.
            tables_query = """
            SELECT table_schema, table_name, table_type
            FROM information_schema.tables
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
            ORDER BY table_schema, table_name
            """

            # Get all columns.
            columns_query = """
            SELECT table_schema, table_name, column_name, data_type, 
                   is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
            ORDER BY table_schema, table_name, ordinal_position
            """

            # Get foreign keys.
            foreign_keys_query = """
            SELECT
                tc.table_schema,
                tc.table_name,
                kcu.column_name,
                ccu.table_schema AS foreign_table_schema,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name
            FROM information_schema.table_constraints AS tc
            JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
            """

            # Get table comments.
            comments_query = """
            SELECT schemaname, tablename, description
            FROM pg_tables t
            JOIN pg_description d ON d.objoid = (
                SELECT oid FROM pg_class WHERE relname = t.tablename
            )
            WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
            """

            try:
                logger.info("📊 Fetching table information")
                tables_result = session.execute(text(tables_query)).fetchall()
                logger.info(f"Found {len(tables_result)} tables")

                logger.info("📋 Fetching column information")
                columns_result = session.execute(text(columns_query)).fetchall()
                logger.info(f"Found {len(columns_result)} columns")

                logger.info("🔗 Fetching foreign key information")
                fks_result = session.execute(text(foreign_keys_query)).fetchall()
                logger.info(f"Found {len(fks_result)} foreign keys")

                logger.info("💬 Fetching table comments")
                comments_result = session.execute(text(comments_query)).fetchall()
                logger.info(f"Found {len(comments_result)} table comments")

                # Build schema structure.
                schema = {}

                # Add tables.
                for row in tables_result:
                    schema_name = row.table_schema
                    table_name = row.table_name

                    if schema_name not in schema:
                        schema[schema_name] = {}

                    schema[schema_name][table_name] = {
                        "type": row.table_type,
                        "columns": [],
                        "foreign_keys": [],
                        "description": None,
                    }

                # Add columns.
                for row in columns_result:
                    schema_name = row.table_schema
                    table_name = row.table_name

                    if schema_name in schema and table_name in schema[schema_name]:
                        schema[schema_name][table_name]["columns"].append(
                            {
                                "name": row.column_name,
                                "type": row.data_type,
                                "nullable": row.is_nullable == "YES",
                                "default": row.column_default,
                            }
                        )

                # Add foreign keys.
                for row in fks_result:
                    schema_name = row.table_schema
                    table_name = row.table_name

                    if schema_name in schema and table_name in schema[schema_name]:
                        schema[schema_name][table_name]["foreign_keys"].append(
                            {
                                "column": row.column_name,
                                "references_schema": row.foreign_table_schema,
                                "references_table": row.foreign_table_name,
                                "references_column": row.foreign_column_name,
                            }
                        )

                # Add table comments.
                for row in comments_result:
                    schema_name = row.schemaname
                    table_name = row.tablename

                    if schema_name in schema and table_name in schema[schema_name]:
                        schema[schema_name][table_name]["description"] = row.description

                # Log schema summary
                total_tables = sum(len(tables) for tables in schema.values())
                logger.info(
                    f"✅ Schema built successfully: {len(schema)} schemas, {total_tables} tables"
                )

                # Log table names for debugging
                all_tables = []
                for schema_name, tables in schema.items():
                    for table_name in tables.keys():
                        all_tables.append(f"{schema_name}.{table_name}")

                logger.info(
                    f"Tables found: {', '.join(all_tables[:10])}{'...' if len(all_tables) > 10 else ''}"
                )

                self._schema_cache = schema
                return schema

            except SQLAlchemyError as e:
                logger.error(f"❌ Error building schema: {str(e)}")
                raise SQLAlchemyError(f"Error getting schema: {str(e)}") from e

    def get_sample_data(self, table_name: str, limit: int = 3) -> list[dict[str, Any]]:
        """Get sample data from a table.

        Args:
            table_name: Name of the table.
            limit: Number of sample rows.

        Returns:
            List of sample rows.
        """
        logger.info(f"📋 Getting sample data from {table_name} (limit: {limit})")
        query = f"SELECT * FROM {table_name} LIMIT {limit}"
        return self.execute_query(query)

    def get_table_row_count(self, table_name: str) -> int:
        """Get row count for a table.

        Args:
            table_name: Name of the table.

        Returns:
            Number of rows in the table.
        """
        logger.info(f"🔢 Getting row count for {table_name}")
        query = f"SELECT COUNT(*) as count FROM {table_name}"
        result = self.execute_query(query)
        count = result[0]["count"] if result else 0
        logger.info(f"Table {table_name} has {count} rows")
        return count

    def health_check(self) -> bool:
        """Check if database connection is healthy.

        Returns:
            True if connection is healthy.
        """
        logger.info("🏥 Performing database health check")
        try:
            with self.Session() as session:
                session.execute(text("SELECT 1"))
                logger.info("✅ Database health check passed")
                return True
        except Exception as e:
            logger.error(f"❌ Database health check failed: {e}")
            return False

    # Predefined query methods for common operations
    def get_database_stats(self) -> dict[str, Any]:
        """Get comprehensive database statistics.

        Returns:
            Dictionary containing various database statistics.
        """
        logger.info("📊 Getting comprehensive database statistics")

        stats: dict[str, Any] = {"health_check": self.health_check()}

        if not stats["health_check"]:
            return stats

        try:
            # System counts query
            system_stats_query = """
                SELECT 
                    COUNT(*) as total_systems,
                    SUM(CASE WHEN pws_type_code = 'CWS' THEN 1 ELSE 0 END) as community_systems,
                    SUM(CASE WHEN pws_type_code = 'TNCWS' THEN 1 ELSE 0 END) as transient_systems,
                    SUM(CASE WHEN pws_type_code = 'NTNCWS' THEN 1 ELSE 0 END) as non_transient_systems,
                    SUM(CASE WHEN pws_activity_code = 'A' THEN 1 ELSE 0 END) as active_systems,
                    SUM(population_served_count) as total_population_served,
                    AVG(population_served_count) as avg_population_served
                FROM sdwa_pub_water_systems
            """

            # Violation stats query
            violation_stats_query = """
                SELECT 
                    COUNT(*) as total_violations,
                    SUM(CASE WHEN is_health_based_ind = 'Y' THEN 1 ELSE 0 END) as health_violations,
                    SUM(CASE WHEN violation_status = 'Unaddressed' THEN 1 ELSE 0 END) as unaddressed_violations,
                    SUM(CASE WHEN violation_status = 'Resolved' THEN 1 ELSE 0 END) as resolved_violations,
                    COUNT(DISTINCT pwsid) as systems_with_violations
                FROM sdwa_violations_enforcement
            """

            # Execute queries
            system_results = self.execute_query(system_stats_query)
            violation_results = self.execute_query(violation_stats_query)

            if system_results:
                stats.update(system_results[0])
            if violation_results:
                stats.update(violation_results[0])

            # Get table row counts for main tables
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
                    count = self.get_table_row_count(table)
                    table_counts[table] = count
                except Exception:
                    table_counts[table] = "Error"

            stats["table_counts"] = table_counts

            logger.info("✅ Database statistics retrieved successfully")
            return stats

        except Exception as e:
            logger.error(f"❌ Error getting database statistics: {e}")
            stats["error"] = str(e)
            return stats

    def search_water_systems(
        self,
        city: str | None = None,
        county: str | None = None,
        system_name: str | None = None,
        limit: int = 20,
    ) -> list[dict[str, Any]]:
        """Search for water systems with improved query.

        Args:
            city: City name to search for (partial match).
            county: County name to search for (partial match).
            system_name: Water system name to search for (partial match).
            limit: Maximum number of results to return.

        Returns:
            List of water system records.
        """
        logger.info(
            f"🔍 Searching water systems: city='{city}', county='{county}', system_name='{system_name}'"
        )

        try:
            # Build the base query with proper joins and safety
            query_parts = [
                "SELECT DISTINCT",
                "  pws.pwsid,",
                "  pws.pws_name,",
                "  pws.city_name,",
                "  geo.county_served,",
                "  pws.population_served_count,",
                "  pws.pws_type_code,",
                "  pws.pws_activity_code,",
                "  COALESCE(v.violation_count, 0) as active_violations",
                "FROM sdwa_pub_water_systems pws",
                "LEFT JOIN sdwa_geographic_areas geo",
                "  ON pws.submissionyearquarter = geo.submissionyearquarter",
                "  AND pws.pwsid = geo.pwsid",
                "LEFT JOIN (",
                "  SELECT pwsid, submissionyearquarter, COUNT(*) as violation_count",
                "  FROM sdwa_violations_enforcement",
                "  WHERE violation_status = 'Unaddressed'",
                "  GROUP BY pwsid, submissionyearquarter",
                ") v ON pws.pwsid = v.pwsid",
                "  AND pws.submissionyearquarter = v.submissionyearquarter",
                "WHERE pws.pws_activity_code = 'A'",  # Only active systems
            ]

            # Add search conditions
            conditions = []

            if city:
                # Search in both system city and geographic areas
                escaped_city = city.replace("'", "''")
                conditions.append(
                    f"(pws.city_name ILIKE '%{escaped_city}%' OR geo.city_served ILIKE '%{escaped_city}%')"
                )

            if county:
                escaped_county = county.replace("'", "''")
                conditions.append(f"geo.county_served ILIKE '%{escaped_county}%'")

            if system_name:
                escaped_name = system_name.replace("'", "''")
                conditions.append(f"pws.pws_name ILIKE '%{escaped_name}%'")

            if conditions:
                query_parts.append(f"AND ({' OR '.join(conditions)})")

            query_parts.extend(["ORDER BY pws.pws_name", f"LIMIT {limit}"])

            query = "\n".join(query_parts)

            results = self.execute_query(query)
            logger.info(f"✅ Found {len(results)} water systems")
            return results

        except Exception as e:
            logger.error(f"❌ Error searching water systems: {e}")
            return [{"error": str(e)}]

    def get_sample_cities(self, limit: int = 20) -> list[dict[str, Any]]:
        """Get sample cities for debugging/suggestions.

        Args:
            limit: Maximum number of cities to return.

        Returns:
            List of city records.
        """
        logger.info(f"🏙️ Getting sample cities (limit: {limit})")

        query = f"""
            SELECT DISTINCT city_name
            FROM sdwa_pub_water_systems 
            WHERE city_name IS NOT NULL 
              AND city_name != ''
              AND pws_activity_code = 'A'
            ORDER BY city_name 
            LIMIT {limit}
        """

        return self.execute_query(query)

    def get_sample_counties(self, limit: int = 20) -> list[dict[str, Any]]:
        """Get sample counties for debugging/suggestions.

        Args:
            limit: Maximum number of counties to return.

        Returns:
            List of county records.
        """
        logger.info(f"🏛️ Getting sample counties (limit: {limit})")

        query = f"""
            SELECT DISTINCT county_served
            FROM sdwa_geographic_areas
            WHERE county_served IS NOT NULL 
              AND county_served != ''
            ORDER BY county_served
            LIMIT {limit}
        """

        return self.execute_query(query)

    def get_recent_violations(self, limit: int = 20) -> list[dict[str, Any]]:
        """Get recent violations for quick access.

        Args:
            limit: Maximum number of violations to return.

        Returns:
            List of violation records.
        """
        logger.info(f"⚠️ Getting recent violations (limit: {limit})")

        query = f"""
            SELECT 
                v.pwsid,
                pws.pws_name,
                v.violation_code,
                v.violation_category_code,
                v.is_health_based_ind,
                v.violation_status,
                v.non_compl_per_begin_date,
                v.contaminant_code,
                geo.city_served,
                geo.county_served
            FROM sdwa_violations_enforcement v
            JOIN sdwa_pub_water_systems pws 
              ON v.pwsid = pws.pwsid 
              AND v.submissionyearquarter = pws.submissionyearquarter
            LEFT JOIN sdwa_geographic_areas geo
              ON v.pwsid = geo.pwsid
              AND v.submissionyearquarter = geo.submissionyearquarter
            WHERE v.violation_status = 'Unaddressed'
              AND pws.pws_activity_code = 'A'
            ORDER BY v.non_compl_per_begin_date DESC NULLS LAST
            LIMIT {limit}
        """

        return self.execute_query(query)

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
        """Enhanced search for water systems with multiple criteria.

        Args:
            system_id: PWS ID to search for.
            city: City name to search for (partial match).
            county: County name to search for (partial match).
            system_name: Water system name to search for (partial match).
            system_type: System type code (CWS, TNCWS, NTNCWS).
            source_type: Primary source water type.
            limit: Maximum number of results to return.

        Returns:
            List of water system records.
        """
        logger.info(
            f"🔍 Enhanced search: id='{system_id}', city='{city}', county='{county}', "
            f"name='{system_name}', type='{system_type}', source='{source_type}'"
        )

        try:
            # Build the base query with proper joins and safety
            query_parts = [
                "SELECT DISTINCT",
                "  pws.pwsid,",
                "  pws.pws_name,",
                "  pws.city_name,",
                "  geo.county_served,",
                "  pws.population_served_count,",
                "  pws.pws_type_code,",
                "  pws.pws_activity_code,",
                "  pws.primary_source_code,",
                "  pws.gw_sw_code,",
                "  COALESCE(v.violation_count, 0) as active_violations",
                "FROM sdwa_pub_water_systems pws",
                "LEFT JOIN sdwa_geographic_areas geo",
                "  ON pws.submissionyearquarter = geo.submissionyearquarter",
                "  AND pws.pwsid = geo.pwsid",
                "LEFT JOIN (",
                "  SELECT pwsid, submissionyearquarter, COUNT(*) as violation_count",
                "  FROM sdwa_violations_enforcement",
                "  WHERE violation_status = 'Unaddressed'",
                "  GROUP BY pwsid, submissionyearquarter",
                ") v ON pws.pwsid = v.pwsid",
                "  AND pws.submissionyearquarter = v.submissionyearquarter",
                "WHERE pws.pws_activity_code = 'A'",  # Only active systems
            ]

            # Add search conditions
            conditions = []

            if system_id:
                # Exact match for system ID
                escaped_id = system_id.replace("'", "''")
                conditions.append(f"pws.pwsid = '{escaped_id}'")

            if system_name:
                # Search in system name
                escaped_name = system_name.replace("'", "''")
                conditions.append(f"pws.pws_name ILIKE '%{escaped_name}%'")

            if city:
                # Search in both system city and geographic areas
                escaped_city = city.replace("'", "''")
                conditions.append(
                    f"(pws.city_name ILIKE '%{escaped_city}%' OR geo.city_served ILIKE '%{escaped_city}%')"
                )

            if county:
                escaped_county = county.replace("'", "''")
                conditions.append(f"geo.county_served ILIKE '%{escaped_county}%'")

            if system_type:
                # Map user-friendly names to codes
                type_map = {
                    "Community (CWS)": "CWS",
                    "Transient Non-Community (TNCWS)": "TNCWS",
                    "Non-Transient Non-Community (NTNCWS)": "NTNCWS",
                }
                type_code = type_map.get(system_type, system_type)
                conditions.append(f"pws.pws_type_code = '{type_code}'")

            if source_type:
                # Map source types to database codes
                source_map = {
                    "Surface Water": "SW",
                    "Groundwater": "GW",
                    "Mixed Sources": "GU",  # Ground/Surface mixed
                }
                source_code = source_map.get(source_type)
                if source_code:
                    conditions.append(f"pws.gw_sw_code = '{source_code}'")

            if conditions:
                query_parts.append(f"AND ({' AND '.join(conditions)})")

            query_parts.extend(["ORDER BY pws.pws_name", f"LIMIT {limit}"])

            query = "\n".join(query_parts)

            results = self.execute_query(query)
            logger.info(f"✅ Enhanced search found {len(results)} water systems")
            return results

        except Exception as e:
            logger.error(f"❌ Error in enhanced search: {e}")
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
        """Search for water systems with sample/violation data.

        Args:
            sample_class: Type of samples (Lead & Copper, Bacteria/Coliform, etc.).
            start_date: Start date for sample period.
            end_date: End date for sample period.
            city: City name filter.
            county: County name filter.
            limit: Maximum number of results.

        Returns:
            List of water system records with sample data.
        """
        logger.info(
            f"🧪 Sample search: class='{sample_class}', dates='{start_date}' to '{end_date}'"
        )

        try:
            # Build query focusing on violations (which include sample results)
            query_parts = [
                "SELECT DISTINCT",
                "  pws.pwsid,",
                "  pws.pws_name,",
                "  pws.city_name,",
                "  geo.county_served,",
                "  pws.population_served_count,",
                "  pws.pws_type_code,",
                "  v.violation_code,",
                "  v.contaminant_code,",
                "  v.violation_category_code,",
                "  v.is_health_based_ind,",
                "  v.violation_status,",
                "  v.non_compl_per_begin_date,",
                "  COUNT(v.violation_id) as violation_count",
                "FROM sdwa_pub_water_systems pws",
                "JOIN sdwa_violations_enforcement v",
                "  ON pws.submissionyearquarter = v.submissionyearquarter",
                "  AND pws.pwsid = v.pwsid",
                "LEFT JOIN sdwa_geographic_areas geo",
                "  ON pws.submissionyearquarter = geo.submissionyearquarter",
                "  AND pws.pwsid = geo.pwsid",
                "WHERE pws.pws_activity_code = 'A'",
            ]

            conditions = []

            # Map sample classes to contaminant/violation patterns
            if sample_class:
                if sample_class == "Lead & Copper":
                    conditions.append(
                        "(v.contaminant_code IN ('PB90', 'CU90') OR v.violation_code LIKE 'LCR%')"
                    )
                elif sample_class == "Bacteria/Coliform":
                    conditions.append(
                        "(v.contaminant_code LIKE '%COL%' OR v.violation_code LIKE '%TCR%' OR v.violation_code LIKE '%RTCR%')"
                    )
                elif sample_class == "Chemical":
                    conditions.append("v.violation_category_code IN ('MR', 'MCL')")
                elif sample_class == "Radiological":
                    conditions.append(
                        "(v.contaminant_code LIKE 'RA%' OR v.violation_code LIKE 'RAD%')"
                    )

            # Date range filtering
            if start_date:
                conditions.append(f"v.non_compl_per_begin_date >= '{start_date}'")
            if end_date:
                conditions.append(f"v.non_compl_per_begin_date <= '{end_date}'")

            # Location filtering
            if city:
                escaped_city = city.replace("'", "''")
                conditions.append(
                    f"(pws.city_name ILIKE '%{escaped_city}%' OR geo.city_served ILIKE '%{escaped_city}%')"
                )

            if county:
                escaped_county = county.replace("'", "''")
                conditions.append(f"geo.county_served ILIKE '%{escaped_county}%'")

            if conditions:
                query_parts.append(f"AND {' AND '.join(conditions)}")

            query_parts.extend(
                [
                    "GROUP BY pws.pwsid, pws.pws_name, pws.city_name, geo.county_served,",
                    "         pws.population_served_count, pws.pws_type_code, v.violation_code,",
                    "         v.contaminant_code, v.violation_category_code, v.is_health_based_ind,",
                    "         v.violation_status, v.non_compl_per_begin_date",
                    "ORDER BY v.non_compl_per_begin_date DESC NULLS LAST",
                    f"LIMIT {limit}",
                ]
            )

            query = "\n".join(query_parts)

            results = self.execute_query(query)
            logger.info(f"✅ Sample search found {len(results)} results")
            return results

        except Exception as e:
            logger.error(f"❌ Error in sample search: {e}")
            return [{"error": str(e)}]

    def get_source_water_types(self) -> list[dict[str, Any]]:
        """Get available source water types from the database.

        Returns:
            List of source water types with counts.
        """
        query = """
            SELECT 
                gw_sw_code,
                COUNT(*) as system_count,
                CASE gw_sw_code
                    WHEN 'SW' THEN 'Surface Water'
                    WHEN 'GW' THEN 'Groundwater'
                    WHEN 'GU' THEN 'Mixed Sources'
                    ELSE 'Unknown'
                END as source_description
            FROM sdwa_pub_water_systems
            WHERE pws_activity_code = 'A'
            AND gw_sw_code IS NOT NULL
            GROUP BY gw_sw_code
            ORDER BY system_count DESC
        """

        return self.execute_query(query)

    def get_contaminant_types(self) -> list[dict[str, Any]]:
        """Get available contaminant codes for sample filtering.

        Returns:
            List of contaminant types with violation counts.
        """
        query = """
            SELECT 
                contaminant_code,
                COUNT(*) as violation_count,
                violation_category_code
            FROM sdwa_violations_enforcement
            WHERE contaminant_code IS NOT NULL
            AND contaminant_code != ''
            GROUP BY contaminant_code, violation_category_code
            ORDER BY violation_count DESC
            LIMIT 50
        """

        return self.execute_query(query)

    def get_all_water_systems_for_dropdown(
        self, limit: int = 1000
    ) -> list[dict[str, Any]]:
        """Get all water systems formatted for dropdown use.

        Args:
            limit: Maximum number of systems to return.

        Returns:
            List of water system records with display names.
        """
        logger.info(f"🏢 Getting all water systems for dropdown (limit: {limit})")

        try:
            query = """
            SELECT DISTINCT 
                pws.pwsid,
                pws.pws_name,
                pws.city_name,
                geo.county_served,
                CONCAT(
                    COALESCE(pws.pws_name, 'Unknown System'), 
                    ' (', pws.pwsid, ')',
                    CASE 
                        WHEN pws.city_name IS NOT NULL THEN ' - ' || pws.city_name
                        ELSE ''
                    END
                ) as display_name
            FROM sdwa_pub_water_systems pws
            LEFT JOIN sdwa_geographic_areas geo 
              ON pws.submissionyearquarter = geo.submissionyearquarter
              AND pws.pwsid = geo.pwsid
            WHERE pws.pws_activity_code = 'A' 
            AND pws.pws_name IS NOT NULL 
            AND pws.pws_name != ''
            ORDER BY pws.pws_name
            LIMIT %s
            """

            # Use string formatting for limit since it's safe (integer)
            formatted_query = query.replace("%s", str(limit))
            results = self.execute_query(formatted_query)

            logger.info(f"✅ Retrieved {len(results)} water systems for dropdown")
            return results

        except Exception as e:
            logger.error(f"❌ Error getting water systems for dropdown: {e}")
            return []

    def get_facilities_for_system_dropdown(self, pwsid: str) -> list[dict[str, Any]]:
        """Get facilities for a specific water system formatted for dropdown.

        Args:
            pwsid: Public Water System ID.

        Returns:
            List of facility records with display names.
        """
        logger.info(f"🏭 Getting facilities for system {pwsid}")

        try:
            # Escape the PWSID to prevent SQL injection
            escaped_pwsid = pwsid.replace("'", "''")

            query = f"""
            SELECT DISTINCT 
                facility_id,
                facility_name,
                facility_type_code,
                CONCAT(
                    COALESCE(facility_name, 'Facility'), 
                    ' (', facility_id, ')',
                    CASE 
                        WHEN facility_type_code IS NOT NULL THEN ' - ' || facility_type_code
                        ELSE ''
                    END
                ) as display_name
            FROM sdwa_facilities 
            WHERE pwsid = '{escaped_pwsid}'
            AND facility_activity_code = 'A'
            ORDER BY facility_name, facility_id
            LIMIT 100
            """

            results = self.execute_query(query)

            logger.info(f"✅ Retrieved {len(results)} facilities for system {pwsid}")
            return results

        except Exception as e:
            logger.error(f"❌ Error getting facilities for system {pwsid}: {e}")
            return []

    def search_water_systems_by_name(
        self, search_term: str, limit: int = 50
    ) -> list[dict[str, Any]]:
        """Search water systems by name with fuzzy matching.

        Args:
            search_term: Search term to match against system names.
            limit: Maximum number of results to return.

        Returns:
            List of matching water system records.
        """
        logger.info(f"🔍 Searching water systems by name: '{search_term}'")

        try:
            # Escape the search term to prevent SQL injection
            escaped_term = search_term.replace("'", "''")

            query = f"""
            SELECT DISTINCT 
                pws.pwsid,
                pws.pws_name,
                pws.city_name,
                geo.county_served,
                pws.population_served_count,
                pws.pws_type_code,
                CONCAT(
                    COALESCE(pws.pws_name, 'Unknown System'), 
                    ' (', pws.pwsid, ')',
                    CASE 
                        WHEN pws.city_name IS NOT NULL THEN ' - ' || pws.city_name
                        ELSE ''
                    END
                ) as display_name,
                -- Calculate relevance score for better sorting
                CASE 
                    WHEN pws.pws_name ILIKE '{escaped_term}%' THEN 1  -- Starts with
                    WHEN pws.pws_name ILIKE '%{escaped_term}%' THEN 2  -- Contains
                    WHEN pws.pwsid ILIKE '%{escaped_term}%' THEN 3     -- ID match
                    ELSE 4                                              -- Other
                END as relevance_score
            FROM sdwa_pub_water_systems pws
            LEFT JOIN sdwa_geographic_areas geo 
              ON pws.submissionyearquarter = geo.submissionyearquarter
              AND pws.pwsid = geo.pwsid
            WHERE pws.pws_activity_code = 'A' 
            AND (
                pws.pws_name ILIKE '%{escaped_term}%' 
                OR pws.pwsid ILIKE '%{escaped_term}%'
                OR pws.city_name ILIKE '%{escaped_term}%'
                OR geo.county_served ILIKE '%{escaped_term}%'
            )
            ORDER BY relevance_score, pws.pws_name
            LIMIT {limit}
            """

            results = self.execute_query(query)

            logger.info(
                f"✅ Found {len(results)} water systems matching '{search_term}'"
            )
            return results

        except Exception as e:
            logger.error(f"❌ Error searching water systems by name: {e}")
            return []

    def get_ccr_years_available(self, pwsid: str | None = None) -> list[int]:
        """Get available CCR years, optionally for a specific system.

        Args:
            pwsid: Optional - specific system ID to check.

        Returns:
            List of available years.
        """
        logger.info(f"📅 Getting available CCR years for {pwsid or 'all systems'}")

        try:
            # Since we don't have actual CCR data in the current schema,
            # return the last 5 years as a reasonable default
            from datetime import datetime

            current_year = datetime.now().year
            available_years = list(range(current_year, current_year - 5, -1))

            logger.info(f"✅ Returning {len(available_years)} available CCR years")
            return available_years

        except Exception as e:
            logger.error(f"❌ Error getting CCR years: {e}")
            return [2024, 2023, 2022, 2021, 2020]  # Fallback

    def get_monitoring_schedules_for_system(self, pwsid: str) -> list[dict[str, Any]]:
        """Get monitoring schedules for a specific water system.

        Args:
            pwsid: Public Water System ID.

        Returns:
            List of monitoring schedule records.
        """
        logger.info(f"📋 Getting monitoring schedules for system {pwsid}")

        try:
            # Since we don't have a dedicated schedules table in the current schema,
            # we'll derive schedule information from violations and events
            escaped_pwsid = pwsid.replace("'", "''")

            query = f"""
            SELECT DISTINCT
                v.pwsid,
                v.rule_code,
                v.rule_family_code,
                v.contaminant_code,
                v.violation_code,
                v.violation_category_code,
                COUNT(*) as violation_count,
                MIN(v.non_compl_per_begin_date) as first_violation_date,
                MAX(v.non_compl_per_end_date) as last_violation_date
            FROM sdwa_violations_enforcement v
            WHERE v.pwsid = '{escaped_pwsid}'
            GROUP BY v.pwsid, v.rule_code, v.rule_family_code, v.contaminant_code, 
                     v.violation_code, v.violation_category_code
            ORDER BY v.rule_code, v.contaminant_code
            LIMIT 100
            """

            results = self.execute_query(query)

            logger.info(
                f"✅ Retrieved {len(results)} schedule records for system {pwsid}"
            )
            return results

        except Exception as e:
            logger.error(f"❌ Error getting schedules for system {pwsid}: {e}")
            return []

    def get_ccr_data_for_system(self, pwsid: str, year: int) -> dict[str, Any]:
        """Get Consumer Confidence Report data for a specific system and year.

        Args:
            pwsid: Public Water System ID.
            year: Report year.

        Returns:
            Dictionary containing CCR data sections.
        """
        logger.info(f"📄 Getting CCR data for system {pwsid}, year {year}")

        try:
            escaped_pwsid = pwsid.replace("'", "''")

            # System basic information
            system_query = f"""
            SELECT DISTINCT
                pws.pwsid,
                pws.pws_name,
                pws.pws_type_code,
                pws.population_served_count,
                pws.service_connections_count,
                pws.org_name,
                pws.admin_name,
                pws.email_addr,
                pws.phone_number,
                pws.address_line1,
                pws.address_line2,
                pws.city_name,
                pws.zip_code,
                pws.gw_sw_code,
                pws.primary_source_code,
                geo.county_served
            FROM sdwa_pub_water_systems pws
            LEFT JOIN sdwa_geographic_areas geo
            ON pws.submissionyearquarter = geo.submissionyearquarter
            AND pws.pwsid = geo.pwsid
            WHERE pws.pwsid = '{escaped_pwsid}'
            AND pws.pws_activity_code = 'A'
            LIMIT 1
            """

            # Violations for the year
            violations_query = f"""
            SELECT 
                violation_code,
                violation_category_code,
                contaminant_code,
                is_health_based_ind,
                violation_status,
                viol_measure,
                unit_of_measure,
                federal_mcl,
                state_mcl,
                non_compl_per_begin_date,
                non_compl_per_end_date,
                rule_code,
                rule_family_code
            FROM sdwa_violations_enforcement
            WHERE pwsid = '{escaped_pwsid}'
            AND EXTRACT(YEAR FROM non_compl_per_begin_date) = {year}
            ORDER BY non_compl_per_begin_date DESC
            """

            # Water quality test results (from LCR samples if available)
            testing_query = f"""
            SELECT 
                contaminant_code,
                sample_measure,
                unit_of_measure,
                sampling_end_date,
                result_sign_code
            FROM sdwa_lcr_samples
            WHERE pwsid = '{escaped_pwsid}'
            AND EXTRACT(YEAR FROM sampling_end_date) = {year}
            ORDER BY sampling_end_date DESC
            LIMIT 100
            """

            # Execute queries
            system_info = self.execute_query(system_query)
            violations = self.execute_query(violations_query)
            test_results = self.execute_query(testing_query)

            # Compile CCR data
            ccr_data = {
                "system_info": system_info[0] if system_info else {},
                "violations": violations,
                "test_results": test_results,
                "year": year,
                "generated_date": "2025-01-01",  # Would be current date in real implementation
            }

            logger.info(
                f"✅ Retrieved CCR data: {len(violations)} violations, {len(test_results)} test results"
            )
            return ccr_data

        except Exception as e:
            logger.error(f"❌ Error getting CCR data for {pwsid}: {e}")
            return {
                "system_info": {},
                "violations": [],
                "test_results": [],
                "year": year,
                "error": str(e),
            }
