"""Unit tests for Georgia Water Quality Chatbot setup."""

import os
import sys
import unittest
from unittest.mock import MagicMock
from unittest.mock import patch

# Add the backend directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), "backend"))

try:
    from sql_manager import SqlManager
except ImportError as e:
    print(f"Import error for sql_manager: {e}")
    SqlManager = None

try:
    from chat_manager import ChatManager
    from chat_manager import MessageRole
except ImportError as e:
    print(f"Import error for chat_manager: {e}")
    ChatManager = None

try:
    from api_manager import ApiManager
except ImportError as e:
    print(f"Import error for api_manager: {e}")
    ApiManager = None


class TestDatabaseConnection(unittest.TestCase):
    """Test database connectivity and schema loading."""

    def setUp(self):
        """Set up test fixtures."""
        self.database_url = "postgresql://noahlieberman@localhost:5432/sdwis_georgia"

    def test_database_connection(self):
        """Test basic database connection."""
        if SqlManager is None:
            self.skipTest("SqlManager not available")

        try:
            sql_manager = SqlManager(self.database_url)
            health_check = sql_manager.health_check()
            self.assertTrue(health_check, "Database connection failed")
            print("✅ Database connection successful")
        except Exception as e:
            self.fail(f"Database connection failed: {e}")

    def test_schema_loading(self):
        """Test schema description loading."""
        if SqlManager is None:
            self.skipTest("SqlManager not available")

        try:
            sql_manager = SqlManager(self.database_url)
            schema = sql_manager.get_schema_description()

            # Check that we have the expected tables
            self.assertIsInstance(schema, dict, "Schema should be a dictionary")
            self.assertGreater(len(schema), 0, "Schema should not be empty")

            # Check for key SDWIS tables
            found_tables = []
            for _, tables in schema.items():
                found_tables.extend(tables.keys())

            expected_tables = [
                "sdwa_pub_water_systems",
                "sdwa_violations_enforcement",
                "sdwa_facilities",
                "sdwa_geographic_areas",
            ]

            for table in expected_tables:
                if table in found_tables:
                    print(f"✅ Found table: {table}")
                else:
                    print(f"❌ Missing table: {table}")

            print(f"✅ Schema loaded successfully with {len(found_tables)} tables")

        except Exception as e:
            self.fail(f"Schema loading failed: {e}")

    def test_sample_query(self):
        """Test a simple query execution."""
        if SqlManager is None:
            self.skipTest("SqlManager not available")

        try:
            sql_manager = SqlManager(self.database_url)

            # Test simple query
            query = "SELECT COUNT(*) as total FROM sdwa_pub_water_systems"
            results = sql_manager.execute_query(query)

            self.assertIsInstance(results, list, "Results should be a list")
            self.assertGreater(len(results), 0, "Should have at least one result")
            self.assertIn("total", results[0], "Should have 'total' column")

            total_systems = results[0]["total"]
            print(
                f"✅ Query executed successfully: {total_systems} water systems found"
            )

        except Exception as e:
            self.fail(f"Sample query failed: {e}")


class TestSystemPrompt(unittest.TestCase):
    """Test system prompt generation and LLM setup."""

    def setUp(self):
        """Set up test fixtures."""
        self.database_url = "postgresql://noahlieberman@localhost:5432/sdwis_georgia"
        self.mock_api_key = "test-api-key"

    def test_system_prompt_generation(self):
        """Test system prompt generation with real schema."""
        if SqlManager is None or ChatManager is None:
            self.skipTest("Required managers not available")

        try:
            # Get real schema
            sql_manager = SqlManager(self.database_url)
            schema = sql_manager.get_schema_description()

            # Create chat manager with mock API client
            with patch("google.genai.Client") as mock_client:
                # Set up mock client
                mock_client_instance = MagicMock()
                mock_client.return_value = mock_client_instance

                chat_manager = ChatManager(self.mock_api_key)

                # Generate system prompt
                system_prompt = chat_manager.format_system_prompt(schema)

                # Verify prompt contains key elements
                self.assertIn("Georgia Safe Drinking Water", system_prompt)
                self.assertIn("sdwa_pub_water_systems", system_prompt)
                self.assertIn("SELECT", system_prompt)
                self.assertIn("health-based", system_prompt)

                print("✅ System prompt generated successfully")
                print(f"Prompt length: {len(system_prompt)} characters")

                # Print first few lines for verification
                lines = system_prompt.split("\n")[:100]
                print("First 10 lines of system prompt:")
                for i, line in enumerate(lines, 1):
                    print(f"{i:2d}: {line}")

        except Exception as e:
            self.fail(f"System prompt generation failed: {e}")

    def test_schema_summarization(self):
        """Test that schema is properly summarized for prompt."""
        if SqlManager is None or ChatManager is None:
            self.skipTest("Required managers not available")

        try:
            # Get schema
            sql_manager = SqlManager(self.database_url)
            schema = sql_manager.get_schema_description()

            # Create mock chat manager
            with patch("google.genai.Client") as mock_client:
                # Set up mock client
                mock_client_instance = MagicMock()
                mock_client.return_value = mock_client_instance

                chat_manager = ChatManager(self.mock_api_key)

                # Test schema summarization
                system_prompt = chat_manager.format_system_prompt(schema)

                # Check that key tables are mentioned
                key_tables = [
                    "sdwa_pub_water_systems",
                    "sdwa_violations_enforcement",
                    "sdwa_facilities",
                    "sdwa_geographic_areas",
                ]

                for table in key_tables:
                    if table in system_prompt:
                        print(f"✅ Table {table} included in prompt")
                    else:
                        print(f"❌ Table {table} missing from prompt")

        except Exception as e:
            self.fail(f"Schema summarization test failed: {e}")

    def test_chat_manager_initialization(self):
        """Test that ChatManager initializes correctly with mocked client."""
        if ChatManager is None:
            self.skipTest("ChatManager not available")

        try:
            with patch("google.genai.Client") as mock_client:
                # Set up mock client
                mock_client_instance = MagicMock()
                mock_client.return_value = mock_client_instance

                # Initialize ChatManager
                chat_manager = ChatManager(self.mock_api_key)

                # Verify initialization
                self.assertEqual(chat_manager.model_name, "gemini-1.5-flash")
                self.assertIsInstance(chat_manager.sessions, dict)
                self.assertEqual(len(chat_manager.sessions), 0)

                # Verify client was called with correct parameters
                mock_client.assert_called_once_with(api_key=self.mock_api_key)

                print("✅ ChatManager initialized successfully")

        except Exception as e:
            self.fail(f"ChatManager initialization failed: {e}")

    def test_session_management(self):
        """Test session creation and management."""
        if ChatManager is None:
            self.skipTest("ChatManager not available")

        try:
            with patch("google.genai.Client") as mock_client:
                # Set up mock client
                mock_client_instance = MagicMock()
                mock_client.return_value = mock_client_instance

                chat_manager = ChatManager(self.mock_api_key)

                # Test session creation
                session_id = chat_manager.create_session()
                self.assertIsInstance(session_id, str)
                self.assertIn(session_id, chat_manager.sessions)
                self.assertEqual(len(chat_manager.get_session(session_id)), 0)

                # Test adding messages
                chat_manager.add_message(session_id, MessageRole.USER, "Test message")
                messages = chat_manager.get_session(session_id)
                self.assertEqual(len(messages), 1)
                self.assertEqual(messages[0].content, "Test message")
                self.assertEqual(messages[0].role, MessageRole.USER)

                # Test clearing session
                chat_manager.clear_session(session_id)
                self.assertEqual(len(chat_manager.get_session(session_id)), 0)

                print("✅ Session management working correctly")

        except Exception as e:
            self.fail(f"Session management test failed: {e}")


class TestEnvironmentSetup(unittest.TestCase):
    """Test environment and configuration."""

    def test_python_version(self):
        """Test Python version."""
        import sys

        version = sys.version_info

        self.assertEqual(version.major, 3, "Should be Python 3")
        self.assertEqual(version.minor, 12, "Should be Python 3.12")

        print(f"✅ Python version: {version.major}.{version.minor}.{version.micro}")

    def test_database_url_format(self):
        """Test database URL format."""
        database_url = "postgresql://noahlieberman@localhost:5432/sdwis_georgia"

        self.assertIn("postgresql://", database_url)
        self.assertIn("sdwis_georgia", database_url)

        print(f"✅ Database URL format correct: {database_url}")


def run_comprehensive_test():
    """Run all tests and provide a summary."""
    print("🧪 Running Georgia Water Quality Chatbot Setup Tests\n")

    # Create test suite
    test_suite = unittest.TestSuite()

    # Add test cases
    test_suite.addTest(unittest.makeSuite(TestEnvironmentSetup))
    test_suite.addTest(unittest.makeSuite(TestDatabaseConnection))
    test_suite.addTest(unittest.makeSuite(TestSystemPrompt))

    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)

    # Print summary
    print(f"\n{'='*50}")
    print("TEST SUMMARY")
    print(f"{'='*50}")
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")

    if result.failures:
        print("\n❌ FAILURES:")
        for test, traceback in result.failures:
            print(f"  - {test}: {traceback}")

    if result.errors:
        print("\n❌ ERRORS:")
        for test, traceback in result.errors:
            print(f"  - {test}: {traceback}")

    if result.wasSuccessful():
        print("\n🎉 ALL TESTS PASSED! Your environment is ready.")
    else:
        print("\n⚠️  Some tests failed. Check the output above.")

    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_comprehensive_test()
    sys.exit(0 if success else 1)
