"""Chat Manager for Georgia Water Quality Assistant - Tool Calling Version with SQL
Retry."""

import json
import os
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Any
from typing import cast

import pydantic
from google import genai
from google.api_core import exceptions
from google.genai import errors as genai_errors
from google.genai import types
from jinja2 import Template


class MessageRole(Enum):
    """Message roles for chat."""

    USER = "user"
    ASSISTANT = "model"
    SYSTEM = "system"


@dataclass
class ChatMessage:
    """Represents a chat message."""

    role: MessageRole
    content: str
    timestamp: str | None = None


class ChatManagerError(Exception):
    """Base exception for ChatManager errors."""


class ContentFilterError(ChatManagerError):
    """Content filter error."""


class RateLimitError(ChatManagerError):
    """Rate limit error."""


class ServerError(ChatManagerError):
    """Server error."""


class ChatManager:
    """Manages chat conversations with LLM using tool calling for SQL."""

    def __init__(self, model_name: str = "gemini-1.5-flash"):
        """Initialize Chat Manager.

        Args:
            model_name: Gemini model to use.
        """
        self.model_name = model_name
        self._project = os.getenv("GOOGLE_CLOUD_PROJECT", "")
        self._client = genai.Client(
            vertexai=True, project=self._project, location="us-east1"
        )

        # Session storage.
        self.sessions: dict[str, list[ChatMessage]] = {}

        # System prompt template - Updated for tool calling with retry instructions.
        self.system_prompt_template = Template("""
You are a helpful assistant for the Georgia Safe Drinking Water Information System (SDWIS).

When users ask about water data, you MUST use the execute_sql tool to get real data.
Never make up or hallucinate information about water systems or violations.

WORKFLOW:
1. When users ask about water data, call execute_sql with an appropriate query
2. If the SQL query fails, you will be told the error message
3. If there's an error, analyze it and try a corrected query
4. You can retry up to 2 times with different approaches
5. Wait for the tool result and explain the results in friendly, conversational language
6. Focus on what the data means for drinking water safety

SQL ERROR HANDLING:
- If you get "column does not exist" error, check the schema carefully
- city_name is in sdwa_pub_water_systems, city_served is in sdwa_geographic_areas
- Always use proper foreign key joins: submissionyearquarter AND pwsid
- If you get a syntax error, fix the SQL syntax
- If you get a table name error, use the correct table names from the schema
- If you get a join error, check foreign key relationships
- Always explain what went wrong and how you fixed it

ENHANCED SEARCH CAPABILITIES:
- System ID searches: Use exact match on pwsid column
- System name searches: Use ILIKE with wildcards on pws_name
- System type filtering: CWS = Community, TNCWS = Transient Non-Community, NTNCWS = Non-Transient Non-Community
- Source water type: SW = Surface Water, GW = Groundwater, GU = Mixed Sources (check gw_sw_code column)
- Sample/violation searches: Join with sdwa_violations_enforcement table
- Date range searches: Use non_compl_per_begin_date for violation date filtering
- Contaminant filtering: Use contaminant_code (PB90=Lead, CU90=Copper, COL%=Coliform)

SAMPLE AND VIOLATION MAPPING:
- "Lead & Copper" searches: contaminant_code IN ('PB90', 'CU90') OR violation_code LIKE 'LCR%'
- "Bacteria/Coliform" searches: contaminant_code LIKE '%COL%' OR violation_code LIKE '%TCR%' OR violation_code LIKE '%RTCR%'
- "Chemical" searches: violation_category_code IN ('MR', 'MCL')
- "Radiological" searches: contaminant_code LIKE 'RA%' OR violation_code LIKE 'RAD%'

RESPONSE STYLE:
- Use natural, conversational tone
- Explain technical terms in simple language
- Focus on practical implications for users
- Don't mention SQL or technical details unless asked
- If SQL fails after retries, explain what information you were trying to get
- When showing search results, highlight key safety information

Key Tables:
- sdwa_pub_water_systems: Main water system information (has city_name, pwsid, pws_type_code, gw_sw_code)
- sdwa_violations_enforcement: Violations and enforcement actions (has contaminant_code, violation_status)
- sdwa_geographic_areas: Geographic coverage areas (has city_served, county_served)
- sdwa_facilities: Treatment facilities and sources
- sdwa_lcr_samples: Lead and Copper Rule sampling results

SQL Guidelines:
- Use proper table joins based on foreign key relationships
- Join tables using BOTH submissionyearquarter AND pwsid
- Filter for active systems (pws_activity_code = 'A') unless specifically asked otherwise
- Consider health-based violations (is_health_based_ind = 'Y') as more serious
- Always include LIMIT clauses to prevent large result sets
- Use ILIKE for case-insensitive text searches

Database Schema: {{ schema }}
""")

    def create_session(self) -> str:
        """Create a new chat session.

        Returns:
            Session ID.
        """
        session_id = str(uuid.uuid4())
        self.sessions[session_id] = []
        return session_id

    def get_session(self, session_id: str) -> list[ChatMessage]:
        """Get chat history for a session.

        Args:
            session_id: Session identifier.

        Returns:
            List of chat messages.

        Raises:
            ValueError: If session doesn't exist.
        """
        if session_id not in self.sessions:
            raise ValueError(f"Session {session_id} not found")
        return self.sessions[session_id]

    def add_message(self, session_id: str, role: MessageRole, content: str) -> None:
        """Add a message to the session.

        Args:
            session_id: Session identifier.
            role: Message role.
            content: Message content.
        """
        if session_id not in self.sessions:
            self.sessions[session_id] = []

        message = ChatMessage(role=role, content=content)
        self.sessions[session_id].append(message)

    def format_system_prompt(self, schema: dict[str, Any]) -> str:
        """Format the system prompt with schema information.

        Args:
            schema: Database schema description.

        Returns:
            Formatted system prompt.
        """
        # Create a condensed schema description for the prompt.
        schema_summary = {}

        for schema_name, tables in schema.items():
            schema_summary[schema_name] = {}
            for table_name, table_info in tables.items():
                # Include key information about each table.
                columns = [
                    f"{col['name']} ({col['type']})"
                    for col in table_info["columns"][:15]  # Limit columns.
                ]
                schema_summary[schema_name][table_name] = {
                    "description": table_info.get("description", "No description"),
                    "columns": columns,
                    "foreign_keys": table_info.get("foreign_keys", []),
                }

        return self.system_prompt_template.render(
            schema=json.dumps(schema_summary, indent=2)
        )

    def _build_conversation_contents(self, session_id: str) -> list[types.Content]:
        """Build conversation contents in Gemini format.

        Args:
            session_id: Session identifier.

        Returns:
            List of Content objects for Gemini API.
        """
        messages = self.get_session(session_id)
        contents = []

        for msg in messages:
            if msg.role == MessageRole.SYSTEM:
                continue
            elif msg.role == MessageRole.USER:
                contents.append(
                    types.Content(
                        role="user",
                        parts=[types.Part.from_text(text=msg.content)],
                    )
                )
            elif msg.role == MessageRole.ASSISTANT:
                contents.append(
                    types.Content(
                        role="model",
                        parts=[types.Part.from_text(text=msg.content)],
                    )
                )

        return contents

    def _create_sql_tool(self) -> types.Tool:
        """Create the SQL execution tool for Gemini.

        Returns:
            Tool definition for SQL execution.
        """
        return types.Tool(
            function_declarations=[
                types.FunctionDeclaration(
                    name="execute_sql",
                    description="Execute a SQL query against the Georgia water quality database",
                    parameters=types.Schema(
                        type="OBJECT",  # type: ignore
                        properties={
                            "query": types.Schema(
                                type="STRING",  # type: ignore
                                description="The SELECT SQL query to execute (read-only queries only)",
                            )
                        },
                        required=["query"],
                    ),
                )
            ]
        )

    async def generate_response_with_tools(  # noqa: C901, PLR0912, PLR0915
        self,
        session_id: str,
        user_message: str,
        schema: dict[str, Any],
        sql_executor: callable,  # type: ignore
    ) -> tuple[str, str | None, list[dict] | None]:
        """Generate response using tool calling for SQL execution with retry logic.

        Args:
            session_id: Session identifier.
            user_message: User's message.
            schema: Database schema for context.
            sql_executor: Function to execute SQL queries.

        Returns:
            Tuple of (response_text, sql_query, sql_results).
        """
        # Add user message to session.
        self.add_message(session_id, MessageRole.USER, user_message)

        # Format system prompt.
        system_prompt = self.format_system_prompt(schema)

        # Build conversation contents.
        contents = self._build_conversation_contents(session_id)

        # Create SQL tool.
        sql_tool = self._create_sql_tool()

        # Track SQL attempts for retry logic
        sql_attempts = []
        final_sql_query = None
        final_sql_results = None

        try:
            # Generate initial response with tools.
            api_contents = cast(list[types.ContentUnionDict], contents)
            response = await self._client.aio.models.generate_content(
                model=self.model_name,
                contents=api_contents,
                config=types.GenerateContentConfig(
                    system_instruction=system_prompt,
                    max_output_tokens=2048,
                    temperature=0.1,
                    tools=[sql_tool],
                ),
            )

            if not response or not response.candidates:
                raise ChatManagerError("Gemini did not return candidates")

            candidate = response.candidates[0]

            # Check finish reason.
            if candidate.finish_reason == "SAFETY":
                raise ContentFilterError("Content was filtered for safety")
            elif candidate.finish_reason != "STOP":
                raise ChatManagerError(
                    f"Gemini did not finish successfully: {candidate.finish_reason}"
                )

            if not candidate.content or not candidate.content.parts:
                raise ChatManagerError("Gemini did not return content")

            # Process tool calls with proper error handling and conversation tracking
            max_retries = 3
            retry_count = 0

            while retry_count < max_retries:
                # Check if LLM made a tool call in current response
                tool_calls = []
                text_response = ""

                for part in candidate.content.parts:  # type: ignore
                    if hasattr(part, "function_call") and part.function_call:
                        tool_calls.append(part.function_call)
                    elif hasattr(part, "text") and part.text:
                        text_response += part.text

                # Execute SQL if tool was called
                if tool_calls:
                    for tool_call in tool_calls:
                        if tool_call.name == "execute_sql":
                            sql_query = tool_call.args.get("query", "")
                            final_sql_query = sql_query  # Keep track of last query

                            # Add the tool call to conversation first
                            contents.append(
                                types.Content(
                                    role="model",
                                    parts=[
                                        types.Part(
                                            function_call=types.FunctionCall(
                                                name="execute_sql",
                                                args={"query": sql_query},
                                            )
                                        )
                                    ],
                                )
                            )

                            # Execute SQL with proper error handling
                            try:
                                sql_results = sql_executor(sql_query)
                                final_sql_results = sql_results

                                # Success! Add result to conversation
                                tool_response = f"Query returned {len(sql_results)} results: {json.dumps(sql_results[:5], indent=2)}..."

                                contents.append(
                                    types.Content(
                                        role="function",
                                        parts=[
                                            types.Part(
                                                function_response=types.FunctionResponse(
                                                    name="execute_sql",
                                                    response={"result": tool_response},
                                                )
                                            )
                                        ],
                                    )
                                )

                                # Generate final response with results
                                final_response = (
                                    await self._client.aio.models.generate_content(
                                        model=self.model_name,
                                        contents=contents,  # type: ignore
                                        config=types.GenerateContentConfig(
                                            system_instruction=system_prompt,
                                            max_output_tokens=1024,
                                            temperature=0.1,
                                        ),
                                    )
                                )

                                if (
                                    final_response
                                    and final_response.candidates
                                    and final_response.candidates[0].content
                                    and final_response.candidates[0].content.parts
                                ):
                                    final_text = ""
                                    for part in final_response.candidates[
                                        0
                                    ].content.parts:
                                        if hasattr(part, "text") and part.text:
                                            final_text += part.text

                                    # Add final response to session
                                    self.add_message(
                                        session_id, MessageRole.ASSISTANT, final_text
                                    )
                                    return (
                                        final_text,
                                        final_sql_query,
                                        final_sql_results,
                                    )

                                # If we get here, final response generation failed but query succeeded
                                success_msg = f"I found {len(sql_results)} results for your query, but had trouble generating the final response."
                                self.add_message(
                                    session_id, MessageRole.ASSISTANT, success_msg
                                )
                                return success_msg, final_sql_query, final_sql_results

                            except Exception as e:
                                # SQL execution failed - add error to conversation
                                sql_error = str(e)
                                sql_attempts.append(
                                    {"query": sql_query, "error": sql_error}
                                )

                                # Add error response to conversation
                                error_feedback = f"SQL Error: {sql_error}"
                                if (
                                    "column" in sql_error.lower()
                                    and "does not exist" in sql_error.lower()
                                ):
                                    error_feedback += " Remember: use 'city_name' from sdwa_pub_water_systems or join with sdwa_geographic_areas to use 'city_served'."

                                contents.append(
                                    types.Content(
                                        role="function",
                                        parts=[
                                            types.Part(
                                                function_response=types.FunctionResponse(
                                                    name="execute_sql",
                                                    response={
                                                        "error": error_feedback,
                                                        "hint": "Check the schema - city_name is in sdwa_pub_water_systems, city_served is in sdwa_geographic_areas",
                                                    },
                                                )
                                            )
                                        ],
                                    )
                                )

                                retry_count += 1

                                if retry_count < max_retries:
                                    # Generate retry response
                                    try:
                                        retry_response = await self._client.aio.models.generate_content(
                                            model=self.model_name,
                                            contents=contents,  # type: ignore
                                            config=types.GenerateContentConfig(
                                                system_instruction=system_prompt,
                                                max_output_tokens=2048,
                                                temperature=0.1,
                                                tools=[sql_tool],
                                            ),
                                        )

                                        if retry_response and retry_response.candidates:
                                            candidate = retry_response.candidates[0]
                                            # Continue the retry loop with new candidate
                                            continue
                                        else:
                                            # Retry generation failed
                                            break
                                    except Exception as retry_error:
                                        # Retry generation failed
                                        error_msg = f"Error during retry attempt {retry_count}: {str(retry_error)}"
                                        self.add_message(
                                            session_id, MessageRole.ASSISTANT, error_msg
                                        )
                                        return error_msg, final_sql_query, None
                                else:
                                    # Max retries reached
                                    error_summary = f"I tried {retry_count} different approaches to get that information, but ran into database errors. "
                                    if sql_attempts:
                                        last_error = sql_attempts[-1]["error"]
                                        error_summary += (
                                            f"The most recent issue was: {last_error}. "
                                        )
                                    error_summary += "Please try rephrasing your question or asking for different information."

                                    self.add_message(
                                        session_id, MessageRole.ASSISTANT, error_summary
                                    )
                                    return error_summary, final_sql_query, None

                # If no tool calls in this iteration, break the retry loop
                if not tool_calls:
                    if text_response:
                        self.add_message(
                            session_id, MessageRole.ASSISTANT, text_response
                        )
                        return text_response, None, None
                    break

            # If no tool calls, return the direct text response.
            if text_response:
                self.add_message(session_id, MessageRole.ASSISTANT, text_response)
                return text_response, None, None

            # Fallback response.
            fallback = "I'm ready to help you with Georgia water quality information. What would you like to know?"
            self.add_message(session_id, MessageRole.ASSISTANT, fallback)
            return fallback, None, None

        except exceptions.ResourceExhausted as e:
            raise RateLimitError("Rate limit exceeded") from e
        except exceptions.TooManyRequests as e:
            raise RateLimitError("Rate limit exceeded") from e
        except (exceptions.ClientError, genai_errors.ClientError) as e:
            if hasattr(e, "code") and e.code == 429:
                raise RateLimitError("Rate limit exceeded") from e
            raise ChatManagerError("Client error invoking Gemini API") from e
        except genai_errors.ServerError as e:
            raise ServerError("Server error") from e
        except pydantic.ValidationError as e:
            raise ContentFilterError("Validation error invoking Gemini API") from e
        except Exception as e:
            error_msg = f"Error generating response: {str(e)}"
            self.add_message(session_id, MessageRole.ASSISTANT, error_msg)
            raise ChatManagerError(error_msg) from e

    def clear_session(self, session_id: str) -> None:
        """Clear a chat session.

        Args:
            session_id: Session identifier.
        """
        if session_id in self.sessions:
            self.sessions[session_id] = []
