from fastmcp import FastMCP
from risingwave import OutputFormat
from connection import setup_risingwave_connection


def _escape_sql_string(value: str) -> str:
    """
    Escape a string value for safe use in SQL statements.

    This escapes single quotes by doubling them, which is the standard
    SQL escaping mechanism for string literals.

    Args:
        value: The string value to escape

    Returns:
        The escaped string (without surrounding quotes)
    """
    return value.replace("'", "''")


def register_session_tools(mcp: FastMCP):
    """Register all session variable MCP tools"""

    @mcp.tool
    def set_session_variable(variable_name: str, value: str) -> str:
        """
        Set a session variable/runtime parameter in RisingWave.

        This sets the value for the current session only. Common variables include:
        - query_mode: 'local', 'distributed', or 'auto'
        - streaming_parallelism: parallelism for CREATE MATERIALIZED VIEW/TABLE/INDEX
        - search_path: schema search order
        - timezone: session timezone (e.g., 'UTC', 'America/New_York')
        - statement_timeout: query timeout in seconds
        - visibility_mode: 'default' or 'all' (to query uncommitted data)
        - implicit_flush: 'true' or 'false'
        - background_ddl: 'true' or 'false' to run DDL in background
        - source_rate_limit: max records per second per source parallelism
        - backfill_rate_limit: max records per second for backfill process
        - batch_parallelism: parallelism for batch queries

        Args:
            variable_name: Name of the session variable to set
            value: Value to set (use 'DEFAULT' to reset to default)

        Returns:
            Success message or error
        """
        rw = setup_risingwave_connection()
        try:
            # Sanitize variable name to prevent SQL injection
            # Variable names should only contain alphanumeric characters and underscores
            if not variable_name.replace("_", "").isalnum():
                return f"Error: Invalid variable name '{variable_name}'. Variable names should only contain alphanumeric characters and underscores."

            # Handle special value 'DEFAULT'
            if value.upper() == "DEFAULT":
                query = f"SET {variable_name} TO DEFAULT"
            else:
                # Escape single quotes to prevent SQL injection
                escaped_value = _escape_sql_string(value)
                query = f"SET {variable_name} TO '{escaped_value}'"

            rw.execute(query)
            return f"Successfully set {variable_name} to {value}"
        except Exception as e:
            return f"Error setting session variable: {str(e)}"

    @mcp.tool
    def show_session_variable(variable_name: str) -> str:
        """
        Show the current value of a session variable/runtime parameter.

        Args:
            variable_name: Name of the session variable to show

        Returns:
            Current value of the variable
        """
        rw = setup_risingwave_connection()
        try:
            # Sanitize variable name
            if not variable_name.replace("_", "").isalnum():
                return f"Error: Invalid variable name '{variable_name}'. Variable names should only contain alphanumeric characters and underscores."

            query = f"SHOW {variable_name}"
            result = rw.fetchone(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error showing session variable: {str(e)}"

    @mcp.tool
    def show_all_session_variables() -> str:
        """
        Show all session variables/runtime parameters and their current values.

        Returns:
            All session variables with their current values
        """
        rw = setup_risingwave_connection()
        try:
            result = rw.fetch("SHOW ALL", format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error showing all session variables: {str(e)}"

    @mcp.tool
    def alter_system_variable(variable_name: str, value: str) -> str:
        """
        Alter a system-wide default for a runtime parameter.

        This sets the default value for all NEW sessions. It does not affect
        the current session. Use set_session_variable to change the current session.

        Note: This requires appropriate permissions.

        Args:
            variable_name: Name of the system variable to alter
            value: Value to set (use 'DEFAULT' to reset to default)

        Returns:
            Success message or error
        """
        rw = setup_risingwave_connection()
        try:
            # Sanitize variable name
            if not variable_name.replace("_", "").isalnum():
                return f"Error: Invalid variable name '{variable_name}'. Variable names should only contain alphanumeric characters and underscores."

            # Handle special value 'DEFAULT'
            if value.upper() == "DEFAULT":
                query = f"ALTER SYSTEM SET {variable_name} TO DEFAULT"
            else:
                # Escape single quotes to prevent SQL injection
                escaped_value = _escape_sql_string(value)
                query = f"ALTER SYSTEM SET {variable_name} TO '{escaped_value}'"

            rw.execute(query)
            return f"Successfully altered system default for {variable_name} to {value}. This will apply to new sessions."
        except Exception as e:
            return f"Error altering system variable: {str(e)}"
