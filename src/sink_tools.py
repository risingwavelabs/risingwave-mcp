from fastmcp import FastMCP
from risingwave import OutputFormat
from connection import setup_risingwave_connection


def register_sink_tools(mcp: FastMCP):
    """Register all sink-related MCP tools"""

    @mcp.tool
    def describe_sink(sink_name: str) -> str:
        """
        Describe the structure of a sink (columns, types, etc.).

        Args:
            sink_name: Name of the sink to describe

        Returns:
            Sink structure information
        """
        rw = setup_risingwave_connection()
        query = f"DESCRIBE {sink_name}"
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error describing sink {sink_name}: {str(e)}"

    @mcp.tool
    def alter_sink_parallelism(sink_name: str, parallelism: str) -> str:
        """
        Change the parallelism of a sink.

        Args:
            sink_name: Name of the sink to alter
            parallelism: Either 'ADAPTIVE' or a fixed number (e.g., '4'), or '0' for ADAPTIVE

        Returns:
            Success or error message
        """
        rw = setup_risingwave_connection()
        # Validate parallelism value
        if parallelism.upper() != 'ADAPTIVE' and not parallelism.isdigit():
            return "Error: parallelism must be 'ADAPTIVE' or a non-negative integer"

        parallelism_value = parallelism.upper() if parallelism.upper() == 'ADAPTIVE' else parallelism
        query = f"ALTER SINK {sink_name} SET PARALLELISM = {parallelism_value}"
        try:
            rw.execute(query)
            return f"Sink '{sink_name}' parallelism set to {parallelism_value}"
        except Exception as e:
            return f"Error altering sink parallelism: {str(e)}"

    @mcp.tool
    def alter_sink_rate_limit(sink_name: str, rate_limit: str) -> str:
        """
        Modify the output rate limit for a sink.

        Args:
            sink_name: Name of the sink to alter
            rate_limit: Either 'default' or a numeric value (records per second)

        Returns:
            Success or error message
        """
        rw = setup_risingwave_connection()
        # Validate rate_limit value
        if rate_limit.lower() != 'default' and not rate_limit.isdigit():
            return "Error: rate_limit must be 'default' or a positive integer"

        rate_value = 'default' if rate_limit.lower() == 'default' else rate_limit
        query = f"ALTER SINK {sink_name} SET SINK_RATE_LIMIT = {rate_value}"
        try:
            rw.execute(query)
            return f"Sink '{sink_name}' rate limit set to {rate_value}"
        except Exception as e:
            return f"Error altering sink rate limit: {str(e)}"

    @mcp.tool
    def rename_sink(sink_name: str, new_name: str) -> str:
        """
        Rename a sink.

        Args:
            sink_name: Current name of the sink
            new_name: New name for the sink

        Returns:
            Success or error message
        """
        rw = setup_risingwave_connection()
        query = f"ALTER SINK {sink_name} RENAME TO {new_name}"
        try:
            rw.execute(query)
            return f"Sink '{sink_name}' renamed to '{new_name}'"
        except Exception as e:
            return f"Error renaming sink: {str(e)}"

    @mcp.tool
    def drop_sink(sink_name: str, if_exists: bool = False, cascade: bool = False) -> str:
        """
        Drop a sink from the database.

        Args:
            sink_name: Name of the sink to drop
            if_exists: If True, do not error if sink does not exist
            cascade: If True, also drop dependent objects

        Returns:
            Success or error message
        """
        rw = setup_risingwave_connection()
        if_exists_clause = "IF EXISTS " if if_exists else ""
        cascade_clause = " CASCADE" if cascade else ""
        query = f"DROP SINK {if_exists_clause}{sink_name}{cascade_clause}"
        try:
            rw.execute(query)
            return f"Sink '{sink_name}' dropped successfully"
        except Exception as e:
            return f"Error dropping sink: {str(e)}"
