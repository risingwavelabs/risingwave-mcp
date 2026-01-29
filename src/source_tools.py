from fastmcp import FastMCP
from risingwave import OutputFormat
from connection import setup_risingwave_connection


def register_source_tools(mcp: FastMCP):
    """Register all source-related MCP tools"""

    @mcp.tool
    def list_sources(schema_name: str = None) -> str:
        """
        List all sources in the database.

        Args:
            schema_name: Optional schema name to filter sources. If not specified,
                        lists sources from all schemas in the current search_path.

        Returns:
            List of sources as a formatted string
        """
        rw = setup_risingwave_connection()
        query = f"SHOW SOURCES FROM {schema_name}" if schema_name else "SHOW SOURCES"
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error listing sources: {str(e)}"

    @mcp.tool
    def describe_source(source_name: str) -> str:
        """
        Describe the structure of a source (columns, types, etc.).

        Args:
            source_name: Name of the source to describe

        Returns:
            Source structure information
        """
        rw = setup_risingwave_connection()
        query = f"DESCRIBE {source_name}"
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error describing source {source_name}: {str(e)}"

    @mcp.tool
    def alter_source_parallelism(source_name: str, parallelism: str) -> str:
        """
        Change the parallelism of a source.

        Args:
            source_name: Name of the source to alter
            parallelism: Either 'ADAPTIVE' or a fixed number (e.g., '4')

        Returns:
            Success or error message
        """
        rw = setup_risingwave_connection()
        # Validate parallelism value
        if parallelism.upper() != 'ADAPTIVE' and not parallelism.isdigit():
            return "Error: parallelism must be 'ADAPTIVE' or a positive integer"

        parallelism_value = parallelism.upper() if parallelism.upper() == 'ADAPTIVE' else parallelism
        query = f"ALTER SOURCE {source_name} SET PARALLELISM = {parallelism_value}"
        try:
            rw.execute(query)
            return f"Source '{source_name}' parallelism set to {parallelism_value}"
        except Exception as e:
            return f"Error altering source parallelism: {str(e)}"

    @mcp.tool
    def alter_source_rate_limit(source_name: str, rate_limit: str) -> str:
        """
        Modify the ingestion rate limit for a source.

        Args:
            source_name: Name of the source to alter
            rate_limit: Either 'default' or a numeric value (records per second)

        Returns:
            Success or error message
        """
        rw = setup_risingwave_connection()
        # Validate rate_limit value
        if rate_limit.lower() != 'default' and not rate_limit.isdigit():
            return "Error: rate_limit must be 'default' or a positive integer"

        rate_value = 'default' if rate_limit.lower() == 'default' else rate_limit
        query = f"ALTER SOURCE {source_name} SET SOURCE_RATE_LIMIT = {rate_value}"
        try:
            rw.execute(query)
            return f"Source '{source_name}' rate limit set to {rate_value}"
        except Exception as e:
            return f"Error altering source rate limit: {str(e)}"

    @mcp.tool
    def refresh_source_schema(source_name: str) -> str:
        """
        Fetch the latest schema from the schema registry and update the source schema.
        Only works for sources that use a schema registry.

        Args:
            source_name: Name of the source to refresh

        Returns:
            Success or error message
        """
        rw = setup_risingwave_connection()
        query = f"ALTER SOURCE {source_name} REFRESH SCHEMA"
        try:
            rw.execute(query)
            return f"Source '{source_name}' schema refreshed successfully"
        except Exception as e:
            return f"Error refreshing source schema: {str(e)}"

    @mcp.tool
    def rename_source(source_name: str, new_name: str) -> str:
        """
        Rename a source.

        Args:
            source_name: Current name of the source
            new_name: New name for the source

        Returns:
            Success or error message
        """
        rw = setup_risingwave_connection()
        query = f"ALTER SOURCE {source_name} RENAME TO {new_name}"
        try:
            rw.execute(query)
            return f"Source '{source_name}' renamed to '{new_name}'"
        except Exception as e:
            return f"Error renaming source: {str(e)}"

    @mcp.tool
    def drop_source(source_name: str, if_exists: bool = False, cascade: bool = False) -> str:
        """
        Drop a source from the database.

        Args:
            source_name: Name of the source to drop
            if_exists: If True, do not error if source does not exist
            cascade: If True, also drop dependent objects (materialized views, etc.)

        Returns:
            Success or error message
        """
        rw = setup_risingwave_connection()
        if_exists_clause = "IF EXISTS " if if_exists else ""
        cascade_clause = " CASCADE" if cascade else ""
        query = f"DROP SOURCE {if_exists_clause}{source_name}{cascade_clause}"
        try:
            rw.execute(query)
            return f"Source '{source_name}' dropped successfully"
        except Exception as e:
            return f"Error dropping source: {str(e)}"
