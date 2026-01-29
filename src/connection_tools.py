from fastmcp import FastMCP
from risingwave import OutputFormat
from connection import setup_risingwave_connection


def register_connection_tools(mcp: FastMCP):
    """Register all connection-related MCP tools"""

    @mcp.tool
    def list_connections(schema_name: str = None) -> str:
        """
        List all connections in the database.

        Args:
            schema_name: Optional schema name to filter connections. If not specified,
                        lists connections from all schemas in the current search_path.

        Returns:
            List of connections with their types and properties
        """
        rw = setup_risingwave_connection()
        query = f"SHOW CONNECTIONS FROM {schema_name}" if schema_name else "SHOW CONNECTIONS"
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error listing connections: {str(e)}"

    @mcp.tool
    def show_create_connection(connection_name: str) -> str:
        """
        Show the CREATE CONNECTION statement for a specific connection.

        Args:
            connection_name: Name of the connection

        Returns:
            CREATE CONNECTION statement
        """
        rw = setup_risingwave_connection()
        query = f"SHOW CREATE CONNECTION {connection_name}"
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error showing create connection: {str(e)}"

    @mcp.tool
    def drop_connection(connection_name: str, if_exists: bool = False, cascade: bool = False) -> str:
        """
        Drop a connection from the database.
        Note: All dependent sources and sinks must be removed first, unless CASCADE is used.
        CASCADE is not supported for Iceberg connections.

        Args:
            connection_name: Name of the connection to drop
            if_exists: If True, do not error if connection does not exist
            cascade: If True, also drop dependent objects (not supported for Iceberg)

        Returns:
            Success or error message
        """
        rw = setup_risingwave_connection()
        if_exists_clause = "IF EXISTS " if if_exists else ""
        cascade_clause = " CASCADE" if cascade else ""
        query = f"DROP CONNECTION {if_exists_clause}{connection_name}{cascade_clause}"
        try:
            rw.execute(query)
            return f"Connection '{connection_name}' dropped successfully"
        except Exception as e:
            return f"Error dropping connection: {str(e)}"
