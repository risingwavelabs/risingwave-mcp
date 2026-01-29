from fastmcp import FastMCP
from connection import setup_risingwave_connection


def register_iceberg_tools(mcp: FastMCP):
    """Register all Iceberg-related MCP tools"""

    @mcp.tool
    def vacuum_table(table_name: str, schema_name: str = "public") -> str:
        """
        Expire old snapshots on an Iceberg table or sink, freeing up space.
        This removes outdated versions without rewriting the table.

        Args:
            table_name: Name of the Iceberg table or sink
            schema_name: Schema name (default: "public")

        Returns:
            Success or error message
        """
        rw = setup_risingwave_connection()
        query = f"VACUUM {schema_name}.{table_name}"
        try:
            rw.execute(query)
            return f"VACUUM completed successfully on '{schema_name}.{table_name}'"
        except Exception as e:
            return f"Error running VACUUM: {str(e)}"

    @mcp.tool
    def vacuum_full_table(table_name: str, schema_name: str = "public") -> str:
        """
        Perform full compaction on an Iceberg table followed by snapshot expiration.
        This rewrites the table to reclaim space more aggressively.

        Note: VACUUM FULL requires extra disk space temporarily as a new copy
        is written before the old copy is released.

        Args:
            table_name: Name of the Iceberg table or sink
            schema_name: Schema name (default: "public")

        Returns:
            Success or error message
        """
        rw = setup_risingwave_connection()
        query = f"VACUUM FULL {schema_name}.{table_name}"
        try:
            rw.execute(query)
            return f"VACUUM FULL completed successfully on '{schema_name}.{table_name}'"
        except Exception as e:
            return f"Error running VACUUM FULL: {str(e)}"
