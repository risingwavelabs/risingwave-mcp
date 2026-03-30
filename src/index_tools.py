from fastmcp import FastMCP
from risingwave import OutputFormat
from connection import setup_risingwave_connection
from sql_utils import validate_identifier


def register_index_tools(mcp: FastMCP):
    """Register all index-related MCP tools"""

    @mcp.tool
    def list_indexes(table_name: str) -> str:
        """
        List all indexes for a specific table.

        Args:
            table_name: Name of the table to list indexes for

        Returns:
            List of indexes with their details (name, key columns, included columns, distribution)
        """
        try:
            validate_identifier(table_name, "table_name")
        except ValueError as e:
            return f"Error: {str(e)}"
        rw = setup_risingwave_connection()
        query = f"SHOW INDEXES FROM {table_name}"
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error listing indexes for table {table_name}: {str(e)}"

    @mcp.tool
    def describe_index(index_name: str) -> str:
        """
        Describe the structure of an index.

        Args:
            index_name: Name of the index to describe

        Returns:
            Index structure information
        """
        try:
            validate_identifier(index_name, "index_name")
        except ValueError as e:
            return f"Error: {str(e)}"
        rw = setup_risingwave_connection()
        query = f"DESCRIBE {index_name}"
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error describing index {index_name}: {str(e)}"

    @mcp.tool
    def show_create_index(index_name: str) -> str:
        """
        Show the CREATE INDEX statement for a specific index.

        Args:
            index_name: Name of the index

        Returns:
            CREATE INDEX statement
        """
        try:
            validate_identifier(index_name, "index_name")
        except ValueError as e:
            return f"Error: {str(e)}"
        rw = setup_risingwave_connection()
        query = f"SHOW CREATE INDEX {index_name}"
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error showing create index: {str(e)}"

    @mcp.tool
    def drop_index(index_name: str, if_exists: bool = False, cascade: bool = False) -> str:
        """
        Drop an index from a table.

        Args:
            index_name: Name of the index to drop
            if_exists: If True, do not error if index does not exist
            cascade: If True, also drop dependent objects

        Returns:
            Success or error message
        """
        try:
            validate_identifier(index_name, "index_name")
        except ValueError as e:
            return f"Error: {str(e)}"
        rw = setup_risingwave_connection()
        if_exists_clause = "IF EXISTS " if if_exists else ""
        cascade_clause = " CASCADE" if cascade else ""
        query = f"DROP INDEX {if_exists_clause}{index_name}{cascade_clause}"
        try:
            rw.execute(query)
            return f"Index '{index_name}' dropped successfully"
        except Exception as e:
            return f"Error dropping index: {str(e)}"
