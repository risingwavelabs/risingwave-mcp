from fastmcp import FastMCP
from risingwave import OutputFormat
from connection import setup_risingwave_connection


def register_function_tools(mcp: FastMCP):
    """Register all user-defined function (UDF) related MCP tools"""

    @mcp.tool
    def list_functions(schema_name: str = None) -> str:
        """
        List all user-defined functions in the database.

        Args:
            schema_name: Optional schema name to filter functions. If not specified,
                        lists functions from all schemas in the current search_path.

        Returns:
            List of functions with their names, arguments, return types, language, and server link
        """
        rw = setup_risingwave_connection()
        query = f"SHOW FUNCTIONS FROM {schema_name}" if schema_name else "SHOW FUNCTIONS"
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error listing functions: {str(e)}"

    @mcp.tool
    def show_create_function(function_name: str, argument_types: str = None) -> str:
        """
        Show the CREATE FUNCTION statement for a specific function.

        Args:
            function_name: Name of the function
            argument_types: Optional comma-separated argument types (e.g., "int, varchar")
                           to disambiguate overloaded functions

        Returns:
            CREATE FUNCTION statement
        """
        rw = setup_risingwave_connection()
        if argument_types:
            query = f"SHOW CREATE FUNCTION {function_name}({argument_types})"
        else:
            query = f"SHOW CREATE FUNCTION {function_name}"
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error showing create function: {str(e)}"

    @mcp.tool
    def drop_function(function_name: str, argument_types: str = None,
                      if_exists: bool = False, cascade: bool = False) -> str:
        """
        Drop a user-defined function.

        Args:
            function_name: Name of the function to drop
            argument_types: Optional comma-separated argument types (e.g., "int, varchar")
                           to disambiguate overloaded functions. If the function name is
                           unique in its schema, this can be omitted.
            if_exists: If True, do not error if function does not exist
            cascade: If True, also drop dependent objects (like materialized views)

        Returns:
            Success or error message
        """
        rw = setup_risingwave_connection()
        if_exists_clause = "IF EXISTS " if if_exists else ""
        cascade_clause = " CASCADE" if cascade else ""

        if argument_types:
            query = f"DROP FUNCTION {if_exists_clause}{function_name}({argument_types}){cascade_clause}"
        else:
            query = f"DROP FUNCTION {if_exists_clause}{function_name}{cascade_clause}"

        try:
            rw.execute(query)
            return f"Function '{function_name}' dropped successfully"
        except Exception as e:
            return f"Error dropping function: {str(e)}"
