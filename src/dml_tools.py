from fastmcp import FastMCP
from risingwave import OutputFormat
from connection import setup_risingwave_connection
from sql_utils import is_valid_identifier


def register_dml_tools(mcp: FastMCP):
    """Register all DML-related MCP tools"""

    @mcp.tool
    def insert_single_row(table_name: str, column_data: str, schema_name: str = "public") -> str:
        """
        Insert a single row into a table.

        Args:
            table_name: Name of the table
            column_data: JSON string of column names and values (e.g., '{"col1": "value1", "col2": 123}')
            schema_name: Name of the schema (default: "public")

        Returns:
            Success or error message
        """
        import json
        rw = setup_risingwave_connection()
        try:
            # Parse the JSON column data
            column_values = json.loads(column_data)
            rw.insert_row(
                table_name=table_name,
                schema_name=schema_name,
                force_flush=True,
                **column_values
            )
            return f"Row inserted successfully into {schema_name}.{table_name}"
        except json.JSONDecodeError:
            return "Error: column_data must be valid JSON format"
        except Exception as e:
            return f"Error inserting row: {str(e)}"

    @mcp.tool
    def update_rows(table_name: str, set_clause: str, where_clause: str = None,
                    returning_columns: str = None, confirm_no_where: bool = False) -> str:
        """
        Update rows in a table.
        Note: Cannot modify primary key columns.

        WARNING: This tool accepts raw SQL clauses which could be used for SQL injection.
        Only use with trusted input.

        Args:
            table_name: Name of the table to update
            set_clause: Column assignments (e.g., "city = 'New York', status = 'active'")
            where_clause: Optional condition to filter rows (e.g., "id = 5")
                         If omitted, ALL rows will be updated!
            returning_columns: Optional columns to return (e.g., "id, city")
            confirm_no_where: Must be True to update all rows without a WHERE clause

        Returns:
            Success message or returned rows if RETURNING specified
        """
        rw = setup_risingwave_connection()

        if not is_valid_identifier(table_name):
            return f"Error: Invalid table name: '{table_name}'"

        # Safety check for mass updates
        if where_clause is None and not confirm_no_where:
            return ("Error: No WHERE clause specified. This would update ALL rows in the table. "
                    "Set confirm_no_where=True to proceed with updating all rows.")

        # Build query
        query = f"UPDATE {table_name} SET {set_clause}"
        if where_clause:
            query += f" WHERE {where_clause}"
        if returning_columns:
            query += f" RETURNING {returning_columns}"

        try:
            if returning_columns:
                result = rw.fetch(query, format=OutputFormat.DATAFRAME)
                rw.execute("FLUSH")
                return f"Update successful. Returned: {result.to_json()}"
            else:
                rw.execute(query)
                rw.execute("FLUSH")
                return f"Update executed successfully on table '{table_name}'"
        except Exception as e:
            return f"Error updating rows: {str(e)}"

    @mcp.tool
    def delete_rows(table_name: str, where_clause: str = None,
                    returning_columns: str = None, confirm_no_where: bool = False) -> str:
        """
        Delete rows from a table.

        WARNING: This tool accepts raw SQL clauses which could be used for SQL injection.
        Only use with trusted input.

        Args:
            table_name: Name of the table to delete from
            where_clause: Optional condition to filter rows (e.g., "id = 5")
                         If omitted, ALL rows will be deleted!
            returning_columns: Optional columns to return from deleted rows (e.g., "id")
            confirm_no_where: Must be True to delete all rows without a WHERE clause

        Returns:
            Success message or returned rows if RETURNING specified
        """
        rw = setup_risingwave_connection()

        if not is_valid_identifier(table_name):
            return f"Error: Invalid table name: '{table_name}'"

        # Safety check for mass deletes
        if where_clause is None and not confirm_no_where:
            return ("Error: No WHERE clause specified. This would delete ALL rows from the table. "
                    "Set confirm_no_where=True to proceed with deleting all rows.")

        # Build query
        query = f"DELETE FROM {table_name}"
        if where_clause:
            query += f" WHERE {where_clause}"
        if returning_columns:
            query += f" RETURNING {returning_columns}"

        try:
            if returning_columns:
                result = rw.fetch(query, format=OutputFormat.DATAFRAME)
                rw.execute("FLUSH")
                return f"Delete successful. Returned: {result.to_json()}"
            else:
                rw.execute(query)
                rw.execute("FLUSH")
                return f"Delete executed successfully on table '{table_name}'"
        except Exception as e:
            return f"Error deleting rows: {str(e)}"

    @mcp.tool
    def insert_multiple_rows(table_name: str, columns: str, values_list: str,
                             schema_name: str = "public") -> str:
        """
        Insert multiple rows into a table using a single INSERT statement.

        Args:
            table_name: Name of the table
            columns: Comma-separated column names (e.g., "id, name, value")
            values_list: Values in SQL format, rows separated by ), (
                        (e.g., "(1, 'Alice', 100), (2, 'Bob', 200)")
            schema_name: Name of the schema (default: "public")

        Returns:
            Success or error message
        """
        rw = setup_risingwave_connection()

        query = f"INSERT INTO {schema_name}.{table_name} ({columns}) VALUES {values_list}"
        try:
            rw.execute(query)
            rw.execute("FLUSH")
            return f"Multiple rows inserted successfully into {schema_name}.{table_name}"
        except Exception as e:
            return f"Error inserting rows: {str(e)}"
