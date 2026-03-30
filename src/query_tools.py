from risingwave import OutputFormat
from connection import setup_risingwave_connection
from sql_utils import validate_identifier, escape_sql_string
import json


def run_select_query(query: str) -> str:
    """
    Execute a SELECT query against the RisingWave database.

    Args:
        query: The SELECT SQL query to execute (must start with SELECT or WITH)

    Returns:
        Query results as a JSON-formatted string
    """
    query_upper = query.strip().upper()
    if not query_upper.startswith('SELECT') and not query_upper.startswith('WITH'):
        return "Error: Only SELECT/WITH queries are allowed for security reasons"

    rw = setup_risingwave_connection()
    try:
        result = rw.fetch(query, format=OutputFormat.DATAFRAME)
        records = result.to_dict(orient='records')
        return json.dumps(records, default=str, ensure_ascii=False, indent=2)
    except Exception as e:
        return f"Error executing SELECT query: {str(e)}"


def table_row_count(table_name: str) -> str:
    """
    Get the row count for a specific table.

    Args:
        table_name: Name of the table

    Returns:
        Row count as a JSON-formatted string
    """
    try:
        validate_identifier(table_name, "table_name")
    except ValueError as e:
        return f"Error: {str(e)}"
    rw = setup_risingwave_connection()
    query = f"SELECT COUNT(*) as row_count FROM {table_name}"
    try:
        result = rw.fetch(query, format=OutputFormat.DATAFRAME)
        records = result.to_dict(orient='records')
        return json.dumps(records, default=str, ensure_ascii=False)
    except Exception as e:
        return f"Error getting row count for table {table_name}: {str(e)}"


def get_table_stats(table_name: str, schema_name: str = "public") -> str:
    """
    Get comprehensive statistics for a table.

    Args:
        table_name: Name of the table
        schema_name: Name of the schema (default: "public")

    Returns:
        Table statistics including row count and column information
    """
    try:
        validate_identifier(table_name, "table_name")
        validate_identifier(schema_name, "schema_name")
    except ValueError as e:
        return f"Error: {str(e)}"

    safe_table = escape_sql_string(table_name)
    safe_schema = escape_sql_string(schema_name)

    rw = setup_risingwave_connection()

    try:
        row_count_query = f"SELECT COUNT(*) as row_count FROM {schema_name}.{table_name}"
        row_count = rw.fetchone(row_count_query, format=OutputFormat.DATAFRAME)

        column_query = f"""
        SELECT
            COUNT(*) as column_count,
            STRING_AGG(column_name, ', ') as column_names
        FROM information_schema.columns
        WHERE table_name = '{safe_table}' AND table_schema = '{safe_schema}'
        """
        column_info = rw.fetchone(column_query, format=OutputFormat.DATAFRAME)

        stats = {
            "table": f"{schema_name}.{table_name}",
            "row_count": row_count,
            "column_info": column_info
        }

        return json.dumps(stats, default=str, ensure_ascii=False, indent=2)
    except Exception as e:
        return f"Error getting table stats for {table_name}: {str(e)}"
