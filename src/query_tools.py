from risingwave import OutputFormat
from connection import setup_risingwave_connection
import json


def run_select_query(query: str) -> str:
    """
    Execute a SELECT query against the RisingWave database.

    Args:
        query: The SELECT SQL query to execute (must start with SELECT)

    Returns:
        Query results as a JSON-formatted string
    """
    query_upper = query.strip().upper()
    if not query_upper.startswith('SELECT'):
        raise ValueError(
            "Only SELECT queries are allowed for security reasons")

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
    rw = setup_risingwave_connection()

    row_count_query = f"SELECT COUNT(*) as row_count FROM {schema_name}.{table_name}"
    row_count = rw.fetchone(row_count_query, format=OutputFormat.DATAFRAME)

    column_query = f"""
    SELECT 
        COUNT(*) as column_count,
        STRING_AGG(column_name, ', ') as column_names
    FROM information_schema.columns 
    WHERE table_name = '{table_name}' AND table_schema = '{schema_name}'
    """
    try:
        column_info = rw.fetchone(column_query, format=OutputFormat.DATAFRAME)
    except Exception as e:
        return f"Error getting column info for table {table_name}: {str(e)}"

    stats = {
        "table": f"{schema_name}.{table_name}",
        "row_count": row_count,
        "column_info": column_info
    }

    return str(stats)
