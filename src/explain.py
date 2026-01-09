from risingwave import OutputFormat
from connection import setup_risingwave_connection


def explain_analyze(query: str) -> str:
    """
    Execute EXPLAIN ANALYZE on a query to get detailed execution statistics.

    EXPLAIN ANALYZE analyzes a running stream job, including table, materialized view, 
    sink, or index, and collects runtime statistics to help identify performance bottlenecks.

    Args:
        query: The SQL query to analyze streaming job (TABLE, MATERIALIZED VIEW, SINK, INDEX, or ID)

    Returns:
        Query execution plan with detailed runtime statistics
    """
    query_upper = query.strip().upper()
    allowed_keywords = ["TABLE", "MATERIALIZED VIEW", "SINK", "INDEX"]

    if not any(query_upper.startswith(keyword) for keyword in allowed_keywords):
        raise ValueError(
            "Only TABLE, MATERIALIZED VIEW, SINK, INDEX, or ID queries are allowed for EXPLAIN ANALYZE")

    rw = setup_risingwave_connection()
    try:
        explain_query = f"EXPLAIN ANALYZE {query}"
        result = rw.fetch(explain_query, format=OutputFormat.DATAFRAME)
        return result.to_json()
    except Exception as e:
        return f"Error executing EXPLAIN ANALYZE: {str(e)}"


def explain_query(query: str) -> str:
    """
    Execute EXPLAIN on a query to get the execution plan without running it.

    EXPLAIN shows the query execution plan without actually executing the query,
    providing estimated costs and row counts.

    Args:
        query: The SQL query to explain (SELECT, INSERT, UPDATE, DELETE)

    Returns:
        Query execution plan
    """
    query_upper = query.strip().upper()
    allowed_queries = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'WITH']

    if not any(query_upper.startswith(keyword) for keyword in allowed_queries):
        raise ValueError(
            "Only SELECT, INSERT, UPDATE, DELETE, and WITH queries are allowed for EXPLAIN")

    dangerous_keywords = ['DROP', 'TRUNCATE', 'ALTER', 'CREATE']
    if any(keyword in query_upper for keyword in dangerous_keywords):
        raise ValueError("Queries containing DDL operations are not allowed")

    rw = setup_risingwave_connection()
    try:
        explain_query = f"EXPLAIN {query}"
        result = rw.fetch(explain_query, format=OutputFormat.DATAFRAME)
        return result.to_json()
    except Exception as e:
        return f"Error executing EXPLAIN: {str(e)}"
