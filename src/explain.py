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


def explain_distsql(statement: str, verbose: bool = False) -> str:
    """
    Execute EXPLAIN (DISTSQL) on a streaming statement to get the distributed execution plan.

    This shows the distributed query plan with state information, useful for understanding
    how streaming jobs are executed across the cluster. Use this for CREATE MATERIALIZED VIEW,
    CREATE SINK, or other streaming DDL statements.

    Args:
        statement: The streaming SQL statement (e.g., CREATE MATERIALIZED VIEW mv AS SELECT ...)
        verbose: If True, include additional details like stream keys (default: False)

    Returns:
        Distributed execution plan with state table information
    """
    statement_upper = statement.strip().upper()

    # Allow CREATE statements for streaming jobs
    allowed_prefixes = ['CREATE MATERIALIZED VIEW', 'CREATE SINK', 'CREATE INDEX',
                        'CREATE TABLE', 'SELECT', 'WITH']

    if not any(statement_upper.startswith(prefix) for prefix in allowed_prefixes):
        raise ValueError(
            "Only CREATE MATERIALIZED VIEW, CREATE SINK, CREATE INDEX, CREATE TABLE, "
            "SELECT, or WITH statements are allowed for EXPLAIN (DISTSQL)")

    rw = setup_risingwave_connection()
    try:
        if verbose:
            explain_stmt = f"EXPLAIN (DISTSQL, VERBOSE) {statement}"
        else:
            explain_stmt = f"EXPLAIN (DISTSQL) {statement}"
        result = rw.fetch(explain_stmt, format=OutputFormat.DATAFRAME)
        return result.to_json()
    except Exception as e:
        return f"Error executing EXPLAIN (DISTSQL): {str(e)}"
