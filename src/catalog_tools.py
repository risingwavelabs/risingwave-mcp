from fastmcp import FastMCP
from risingwave import OutputFormat
from connection import setup_risingwave_connection
from sql_utils import escape_sql_string, is_valid_identifier


def register_catalog_tools(mcp: FastMCP):
    """Register all system catalog query MCP tools"""

    @mcp.tool
    def get_table_constraints(table_name: str, schema_name: str = "public") -> str:
        """
        Get all constraints for a specific table.

        Args:
            table_name: Name of the table
            schema_name: Schema name (default: "public")

        Returns:
            List of constraints including name, type, and enforcement status
        """
        rw = setup_risingwave_connection()
        # Escape string values to prevent SQL injection
        safe_table = escape_sql_string(table_name)
        safe_schema = escape_sql_string(schema_name)
        query = f"""
        SELECT constraint_name,
               constraint_type,
               is_deferrable,
               initially_deferred,
               enforced
        FROM information_schema.table_constraints
        WHERE table_name = '{safe_table}'
          AND table_schema = '{safe_schema}'
        ORDER BY constraint_type, constraint_name
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting table constraints: {str(e)}"

    @mcp.tool
    def get_views_info(schema_name: str = "public") -> str:
        """
        Get information about all views in a schema.

        Args:
            schema_name: Schema name (default: "public")

        Returns:
            List of views with their definitions
        """
        rw = setup_risingwave_connection()
        safe_schema = escape_sql_string(schema_name)
        query = f"""
        SELECT table_name as view_name,
               view_definition
        FROM information_schema.views
        WHERE table_schema = '{safe_schema}'
        ORDER BY table_name
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting views info: {str(e)}"

    @mcp.tool
    def get_all_objects_in_schema(schema_name: str = "public") -> str:
        """
        List all database objects (tables, MVs, sources, sinks, views) in a schema.

        Args:
            schema_name: Schema name (default: "public")

        Returns:
            List of all objects with their types
        """
        rw = setup_risingwave_connection()
        safe_schema = escape_sql_string(schema_name)
        query = f"""
        SELECT table_name as object_name,
               table_type as object_type
        FROM information_schema.tables
        WHERE table_schema = '{safe_schema}'
        ORDER BY table_type, table_name
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error listing objects in schema: {str(e)}"

    @mcp.tool
    def get_object_comments(object_type: str = None, object_name: str = None) -> str:
        """
        Get comments/descriptions attached to database objects.

        Args:
            object_type: Optional filter by object type ('table', 'column', etc.)
            object_name: Optional filter by object name

        Returns:
            List of object comments from rw_description catalog
        """
        rw = setup_risingwave_connection()
        query = "SELECT * FROM rw_catalog.rw_description"
        conditions = []

        if object_type:
            safe_type = escape_sql_string(object_type)
            conditions.append(f"objtype = '{safe_type}'")
        if object_name:
            safe_name = escape_sql_string(object_name)
            conditions.append(f"objname = '{safe_name}'")

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        query += " ORDER BY objtype, objname"

        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting object comments: {str(e)}"

    @mcp.tool
    def get_streaming_job_status() -> str:
        """
        Get status of all streaming jobs from the system catalog.

        Returns:
            Status information for all streaming jobs
        """
        rw = setup_risingwave_connection()
        query = """
        SELECT relationid as id,
               schemaname as schema,
               relationname as name,
               relationtype as type,
               relationowner as owner,
               initialized_at,
               created_at
        FROM rw_catalog.rw_relation_info
        WHERE relationtype IN ('materialized view', 'table', 'sink', 'source', 'index')
        ORDER BY relationtype, relationname
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting streaming job status: {str(e)}"

    @mcp.tool
    def get_system_tables_info() -> str:
        """
        List all available system tables and views in rw_catalog.

        Returns:
            List of system tables/views that can be queried
        """
        rw = setup_risingwave_connection()
        query = """
        SELECT table_name,
               table_type
        FROM information_schema.tables
        WHERE table_schema = 'rw_catalog'
        ORDER BY table_name
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error listing system tables: {str(e)}"

    @mcp.tool
    def query_system_catalog(catalog_table: str, limit: int = 100) -> str:
        """
        Query a specific system catalog table.

        Args:
            catalog_table: Name of the system catalog table (e.g., 'rw_relations', 'rw_actors')
            limit: Maximum number of rows to return (default: 100)

        Returns:
            Query results from the system catalog table
        """
        rw = setup_risingwave_connection()
        # Security: only allow querying rw_catalog and information_schema
        allowed_prefixes = ['rw_catalog.', 'information_schema.', 'pg_catalog.']

        if not any(catalog_table.startswith(prefix) for prefix in allowed_prefixes):
            # Assume rw_catalog if no schema specified
            if '.' not in catalog_table:
                catalog_table = f"rw_catalog.{catalog_table}"
            else:
                return "Error: Only rw_catalog, information_schema, and pg_catalog tables can be queried"

        # Validate catalog_table is a valid identifier to prevent injection
        if not is_valid_identifier(catalog_table):
            return f"Error: Invalid catalog table name: '{catalog_table}'"

        # Ensure limit is a valid integer
        try:
            limit = int(limit)
            if limit < 1:
                limit = 100
        except (ValueError, TypeError):
            limit = 100

        query = f"SELECT * FROM {catalog_table} LIMIT {limit}"
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error querying system catalog: {str(e)}"
