from fastmcp import FastMCP
from risingwave import OutputFormat
from connection import setup_risingwave_connection


def register_schema_tools(mcp: FastMCP):
    """Register all schema-related MCP tools"""

    @mcp.tool
    def show_tables() -> str:
        """List all tables in the database."""
        rw = setup_risingwave_connection()
        try:
            result = rw.fetch("SHOW TABLES", format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error listing tables: {str(e)}"

    @mcp.tool
    def list_databases() -> str:
        """List all databases in the RisingWave cluster."""
        rw = setup_risingwave_connection()
        try:
            result = rw.fetch("SHOW DATABASES", format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error listing databases: {str(e)}" 

    @mcp.tool
    def describe_table(table_name: str) -> str:
        """
        Describe the structure of a table (columns, types, etc.).

        Args:
            table_name: Name of the table to describe

        Returns:
            Table structure information
        """
        rw = setup_risingwave_connection()
        query = f"DESCRIBE {table_name}"
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error describing table {table_name}: {str(e)}"

    @mcp.tool
    def describe_materialized_view(mv_name: str) -> str:
        """
        Describe the structure of a materialized view (columns, types, etc.).

        Args:
            mv_name: Name of the table to describe

        Returns:
            Table structure information
        """
        rw = setup_risingwave_connection()
        query = f"DESCRIBE {mv_name}"
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error describing materialized view {mv_name}: {str(e)}"

    @mcp.tool
    def show_create_table(table_name: str) -> str:
        """
        Show the CREATE TABLE statement for a specific table.

        Args:
            table_name: Name of the table

        Returns:
            CREATE TABLE statement
        """
        rw = setup_risingwave_connection()
        query = f"SHOW CREATE TABLE {table_name}"
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error showing create table for {table_name}: {str(e)}"

    @mcp.tool
    def show_create_materialized_view(mv_name: str) -> str:
        """
        Show the CREATE MATERIALIZED VIEW statement for a specific materialized view.

        Args:
            mv_name: Name of the materialized view

        Returns:
            CREATE MATERIALIZED VIEW statement
        """
        rw = setup_risingwave_connection()
        query = f"SHOW CREATE MATERIALIZED VIEW {mv_name}"
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error showing create materialized view for {mv_name}: {str(e)}"

    @mcp.tool
    def check_table_exists(table_name: str, schema_name: str = "public") -> str:
        """
        Check if a table or materialized view exists in the specified schema.

        Args:
            table_name: Name of the table to check
            schema_name: Name of the schema (default: "public")

        Returns:
            Boolean result as string indicating if table exists
        """
        rw = setup_risingwave_connection()
        try:
            exists = rw.check_exist(name=table_name, schema_name=schema_name)
            return f"Table '{table_name}' in schema '{schema_name}' exists: {exists}"
        except Exception as e:
            return f"Error checking if table exists: {str(e)}"

    @mcp.tool
    def list_schemas() -> str:
        """
        List all schemas in the RisingWave database.

        Returns:
            List of schemas as a formatted string
        """
        rw = setup_risingwave_connection()
        try:
            result = rw.fetch(
                "SELECT schema_name FROM information_schema.schemata", format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error listing schemas: {str(e)}"

    @mcp.tool
    def list_materialized_views(schema_name: str = "public") -> str:
        """
        List all materialized views in a specific schema.

        Args:
            schema_name: Name of the schema (default: "public")

        Returns:
            List of materialized views
        """
        rw = setup_risingwave_connection()
        query = f"SHOW MATERIALIZED VIEWS FROM {schema_name}"
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error listing materialized views: {str(e)}"

    @mcp.tool
    def get_table_columns(table_name: str, schema_name: str = "public") -> str:
        """
        Get detailed column information for a table.

        Args:
            table_name: Name of the table
            schema_name: Name of the schema (default: "public")

        Returns:
            Column details including names, types, and constraints
        """
        rw = setup_risingwave_connection()
        query = f"""
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns 
        WHERE table_name = '{table_name}' AND table_schema = '{schema_name}'
        ORDER BY ordinal_position
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting table columns for {table_name}: {str(e)}"

    @mcp.tool
    def list_subscriptions(schema_name: str = "public") -> str:
        """
        List all subscriptions in a specific schema.

        Args:
            schema_name: Name of the schema (default: "public")

        Returns:
            List of subscriptions
        """
        rw = setup_risingwave_connection()
        query = f"SHOW SUBSCRIPTIONS FROM {schema_name}"
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error listing subscriptions: {str(e)}"

    @mcp.tool
    def list_table_privileges(table_name: str, schema_name: str = "public") -> str:
        """
        List privileges for a specific table.

        Args:
            table_name: Name of the table
            schema_name: Name of the schema (default: "public")

        Returns:
            Table privileges information
        """
        rw = setup_risingwave_connection()
        query = f"""
        SELECT grantee, privilege_type, is_grantable
        FROM information_schema.table_privileges 
        WHERE table_name = '{table_name}' AND table_schema = '{schema_name}'
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting table privileges: {str(e)}"
        
    @mcp.tool
    def list_sinks() -> str:
        """
        List all sinks in the RisingWave database.

        Returns:
            List of sinks as a formatted string
        """
        rw = setup_risingwave_connection()
        query = "SHOW SINKS"
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error listing sinks: {str(e)}"
        
    @mcp.tool
    def show_create_sink(sink_name: str) -> str:
        """
        Show the CREATE SINK statement for a specific sink.

        Args:
            sink_name: Name of the sink
        Returns:
            CREATE SINK statement
        """
        rw = setup_risingwave_connection()
        query = f"SHOW CREATE SINK {sink_name}"
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error showing create sink: {str(e)}"

    @mcp.tool
    def get_relation_info(relation_name: str) -> str:
        """
        Get information about a relation (table, MV, source, sink, etc.) by name.
        Useful when you don't know what type of object it is.

        Args:
            relation_name: Name of the relation to look up.

        Returns:
            Relation info including schema, type, definition, and timestamps.
        """
        rw = setup_risingwave_connection()
        # Note: Excludes 'definition' and 'fragments' columns as they can be very large
        # Use show_create_table/show_create_materialized_view for full definitions
        query = f"""
        SELECT relationid,
               schemaname,
               relationname,
               relationtype,
               relationowner,
               relationtimezone,
               initialized_at,
               created_at,
               initialized_at_cluster_version,
               created_at_cluster_version
        FROM rw_catalog.rw_relation_info
        WHERE relationname = '{relation_name}'
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting relation info: {str(e)}"

    @mcp.tool
    def show_create_source(source_name: str) -> str:
        """
        Show the CREATE SOURCE statement for a specific source.

        Args:
            source_name: Name of the source

        Returns:
            CREATE SOURCE statement
        """
        rw = setup_risingwave_connection()
        query = f"SHOW CREATE SOURCE {source_name}"
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error showing create source: {str(e)}"
