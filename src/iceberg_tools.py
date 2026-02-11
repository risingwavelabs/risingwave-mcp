from fastmcp import FastMCP
from risingwave import OutputFormat
from connection import setup_risingwave_connection
from sql_utils import is_valid_identifier, escape_sql_string


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

    @mcp.tool
    def get_iceberg_snapshots(source_name: str) -> str:
        """
        Get all snapshots of an Iceberg source.
        Returns snapshot metadata including IDs, timestamps, and operations.

        Args:
            source_name: Name of the Iceberg source

        Returns:
            Snapshot information as JSON or error message
        """
        rw = setup_risingwave_connection()

        if not is_valid_identifier(source_name):
            return f"Error: Invalid source name: '{source_name}'"

        query = f"SELECT * FROM {source_name}$snapshots"
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting Iceberg snapshots: {str(e)}"

    @mcp.tool
    def get_iceberg_files(source_name: str) -> str:
        """
        Get data files of an Iceberg source.
        Returns file metadata including paths, sizes, and record counts.

        Args:
            source_name: Name of the Iceberg source

        Returns:
            File information as JSON or error message
        """
        rw = setup_risingwave_connection()

        if not is_valid_identifier(source_name):
            return f"Error: Invalid source name: '{source_name}'"

        query = f"SELECT * FROM {source_name}$files"
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting Iceberg files: {str(e)}"

    @mcp.tool
    def get_iceberg_manifests(source_name: str) -> str:
        """
        Get manifest files of an Iceberg source.
        Manifests track data files and their metadata.

        Args:
            source_name: Name of the Iceberg source

        Returns:
            Manifest information as JSON or error message
        """
        rw = setup_risingwave_connection()

        if not is_valid_identifier(source_name):
            return f"Error: Invalid source name: '{source_name}'"

        query = f"SELECT * FROM {source_name}$manifests"
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting Iceberg manifests: {str(e)}"

    @mcp.tool
    def get_iceberg_history(source_name: str) -> str:
        """
        Get the history of an Iceberg source.
        Shows table evolution including schema changes and operations.

        Args:
            source_name: Name of the Iceberg source

        Returns:
            History information as JSON or error message
        """
        rw = setup_risingwave_connection()

        if not is_valid_identifier(source_name):
            return f"Error: Invalid source name: '{source_name}'"

        query = f"SELECT * FROM {source_name}$history"
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting Iceberg history: {str(e)}"

    @mcp.tool
    def get_iceberg_refresh_state() -> str:
        """
        Get the refresh state of Iceberg tables using full reload mode.
        Shows which tables are refreshing, their status, and timing.

        Returns:
            Refresh state information as JSON or error message
        """
        rw = setup_risingwave_connection()

        query = "SELECT * FROM rw_catalog.rw_refresh_table_state"
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting Iceberg refresh state: {str(e)}"

    @mcp.tool
    def create_iceberg_sink(
        sink_name: str,
        source_object: str,
        warehouse_path: str,
        database_name: str,
        table_name: str,
        catalog_type: str = "storage",
        sink_type: str = "append-only",
        primary_key: str = None,
        partition_by: str = None,
        force_append_only: bool = False,
        create_table_if_not_exists: bool = False,
        commit_checkpoint_interval: int = None,
        s3_region: str = None,
        s3_access_key: str = None,
        s3_secret_key: str = None
    ) -> str:
        """
        Create an Iceberg sink to stream data from RisingWave to an Iceberg table.

        Args:
            sink_name: Name for the sink
            source_object: Source table, MV, or query to sink from
            warehouse_path: S3 path to Iceberg warehouse (e.g., 's3://bucket/warehouse')
            database_name: Iceberg database/namespace name
            table_name: Iceberg table name
            catalog_type: Catalog type - 'storage', 'rest', or 'glue' (default: 'storage')
            sink_type: 'append-only' or 'upsert' (default: 'append-only')
            primary_key: Required for upsert - comma-separated key columns
            partition_by: Optional partition columns (e.g., 'date,region')
            force_append_only: Convert updates/deletes to inserts (default: false)
            create_table_if_not_exists: Auto-create table if missing (default: false)
            commit_checkpoint_interval: Commit frequency in seconds (default: 60)
            s3_region: AWS S3 region
            s3_access_key: AWS access key (consider using secrets instead)
            s3_secret_key: AWS secret key (consider using secrets instead)

        Returns:
            Success message with CREATE SINK statement or error message
        """
        rw = setup_risingwave_connection()

        if not is_valid_identifier(sink_name):
            return f"Error: Invalid sink name: '{sink_name}'"

        if sink_type == "upsert" and not primary_key:
            return "Error: primary_key is required for upsert sink type"

        # Build WITH clause parameters
        params = [
            "connector = 'iceberg'",
            f"type = '{sink_type}'",
            f"warehouse.path = '{escape_sql_string(warehouse_path)}'",
            f"database.name = '{escape_sql_string(database_name)}'",
            f"table.name = '{escape_sql_string(table_name)}'",
            f"catalog.type = '{escape_sql_string(catalog_type)}'"
        ]

        if primary_key:
            params.append(f"primary_key = '{escape_sql_string(primary_key)}'")
        if partition_by:
            params.append(f"partition_by = '{escape_sql_string(partition_by)}'")
        if force_append_only:
            params.append("force_append_only = true")
        if create_table_if_not_exists:
            params.append("create_table_if_not_exists = true")
        if commit_checkpoint_interval is not None:
            params.append(f"commit_checkpoint_interval = {int(commit_checkpoint_interval)}")
        if s3_region:
            params.append(f"s3.region = '{escape_sql_string(s3_region)}'")
        if s3_access_key:
            params.append(f"s3.access.key = '{escape_sql_string(s3_access_key)}'")
        if s3_secret_key:
            params.append(f"s3.secret.key = '{escape_sql_string(s3_secret_key)}'")

        query = f"""CREATE SINK {sink_name} FROM {source_object}
WITH (
    {',\n    '.join(params)}
)"""

        try:
            rw.execute(query)
            return f"Iceberg sink '{sink_name}' created successfully.\n\nStatement:\n{query}"
        except Exception as e:
            return f"Error creating Iceberg sink: {str(e)}\n\nAttempted statement:\n{query}"

    @mcp.tool
    def create_iceberg_source(
        source_name: str,
        warehouse_path: str,
        database_name: str,
        table_name: str,
        catalog_type: str = "storage",
        s3_region: str = None,
        s3_access_key: str = None,
        s3_secret_key: str = None
    ) -> str:
        """
        Create an Iceberg source to read data from an Iceberg table.

        Args:
            source_name: Name for the source
            warehouse_path: S3 path to Iceberg warehouse (e.g., 's3://bucket/warehouse')
            database_name: Iceberg database/namespace name
            table_name: Iceberg table name
            catalog_type: Catalog type - 'storage', 'rest', or 'glue' (default: 'storage')
            s3_region: AWS S3 region
            s3_access_key: AWS access key (consider using secrets instead)
            s3_secret_key: AWS secret key (consider using secrets instead)

        Returns:
            Success message with CREATE SOURCE statement or error message
        """
        rw = setup_risingwave_connection()

        if not is_valid_identifier(source_name):
            return f"Error: Invalid source name: '{source_name}'"

        # Build WITH clause parameters
        params = [
            "connector = 'iceberg'",
            f"warehouse.path = '{escape_sql_string(warehouse_path)}'",
            f"database.name = '{escape_sql_string(database_name)}'",
            f"table.name = '{escape_sql_string(table_name)}'",
            f"catalog.type = '{escape_sql_string(catalog_type)}'"
        ]

        if s3_region:
            params.append(f"s3.region = '{escape_sql_string(s3_region)}'")
        if s3_access_key:
            params.append(f"s3.access.key = '{escape_sql_string(s3_access_key)}'")
        if s3_secret_key:
            params.append(f"s3.secret.key = '{escape_sql_string(s3_secret_key)}'")

        query = f"""CREATE SOURCE {source_name}
WITH (
    {',\n    '.join(params)}
)"""

        try:
            rw.execute(query)
            return f"Iceberg source '{source_name}' created successfully.\n\nStatement:\n{query}"
        except Exception as e:
            return f"Error creating Iceberg source: {str(e)}\n\nAttempted statement:\n{query}"
