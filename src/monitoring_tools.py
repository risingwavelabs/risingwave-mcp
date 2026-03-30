from fastmcp import FastMCP
from risingwave import OutputFormat
from connection import setup_risingwave_connection
from sql_utils import escape_sql_string


def register_monitoring_tools(mcp: FastMCP):
    """Register monitoring and diagnostics tools"""

    @mcp.tool
    def get_event_logs(event_type: str = None, limit: int = 50) -> str:
        """
        Get system event logs for debugging and auditing.
        Events include CREATE/DROP operations, barrier events, recovery events, etc.

        Args:
            event_type: Optional filter by event type (e.g., 'CREATE_STREAM_JOB', 'BARRIER_COMPLETE', 'RECOVERY')
            limit: Maximum number of events to return (default: 50)

        Returns:
            Event log entries with timestamp, type, and details
        """
        rw = setup_risingwave_connection()
        limit = max(1, min(int(limit), 500))
        if event_type:
            safe_type = escape_sql_string(event_type)
            query = f"""
            SELECT unique_id, timestamp, event_type, info
            FROM rw_catalog.rw_event_logs
            WHERE event_type = '{safe_type}'
            ORDER BY timestamp DESC
            LIMIT {limit}
            """
        else:
            query = f"""
            SELECT unique_id, timestamp, event_type, info
            FROM rw_catalog.rw_event_logs
            ORDER BY timestamp DESC
            LIMIT {limit}
            """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting event logs: {str(e)}"

    @mcp.tool
    def get_rate_limits() -> str:
        """
        Get current rate limit configuration for all fragments.
        Shows which streaming fragments have rate limits applied.

        Returns:
            Rate limit settings per fragment including fragment type and node name
        """
        rw = setup_risingwave_connection()
        query = """
        SELECT fragment_id, fragment_type, node_name, table_id, rate_limit
        FROM rw_catalog.rw_rate_limit
        ORDER BY table_id, fragment_id
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting rate limits: {str(e)}"

    @mcp.tool
    def get_streaming_job_configs(job_name: str = None) -> str:
        """
        Get configuration parameters for streaming jobs.
        Shows settings like retention, checkpoint interval, etc.

        Args:
            job_name: Optional job name to filter. If not specified, shows all jobs.

        Returns:
            Job configuration key-value pairs
        """
        rw = setup_risingwave_connection()
        if job_name:
            safe_name = escape_sql_string(job_name)
            query = f"""
            SELECT id, name, key, value
            FROM rw_catalog.rw_streaming_job_config
            WHERE name = '{safe_name}'
            ORDER BY key
            """
        else:
            query = """
            SELECT id, name, key, value
            FROM rw_catalog.rw_streaming_job_config
            ORDER BY name, key
            """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting streaming job configs: {str(e)}"

    @mcp.tool
    def get_compaction_task_progress() -> str:
        """
        Get progress of active compaction tasks.
        Shows SSTs sealed/uploaded and pending I/O for each compaction group.

        Returns:
            Compaction task progress with I/O statistics
        """
        rw = setup_risingwave_connection()
        query = """
        SELECT compaction_group_id, task_id,
               num_ssts_sealed, num_ssts_uploaded,
               num_progress_key, num_pending_read_io, num_pending_write_io
        FROM rw_catalog.rw_hummock_compact_task_progress
        ORDER BY compaction_group_id, task_id
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting compaction task progress: {str(e)}"

    @mcp.tool
    def get_streaming_jobs_overview() -> str:
        """
        Get an overview of all streaming jobs with their status and parallelism.
        This is the primary tool for checking the health of all streaming jobs.

        Returns:
            All streaming jobs with id, name, status, parallelism, and resource group
        """
        rw = setup_risingwave_connection()
        query = """
        SELECT id, name, status, parallelism, max_parallelism, resource_group
        FROM rw_catalog.rw_streaming_jobs
        ORDER BY name
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting streaming jobs overview: {str(e)}"

    @mcp.tool
    def get_recovery_info() -> str:
        """
        Get information about the last cluster recovery event.
        Useful for diagnosing cluster stability issues.

        Returns:
            Recovery information including reason and timing
        """
        rw = setup_risingwave_connection()
        query = "SELECT * FROM rw_catalog.rw_recovery_info"
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting recovery info: {str(e)}"

    @mcp.tool
    def get_object_dependencies(object_name: str = None) -> str:
        """
        Get object dependency information from the system catalog.
        Shows which objects depend on which other objects.

        Args:
            object_name: Optional name to filter dependencies for a specific object

        Returns:
            Dependency relationships between database objects
        """
        rw = setup_risingwave_connection()
        if object_name:
            safe_name = escape_sql_string(object_name)
            query = f"""
            SELECT d.objid, d.refobjid,
                   r1.name as object_name, r1.relation_type as object_type,
                   r2.name as depends_on, r2.relation_type as depends_on_type
            FROM rw_catalog.rw_depend d
            LEFT JOIN rw_catalog.rw_relations r1 ON d.objid = r1.id
            LEFT JOIN rw_catalog.rw_relations r2 ON d.refobjid = r2.id
            WHERE r1.name = '{safe_name}' OR r2.name = '{safe_name}'
            ORDER BY r1.name, r2.name
            """
        else:
            query = """
            SELECT d.objid, d.refobjid,
                   r1.name as object_name, r1.relation_type as object_type,
                   r2.name as depends_on, r2.relation_type as depends_on_type
            FROM rw_catalog.rw_depend d
            LEFT JOIN rw_catalog.rw_relations r1 ON d.objid = r1.id
            LEFT JOIN rw_catalog.rw_relations r2 ON d.refobjid = r2.id
            ORDER BY r1.name, r2.name
            LIMIT 200
            """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting object dependencies: {str(e)}"
