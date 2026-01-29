from fastmcp import FastMCP
from risingwave import OutputFormat
from connection import setup_risingwave_connection


def register_cluster_tools(mcp: FastMCP):
    """Register all cluster management MCP tools"""

    @mcp.tool
    def show_cluster() -> str:
        """
        Display cluster node details including address, type, state, and resources.

        Returns:
            Cluster information with worker nodes (Id, Addr, Type, State, Parallel Units,
            Is Streaming, Is Serving, Is Unschedulable, Started At)
        """
        rw = setup_risingwave_connection()
        query = "SHOW CLUSTER"
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error showing cluster info: {str(e)}"

    @mcp.tool
    def show_jobs(like_pattern: str = None) -> str:
        """
        List all active streaming jobs (creating materialized views, indexes, tables, sinks, sources).

        Args:
            like_pattern: Optional pattern to filter jobs by name (e.g., 'mv_%')

        Returns:
            List of jobs with Id, Statement, and Progress
        """
        rw = setup_risingwave_connection()
        query = f"SHOW JOBS LIKE '{like_pattern}'" if like_pattern else "SHOW JOBS"
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error listing jobs: {str(e)}"

    @mcp.tool
    def cancel_jobs(job_ids: str) -> str:
        """
        Cancel one or more running streaming jobs.

        Args:
            job_ids: Comma-separated job IDs to cancel (e.g., "1010" or "1010, 1012")

        Returns:
            Success message with cancelled job IDs or error message
        """
        rw = setup_risingwave_connection()
        # Validate job_ids - should be comma-separated integers
        try:
            ids = [int(id.strip()) for id in job_ids.split(',')]
            ids_str = ', '.join(str(id) for id in ids)
        except ValueError:
            return "Error: job_ids must be comma-separated integers"

        query = f"CANCEL JOBS {ids_str}"
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return f"Jobs cancelled successfully. Result: {result.to_json()}"
        except Exception as e:
            return f"Error cancelling jobs: {str(e)}"

    @mcp.tool
    def recover_cluster() -> str:
        """
        Manually trigger an ad-hoc recovery operation.
        Useful when there is high barrier latency and you need to force a recovery,
        or to make DROP/CANCEL commands take effect immediately.

        Note: Requires superuser privileges.

        Returns:
            Success or error message
        """
        rw = setup_risingwave_connection()
        query = "RECOVER"
        try:
            rw.execute(query)
            return "Recovery triggered successfully"
        except Exception as e:
            return f"Error triggering recovery: {str(e)}"

    @mcp.tool
    def get_cluster_info() -> str:
        """
        Get comprehensive cluster information including meta, compute, frontend, and compactor nodes.

        Returns:
            Detailed cluster node information
        """
        rw = setup_risingwave_connection()
        query = """
        SELECT id,
               host as addr,
               port,
               type as node_type,
               state,
               parallelism,
               is_streaming,
               is_serving,
               is_unschedulable,
               rw_version,
               total_memory_bytes,
               total_cpu_cores,
               started_at
        FROM rw_catalog.rw_worker_nodes
        ORDER BY type, id
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            # Fall back to SHOW CLUSTER if the catalog query fails
            try:
                result = rw.fetch("SHOW CLUSTER", format=OutputFormat.DATAFRAME)
                return result.to_json()
            except Exception as e2:
                return f"Error getting cluster info: {str(e2)}"
