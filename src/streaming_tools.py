from fastmcp import FastMCP
from risingwave import OutputFormat
from connection import setup_risingwave_connection


def register_streaming_tools(mcp: FastMCP):
    """Register all streaming infrastructure inspection tools"""

    # ==================== Cluster Overview Tools ====================

    @mcp.tool
    def get_worker_nodes() -> str:
        """
        Get all worker nodes in the RisingWave cluster with their status and resources.

        Returns:
            List of worker nodes with id, host, port, type, state, parallelism,
            streaming/serving status, RisingWave version, memory, CPU cores, and start time.
        """
        rw = setup_risingwave_connection()
        query = """
        SELECT id, host, port, type, state, parallelism,
               is_streaming, is_serving, is_unschedulable,
               rw_version, system_total_memory_bytes, system_total_cpu_cores,
               started_at, resource_group
        FROM rw_worker_nodes
        ORDER BY type, id
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting worker nodes: {str(e)}"

    @mcp.tool
    def get_actor_distribution() -> str:
        """
        Show actor count distribution across worker nodes.
        Useful for identifying load imbalance across compute nodes.

        Returns:
            Worker nodes with their actor counts, sorted by count descending.
        """
        rw = setup_risingwave_connection()
        query = """
        SELECT w.id as worker_id, w.host, w.type, w.state,
               w.parallelism as worker_parallelism,
               count(a.actor_id) as actor_count
        FROM rw_worker_nodes w
        LEFT JOIN rw_actors a ON w.id = a.worker_id
        WHERE w.type = 'WORKER_TYPE_COMPUTE_NODE'
        GROUP BY w.id, w.host, w.type, w.state, w.parallelism
        ORDER BY actor_count DESC
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting actor distribution: {str(e)}"

    @mcp.tool
    def get_fragment_stats() -> str:
        """
        Get statistics about fragments in the cluster including total count,
        parallelism distribution, and communication cost.

        Returns:
            Fragment statistics including counts, parallelism info, and shuffle overhead.
        """
        rw = setup_risingwave_connection()
        query = """
        SELECT
            count(*) as total_fragments,
            sum(parallelism) as total_parallelism,
            avg(parallelism) as avg_parallelism,
            max(parallelism) as max_parallelism,
            min(parallelism) as min_parallelism
        FROM rw_fragments
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting fragment stats: {str(e)}"

    @mcp.tool
    def get_fragment_communication_cost() -> str:
        """
        Calculate total cross-fragment data shuffle overhead in the cluster.
        This metric indicates the total number of actor-to-actor data exchange
        channels across all fragment boundaries.

        Returns:
            Total shuffle channels count - useful for capacity planning.
        """
        rw = setup_risingwave_connection()
        query = """
        SELECT coalesce(sum(f1.parallelism * f2.parallelism), 0) as total_shuffle_channels
        FROM rw_fragments f1, rw_fragments f2
        WHERE f2.fragment_id = ANY (f1.upstream_fragment_ids)
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error calculating fragment communication cost: {str(e)}"

    # ==================== Fragment Tools ====================

    @mcp.tool
    def list_fragments(table_id: int = None) -> str:
        """
        List all fragments in the cluster, optionally filtered by table ID.

        Args:
            table_id: Optional table ID to filter fragments for a specific streaming job.

        Returns:
            List of fragments with their parallelism, flags, and upstream dependencies.
        """
        rw = setup_risingwave_connection()
        if table_id is not None:
            query = f"""
            SELECT fragment_id, table_id, distribution_type,
                   upstream_fragment_ids, flags, parallelism,
                   max_parallelism, parallelism_policy
            FROM rw_fragments
            WHERE table_id = {table_id}
            ORDER BY fragment_id
            """
        else:
            query = """
            SELECT fragment_id, table_id, distribution_type,
                   upstream_fragment_ids, flags, parallelism,
                   max_parallelism, parallelism_policy
            FROM rw_fragments
            ORDER BY table_id, fragment_id
            """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error listing fragments: {str(e)}"

    @mcp.tool
    def get_fragment_details(fragment_id: int) -> str:
        """
        Get detailed information about a specific fragment including its execution plan.

        Args:
            fragment_id: The fragment ID to inspect.

        Returns:
            Fragment details including parallelism, flags, upstream dependencies, and node plan.
        """
        rw = setup_risingwave_connection()
        query = f"""
        SELECT fragment_id, table_id, distribution_type, state_table_ids,
               upstream_fragment_ids, flags, parallelism,
               max_parallelism, parallelism_policy, node
        FROM rw_fragments
        WHERE fragment_id = {fragment_id}
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting fragment details: {str(e)}"

    # ==================== Actor Tools ====================

    @mcp.tool
    def list_actors(fragment_id: int = None, worker_id: int = None) -> str:
        """
        List all actors in the cluster, optionally filtered by fragment or worker.

        Args:
            fragment_id: Optional fragment ID to filter actors.
            worker_id: Optional worker ID to filter actors on a specific node.

        Returns:
            List of actors with their fragment assignment and state.
        """
        rw = setup_risingwave_connection()
        conditions = []
        if fragment_id is not None:
            conditions.append(f"fragment_id = {fragment_id}")
        if worker_id is not None:
            conditions.append(f"worker_id = {worker_id}")

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        query = f"""
        SELECT actor_id, fragment_id, worker_id, state
        FROM rw_actors
        {where_clause}
        ORDER BY fragment_id, actor_id
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error listing actors: {str(e)}"

    @mcp.tool
    def get_actors_for_fragment(fragment_id: int) -> str:
        """
        Get all actors running a specific fragment with their worker node info.

        Args:
            fragment_id: The fragment ID to get actors for.

        Returns:
            List of actors with their worker node details.
        """
        rw = setup_risingwave_connection()
        query = f"""
        SELECT a.actor_id, a.fragment_id, a.state as actor_state,
               w.id as worker_id, w.host, w.port, w.state as worker_state
        FROM rw_actors a
        JOIN rw_worker_nodes w ON a.worker_id = w.id
        WHERE a.fragment_id = {fragment_id}
        ORDER BY a.actor_id
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting actors for fragment: {str(e)}"

    # ==================== Lookup/Tracing Tools ====================

    @mcp.tool
    def get_object_id_by_name(name: str, object_type: str = "mv") -> str:
        """
        Get the internal ID of a database object by its name.

        Args:
            name: Name of the object.
            object_type: Type of object - 'mv' for materialized view,
                        'table' for table, 'sink' for sink, 'source' for source.

        Returns:
            The internal ID of the object.
        """
        rw = setup_risingwave_connection()

        type_to_table = {
            "mv": "rw_materialized_views",
            "table": "rw_tables",
            "sink": "rw_sinks",
            "source": "rw_sources"
        }

        if object_type not in type_to_table:
            return f"Error: Invalid object_type '{object_type}'. Must be one of: mv, table, sink, source"

        catalog_table = type_to_table[object_type]
        query = f"SELECT id, name, schema_id FROM {catalog_table} WHERE name = '{name}'"

        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting object ID: {str(e)}"

    @mcp.tool
    def get_mv_by_fragment_id(fragment_id: int) -> str:
        """
        Find the materialized view that owns a specific fragment.
        Useful for debugging when you have a fragment ID from logs or metrics.

        Args:
            fragment_id: The fragment ID to look up.

        Returns:
            The materialized view name, schema, and fragment details.
        """
        rw = setup_risingwave_connection()
        query = f"""
        SELECT m.name as mv_name, s.name as schema_name, m.id as mv_id,
               f.fragment_id, f.parallelism, f.flags, f.distribution_type
        FROM rw_materialized_views m
        JOIN rw_fragments f ON m.id = f.table_id
        JOIN rw_schemas s ON m.schema_id = s.id
        WHERE f.fragment_id = {fragment_id}
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            if result.empty:
                # Try tables if not found in MVs
                query_table = f"""
                SELECT t.name as table_name, s.name as schema_name, t.id as table_id,
                       f.fragment_id, f.parallelism, f.flags, f.distribution_type
                FROM rw_tables t
                JOIN rw_fragments f ON t.id = f.table_id
                JOIN rw_schemas s ON t.schema_id = s.id
                WHERE f.fragment_id = {fragment_id}
                """
                result = rw.fetch(query_table, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting MV by fragment ID: {str(e)}"

    @mcp.tool
    def get_mv_by_actor_id(actor_id: int) -> str:
        """
        Trace an actor back to its owning materialized view or table.
        Useful for debugging when you see an actor ID in error logs or metrics.

        Args:
            actor_id: The actor ID to trace.

        Returns:
            The actor info, fragment info, and owning MV/table details.
        """
        rw = setup_risingwave_connection()
        query = f"""
        SELECT a.actor_id, a.state as actor_state, a.worker_id,
               f.fragment_id, f.parallelism, f.flags,
               m.id as mv_id, m.name as mv_name, s.name as schema_name
        FROM rw_actors a
        JOIN rw_fragments f ON a.fragment_id = f.fragment_id
        JOIN rw_materialized_views m ON f.table_id = m.id
        JOIN rw_schemas s ON m.schema_id = s.id
        WHERE a.actor_id = {actor_id}
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            if result.empty:
                # Try tables if not found in MVs
                query_table = f"""
                SELECT a.actor_id, a.state as actor_state, a.worker_id,
                       f.fragment_id, f.parallelism, f.flags,
                       t.id as table_id, t.name as table_name, s.name as schema_name
                FROM rw_actors a
                JOIN rw_fragments f ON a.fragment_id = f.fragment_id
                JOIN rw_tables t ON f.table_id = t.id
                JOIN rw_schemas s ON t.schema_id = s.id
                WHERE a.actor_id = {actor_id}
                """
                result = rw.fetch(query_table, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting MV by actor ID: {str(e)}"

    @mcp.tool
    def get_worker_for_actor(actor_id: int) -> str:
        """
        Find which worker node is running a specific actor.

        Args:
            actor_id: The actor ID to look up.

        Returns:
            Worker node details for the actor.
        """
        rw = setup_risingwave_connection()
        query = f"""
        SELECT a.actor_id, a.fragment_id, a.state as actor_state,
               w.id as worker_id, w.host, w.port, w.type, w.state as worker_state,
               w.parallelism as worker_parallelism, w.rw_version
        FROM rw_actors a
        JOIN rw_worker_nodes w ON a.worker_id = w.id
        WHERE a.actor_id = {actor_id}
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting worker for actor: {str(e)}"

    @mcp.tool
    def get_workers_for_fragment(fragment_id: int) -> str:
        """
        Find all worker nodes running actors for a specific fragment.

        Args:
            fragment_id: The fragment ID to look up.

        Returns:
            List of worker nodes and their actors for this fragment.
        """
        rw = setup_risingwave_connection()
        query = f"""
        SELECT w.id as worker_id, w.host, w.port, w.type, w.state as worker_state,
               count(a.actor_id) as actor_count,
               array_agg(a.actor_id ORDER BY a.actor_id) as actor_ids
        FROM rw_actors a
        JOIN rw_worker_nodes w ON a.worker_id = w.id
        WHERE a.fragment_id = {fragment_id}
        GROUP BY w.id, w.host, w.port, w.type, w.state
        ORDER BY w.id
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting workers for fragment: {str(e)}"

    # ==================== Streaming Job Inspection Tools ====================

    @mcp.tool
    def get_streaming_job_fragments(job_name: str, job_type: str = "mv") -> str:
        """
        Get all fragments for a streaming job (MV, table, or sink).

        Args:
            job_name: Name of the streaming job.
            job_type: Type of job - 'mv', 'table', or 'sink'.

        Returns:
            List of fragments with their details and upstream dependencies.
        """
        rw = setup_risingwave_connection()

        type_to_table = {
            "mv": "rw_materialized_views",
            "table": "rw_tables",
            "sink": "rw_sinks"
        }

        if job_type not in type_to_table:
            return f"Error: Invalid job_type '{job_type}'. Must be one of: mv, table, sink"

        catalog_table = type_to_table[job_type]

        query = f"""
        SELECT j.name as job_name, f.fragment_id, f.distribution_type,
               f.upstream_fragment_ids, f.flags, f.parallelism,
               f.max_parallelism, f.parallelism_policy
        FROM {catalog_table} j
        JOIN rw_fragments f ON j.id = f.table_id
        WHERE j.name = '{job_name}'
        ORDER BY f.fragment_id
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting streaming job fragments: {str(e)}"

    @mcp.tool
    def get_streaming_job_actors(job_name: str, job_type: str = "mv") -> str:
        """
        Get all actors for a streaming job with their worker node assignments.

        Args:
            job_name: Name of the streaming job.
            job_type: Type of job - 'mv', 'table', or 'sink'.

        Returns:
            List of actors grouped by fragment with worker node info.
        """
        rw = setup_risingwave_connection()

        type_to_table = {
            "mv": "rw_materialized_views",
            "table": "rw_tables",
            "sink": "rw_sinks"
        }

        if job_type not in type_to_table:
            return f"Error: Invalid job_type '{job_type}'. Must be one of: mv, table, sink"

        catalog_table = type_to_table[job_type]

        query = f"""
        SELECT j.name as job_name, f.fragment_id, f.flags,
               a.actor_id, a.state as actor_state,
               w.id as worker_id, w.host, w.state as worker_state
        FROM {catalog_table} j
        JOIN rw_fragments f ON j.id = f.table_id
        JOIN rw_actors a ON f.fragment_id = a.fragment_id
        JOIN rw_worker_nodes w ON a.worker_id = w.id
        WHERE j.name = '{job_name}'
        ORDER BY f.fragment_id, a.actor_id
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting streaming job actors: {str(e)}"

    @mcp.tool
    def get_backfill_progress() -> str:
        """
        Get the backfill progress for all streaming jobs currently being created.

        Returns:
            List of streaming jobs with their backfill progress percentage.
        """
        rw = setup_risingwave_connection()
        query = """
        SELECT * FROM rw_ddl_progress
        ORDER BY progress DESC
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting backfill progress: {str(e)}"

    @mcp.tool
    def get_fragment_backfill_progress() -> str:
        """
        Get detailed fragment-level backfill progress.

        Returns:
            Fragment-level backfill progress information.
        """
        rw = setup_risingwave_connection()
        query = """
        SELECT * FROM rw_fragment_backfill_progress
        ORDER BY fragment_id
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting fragment backfill progress: {str(e)}"

    # ==================== Dependency Tools ====================

    @mcp.tool
    def get_upstream_fragments(fragment_id: int) -> str:
        """
        Get the upstream fragments that feed data into a specific fragment.

        Args:
            fragment_id: The fragment ID to get upstream fragments for.

        Returns:
            List of upstream fragments with their details.
        """
        rw = setup_risingwave_connection()
        query = f"""
        WITH target AS (
            SELECT upstream_fragment_ids
            FROM rw_fragments
            WHERE fragment_id = {fragment_id}
        )
        SELECT f.fragment_id, f.table_id, f.distribution_type,
               f.flags, f.parallelism, f.parallelism_policy
        FROM rw_fragments f, target t
        WHERE f.fragment_id = ANY(t.upstream_fragment_ids)
        ORDER BY f.fragment_id
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting upstream fragments: {str(e)}"

    @mcp.tool
    def get_downstream_fragments(fragment_id: int) -> str:
        """
        Get the downstream fragments that consume data from a specific fragment.

        Args:
            fragment_id: The fragment ID to get downstream fragments for.

        Returns:
            List of downstream fragments with their details.
        """
        rw = setup_risingwave_connection()
        query = f"""
        SELECT fragment_id, table_id, distribution_type,
               upstream_fragment_ids, flags, parallelism, parallelism_policy
        FROM rw_fragments
        WHERE {fragment_id} = ANY(upstream_fragment_ids)
        ORDER BY fragment_id
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting downstream fragments: {str(e)}"
