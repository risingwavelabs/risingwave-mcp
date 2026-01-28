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

    # ==================== State Table and Parallelism Tools ====================

    @mcp.tool
    def get_fragments_by_state_table(state_table_id: int) -> str:
        """
        Find fragments that use a specific state table.
        State tables are internal storage for streaming operators (joins, aggregations).

        Args:
            state_table_id: The state table ID to look up.

        Returns:
            List of fragments using this state table with their actor counts.
        """
        rw = setup_risingwave_connection()
        query = f"""
        SELECT rf.fragment_id, rf.table_id, rf.flags, rf.parallelism,
               count(ra.actor_id) as actor_count
        FROM rw_fragments rf
        JOIN rw_actors ra ON rf.fragment_id = ra.fragment_id
        WHERE {state_table_id} = ANY(rf.state_table_ids)
        GROUP BY rf.fragment_id, rf.table_id, rf.flags, rf.parallelism
        ORDER BY rf.fragment_id
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting fragments by state table: {str(e)}"

    @mcp.tool
    def get_streaming_parallelism() -> str:
        """
        Get an overview of parallelism settings for all streaming jobs.

        Returns:
            Parallelism information for all streaming jobs.
        """
        rw = setup_risingwave_connection()
        query = """
        SELECT * FROM rw_streaming_parallelism
        ORDER BY name
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting streaming parallelism: {str(e)}"

    @mcp.tool
    def get_mv_worker_distribution(mv_name: str) -> str:
        """
        Get the distribution of actors across worker nodes for a materialized view.
        Useful for checking load balance across compute nodes.

        Args:
            mv_name: Name of the materialized view.

        Returns:
            Worker nodes with their actor counts for this MV.
        """
        rw = setup_risingwave_connection()
        query = f"""
        SELECT w.id as worker_id, w.host, count(a.actor_id) as actor_count
        FROM rw_fragments f
        JOIN rw_materialized_views m ON f.table_id = m.id
        JOIN rw_actors a ON f.fragment_id = a.fragment_id
        JOIN rw_worker_nodes w ON a.worker_id = w.id
        WHERE m.name = '{mv_name}'
        GROUP BY w.id, w.host
        ORDER BY actor_count DESC
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting MV worker distribution: {str(e)}"

    @mcp.tool
    def get_job_by_state_table_id(state_table_id: int) -> str:
        """
        Find the streaming job (MV, table, sink, or source) that owns a state table.
        Useful for debugging when you see state table IDs in logs or metrics.

        Args:
            state_table_id: The state table ID to look up.

        Returns:
            The streaming job information including schema, name, and job type.
        """
        rw = setup_risingwave_connection()
        query = f"""
        WITH t AS (
            SELECT table_id
            FROM rw_fragments
            WHERE array_position(state_table_ids, {state_table_id}) IS NOT NULL
        ),
        jobs AS (
            SELECT schema_id, id, name, 'materialized view' AS job_type FROM rw_materialized_views
            UNION ALL
            SELECT schema_id, id, name, 'table' AS job_type FROM rw_tables
            UNION ALL
            SELECT schema_id, id, name, 'sink' AS job_type FROM rw_sinks
            UNION ALL
            SELECT schema_id, id, name, 'source' AS job_type FROM rw_sources
        )
        SELECT s.name AS schema_name, jobs.name AS job_name, jobs.job_type, jobs.id AS job_id
        FROM t
        JOIN jobs ON t.table_id = jobs.id
        JOIN rw_schemas s ON s.id = jobs.schema_id
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting job by state table ID: {str(e)}"

    # ==================== Internal Tables Tools ====================

    @mcp.tool
    def get_internal_tables(job_name: str = None, job_id: int = None, schema_name: str = "public") -> str:
        """
        Get all internal state tables for a streaming job.
        Internal tables store state for operators like joins, aggregations, etc.

        Args:
            job_name: Name of the streaming job (optional if job_id is provided).
            job_id: ID of the streaming job (optional if job_name is provided).
            schema_name: Schema name (default: "public"), used with job_name.

        Returns:
            List of internal tables with their details.
        """
        rw = setup_risingwave_connection()

        if job_id is not None:
            query = f"""
            SELECT * FROM rw_internal_table_info
            WHERE job_id = {job_id}
            ORDER BY id
            """
        elif job_name is not None:
            query = f"""
            SELECT * FROM rw_internal_table_info
            WHERE job_name = '{job_name}' AND schema_name = '{schema_name}'
            ORDER BY id
            """
        else:
            return "Error: Either job_name or job_id must be provided"

        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting internal tables: {str(e)}"

    # ==================== Data Distribution Tools ====================

    @mcp.tool
    def check_vnode_distribution(table_name: str, distribution_key: str) -> str:
        """
        Check vnode distribution for a table to detect data skew.
        WARNING: This performs a full table scan and can be heavy on large tables.

        Args:
            table_name: Name of the table to check.
            distribution_key: The column used as distribution key.

        Returns:
            Vnode distribution showing count per vnode.
        """
        rw = setup_risingwave_connection()
        # rw_vnode requires (vnode_count, distribution_keys...)
        # Default vnode count in RisingWave is 256
        query = f"""
        SELECT rw_vnode(256, {distribution_key}) as vnode, count(*) as row_count
        FROM {table_name}
        GROUP BY vnode
        ORDER BY row_count DESC
        LIMIT 100
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error checking vnode distribution: {str(e)}"

    # ==================== Dependency Graph Tools ====================

    @mcp.tool
    def get_downstream_dependents(object_name: str) -> str:
        """
        Find all materialized views that depend on a given table or MV.
        Useful for impact analysis before modifying or dropping an object.

        Args:
            object_name: Name of the table or materialized view.

        Returns:
            List of dependent materialized views with their definitions.
        """
        rw = setup_risingwave_connection()
        query = f"""
        WITH obj_oid AS (
            SELECT oid FROM pg_class WHERE relname = '{object_name}'
        ),
        dependent_oids AS (
            SELECT d.objid AS mv_oid
            FROM rw_depend d
            JOIN obj_oid o ON d.refobjid = o.oid
        )
        SELECT m.id, s.name AS schema_name, m.name, m.owner, m.definition
        FROM rw_materialized_views m
        JOIN dependent_oids d ON m.id = d.mv_oid
        JOIN rw_schemas s ON m.schema_id = s.id
        ORDER BY m.name
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting downstream dependents: {str(e)}"

    @mcp.tool
    def get_upstream_dependents(mv_name: str) -> str:
        """
        Find all tables and materialized views that a given MV depends on.
        Useful for understanding data lineage.

        Args:
            mv_name: Name of the materialized view.

        Returns:
            List of upstream tables and MVs with their definitions.
        """
        rw = setup_risingwave_connection()
        query = f"""
        WITH mv_oid AS (
            SELECT oid FROM pg_class WHERE relname = '{mv_name}'
        ),
        upstream_oids AS (
            SELECT d.refobjid AS obj_oid
            FROM rw_depend d
            JOIN mv_oid m ON d.objid = m.oid
        )
        SELECT 'materialized view' AS object_type, m.id, s.name AS schema_name,
               m.name, m.definition
        FROM rw_materialized_views m
        JOIN upstream_oids u ON m.id = u.obj_oid
        JOIN rw_schemas s ON m.schema_id = s.id
        UNION ALL
        SELECT 'table' AS object_type, t.id, s.name AS schema_name,
               t.name, t.definition
        FROM rw_tables t
        JOIN upstream_oids u ON t.id = u.obj_oid
        JOIN rw_schemas s ON t.schema_id = s.id
        ORDER BY object_type, name
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting upstream dependents: {str(e)}"

    # ==================== Sink Decouple Tools ====================

    @mcp.tool
    def get_sink_decouple_status() -> str:
        """
        Check whether sinks are decoupled or not.
        Decoupled sinks have separate log stores for better performance isolation.

        Returns:
            All sinks with their decouple status, connector type, and vnode count.
        """
        rw = setup_risingwave_connection()
        query = """
        SELECT d.sink_id,
               d.is_decouple,
               d.watermark_vnode_count,
               s.name,
               s.connector,
               s.sink_type
        FROM rw_catalog.rw_sink_decouple d
        JOIN rw_catalog.rw_sinks s ON d.sink_id = s.id
        ORDER BY s.name
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting sink decouple status: {str(e)}"

    @mcp.tool
    def get_sink_logstore_lag(internal_table_name: str) -> str:
        """
        Monitor the lag of a decoupled sink by querying its internal log store table.
        This is useful for monitoring sink health and backpressure.

        To find the internal table name, use get_internal_tables() for the sink.
        The internal table name typically follows the pattern:
        __internal_<schema>_<sink_id>_sink_<number>

        Args:
            internal_table_name: Name of the sink's internal log store table.

        Returns:
            The lag duration from the oldest unprocessed record.
        """
        rw = setup_risingwave_connection()
        # The kv_log_store_epoch column stores the epoch timestamp
        # Epoch format: upper bits contain timestamp info
        # RisingWave epoch base time: 2021-04-01 00:00:00 UTC (1617235200)
        query = f"""
        SELECT (now() - to_timestamp(((kv_log_store_epoch >> 16) & 70368744177663) / 1000 + 1617235200)) AS lag
        FROM {internal_table_name}
        ORDER BY kv_log_store_epoch ASC
        LIMIT 1
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting sink logstore lag: {str(e)}"
