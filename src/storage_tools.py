from fastmcp import FastMCP
from risingwave import OutputFormat
from connection import setup_risingwave_connection


def register_storage_tools(mcp: FastMCP):
    """Register all storage inspection tools for Hummock storage engine"""

    # ==================== Table Storage Stats ====================

    @mcp.tool
    def get_table_storage_stats(table_id: int) -> str:
        """
        Get the physical storage size and key count of a specific table.

        Args:
            table_id: The internal table ID to inspect.

        Returns:
            Storage statistics including total key count, key size, and value size.
        """
        rw = setup_risingwave_connection()
        query = f"""
        SELECT id,
               total_key_count,
               total_key_size,
               total_value_size,
               (total_key_size + total_value_size) AS total_size_bytes,
               round((total_key_size + total_value_size) / 1024.0 / 1024.0, 2) AS total_size_mb
        FROM rw_catalog.rw_table_stats
        WHERE id = {table_id}
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting table storage stats: {str(e)}"

    @mcp.tool
    def get_all_table_storage_stats() -> str:
        """
        Get physical storage statistics for all tables.

        Returns:
            Storage statistics for all tables, sorted by size descending.
        """
        rw = setup_risingwave_connection()
        query = """
        SELECT id,
               total_key_count,
               total_key_size,
               total_value_size,
               (total_key_size + total_value_size) AS total_size_bytes,
               round((total_key_size + total_value_size) / 1024.0 / 1024.0, 2) AS total_size_mb
        FROM rw_catalog.rw_table_stats
        ORDER BY total_size_bytes DESC
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting all table storage stats: {str(e)}"

    # ==================== SST Level Layout ====================

    @mcp.tool
    def get_sst_level_layout(compaction_group_id: int = None) -> str:
        """
        Check the SST (Sorted String Table) layout per compaction level.
        Shows file counts and sizes at each level of the LSM tree.

        Args:
            compaction_group_id: Optional compaction group ID to filter.
                                If not provided, shows stats for all groups.

        Returns:
            SST distribution across levels with file counts and sizes in KB.
        """
        rw = setup_risingwave_connection()
        where_clause = f"WHERE compaction_group_id = {compaction_group_id}" if compaction_group_id else ""
        query = f"""
        SELECT compaction_group_id,
               level_id,
               level_type,
               sub_level_id,
               count(*) AS total_file_count,
               round(sum(file_size) / 1024.0, 2) AS total_file_size_kb,
               round(sum(file_size) / 1024.0 / 1024.0, 2) AS total_file_size_mb
        FROM rw_catalog.rw_hummock_sstables
        {where_clause}
        GROUP BY compaction_group_id, level_type, level_id, sub_level_id
        ORDER BY compaction_group_id, level_id, sub_level_id DESC, level_type DESC
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting SST level layout: {str(e)}"

    # ==================== Compaction Group Configs ====================

    @mcp.tool
    def get_compaction_group_configs() -> str:
        """
        Get compaction group configurations including write stop limits.
        Useful for understanding compaction behavior and thresholds.

        Returns:
            Compaction group configs with member tables and compaction settings.
        """
        rw = setup_risingwave_connection()
        query = """
        SELECT id,
               parent_id,
               member_tables,
               compaction_config,
               active_write_limit
        FROM rw_catalog.rw_hummock_compaction_group_configs
        ORDER BY id
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting compaction group configs: {str(e)}"

    # ==================== SST Delete Ratio ====================

    @mcp.tool
    def get_sst_delete_ratio(table_id: int) -> str:
        """
        Check delete ratio of SSTs containing a specific table ID.
        High delete ratios can cause slow table scans, affecting backfill,
        batch scans, and range scans.

        Args:
            table_id: The table ID to check delete ratios for.

        Returns:
            SSTs with their delete ratios, sorted by ratio descending.
        """
        rw = setup_risingwave_connection()
        # table_ids is JSONB, need to check if table_id is in the array
        query = f"""
        WITH sst_delete_ratio AS (
            SELECT sstable_id,
                   compaction_group_id,
                   level_id,
                   sub_level_id,
                   total_key_count,
                   stale_key_count,
                   range_tombstone_count,
                   CASE WHEN total_key_count > 0
                        THEN round(range_tombstone_count * 1.0 / total_key_count, 4)
                        ELSE 0 END AS range_delete_ratio,
                   CASE WHEN total_key_count > 0
                        THEN round(stale_key_count * 1.0 / total_key_count, 4)
                        ELSE 0 END AS stale_key_ratio,
                   jsonb_array_elements_text(table_ids)::int AS tid
            FROM rw_catalog.rw_hummock_sstables
        )
        SELECT sstable_id,
               compaction_group_id,
               level_id,
               sub_level_id,
               total_key_count,
               stale_key_count,
               range_tombstone_count,
               range_delete_ratio,
               stale_key_ratio
        FROM sst_delete_ratio
        WHERE tid = {table_id}
        ORDER BY stale_key_ratio DESC, range_delete_ratio DESC
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting SST delete ratio: {str(e)}"

    # ==================== SST Vnode Distribution ====================

    @mcp.tool
    def get_sst_vnode_distribution(table_id: int) -> str:
        """
        Check vnode distribution in SSTs for a specific table ID.
        This helps identify data skew at the storage level.
        Note: This works best for large tables or tables in separate compaction groups.

        Args:
            table_id: The table ID to check vnode distribution for.

        Returns:
            SSTs with their vnode ranges (as hex) and sizes.
        """
        rw = setup_risingwave_connection()
        # key_range_left and key_range_right are bytea
        # The vnode is encoded in bytes 5-6 (after table_id prefix)
        query = f"""
        WITH tmp AS (
            SELECT sstable_id,
                   key_range_left AS l,
                   key_range_right AS r,
                   substr(encode(key_range_left, 'hex'), 1, 8) AS l_tid_part,
                   substr(encode(key_range_right, 'hex'), 1, 8) AS r_tid_part,
                   jsonb_array_elements_text(table_ids)::int AS tid,
                   round(file_size / 1024.0 / 1024.0, 2) AS size_mb,
                   level_id
            FROM rw_catalog.rw_hummock_sstables
        )
        SELECT tid,
               level_id,
               sstable_id,
               substr(encode(l, 'hex'), 9, 4) AS start_vnode_hex,
               substr(encode(r, 'hex'), 9, 4) AS end_vnode_hex,
               size_mb
        FROM tmp
        WHERE tid = {table_id}
              AND l_tid_part = r_tid_part
        ORDER BY tid, start_vnode_hex, end_vnode_hex
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting SST vnode distribution: {str(e)}"

    # ==================== Large State Tables in Compaction Group ====================

    @mcp.tool
    def get_large_state_tables_in_compaction_group(
        compaction_group_id: int, min_size_gb: float = 0
    ) -> str:
        """
        Find state tables with large volume in a specific compaction group.
        Useful for capacity planning and identifying storage hotspots.

        Args:
            compaction_group_id: The compaction group ID to inspect.
            min_size_gb: Minimum size in GB to filter results (default: 0).

        Returns:
            State tables in the compaction group with their sizes.
        """
        rw = setup_risingwave_connection()
        query = f"""
        WITH t AS (
            SELECT id,
                   total_key_count,
                   (total_key_size + total_value_size) / 1024.0 / 1024.0 / 1024.0 AS size_gb
            FROM rw_catalog.rw_table_stats
        ),
        cg_members AS (
            SELECT jsonb_array_elements(member_tables)::int AS tid
            FROM rw_catalog.rw_hummock_compaction_group_configs
            WHERE id = {compaction_group_id}
        )
        SELECT t.id AS table_id,
               t.total_key_count,
               round(t.size_gb::numeric, 4) AS size_gb
        FROM t
        JOIN cg_members ON t.id = cg_members.tid
        WHERE t.size_gb >= {min_size_gb}
        ORDER BY t.size_gb DESC
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting large state tables: {str(e)}"

    # ==================== L0 Level Stats ====================

    @mcp.tool
    def get_l0_stats(compaction_group_id: int) -> str:
        """
        Get L0 level statistics for a compaction group.
        L0 is the first level of the LSM tree where new data lands.
        High L0 file counts indicate write pressure or compaction lag.

        Args:
            compaction_group_id: The compaction group ID to inspect.

        Returns:
            L0 sub-level statistics with file counts and sizes.
        """
        rw = setup_risingwave_connection()
        query = f"""
        SELECT sub_level_id,
               level_type,
               sum(file_size) / 1024.0 / 1024.0 / 1024.0 AS size_gb,
               count(*) AS file_count
        FROM rw_catalog.rw_hummock_sstables
        WHERE compaction_group_id = {compaction_group_id}
              AND level_id = 0
        GROUP BY sub_level_id, level_type
        ORDER BY sub_level_id
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting L0 stats: {str(e)}"

    # ==================== Jobs in Compaction Group ====================

    @mcp.tool
    def get_jobs_in_compaction_group(compaction_group_id: int) -> str:
        """
        Find all streaming jobs that have a state table in a specific compaction group.
        Useful for impact analysis when investigating compaction issues.

        Args:
            compaction_group_id: The compaction group ID to inspect.

        Returns:
            Streaming jobs (sources, tables, MVs, sinks) with state in this group.
        """
        rw = setup_risingwave_connection()
        query = f"""
        WITH m AS (
            SELECT jsonb_array_elements(member_tables)::int AS tid
            FROM rw_catalog.rw_hummock_compaction_group_configs
            WHERE id = {compaction_group_id}
        ),
        f AS (
            SELECT table_id, unnest(state_table_ids) AS stid
            FROM rw_catalog.rw_fragments
        ),
        t AS (
            SELECT DISTINCT f.table_id AS tid
            FROM m
            JOIN f ON m.tid = f.stid
        ),
        n AS (
            SELECT id, name, 'SOURCE' AS job_type FROM rw_catalog.rw_sources
            UNION ALL
            SELECT id, name, 'TABLE' AS job_type FROM rw_catalog.rw_tables
            UNION ALL
            SELECT id, name, 'MATERIALIZED VIEW' AS job_type FROM rw_catalog.rw_materialized_views
            UNION ALL
            SELECT id, name, 'SINK' AS job_type FROM rw_catalog.rw_sinks
        )
        SELECT n.id, n.name, n.job_type
        FROM t
        JOIN n ON t.tid = n.id
        ORDER BY n.job_type, n.name
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting jobs in compaction group: {str(e)}"

    # ==================== Meta Snapshot Info ====================

    @mcp.tool
    def get_meta_snapshot_info() -> str:
        """
        Get metadata snapshot information including creation times.
        Meta snapshots are used for recovery and point-in-time restore.

        Returns:
            Meta snapshot IDs, RisingWave versions, and approximate creation times.
        """
        rw = setup_risingwave_connection()
        # state_table_info is JSONB with structure like {"table_id": {"committedEpoch": ...}}
        # committedEpoch is a bigint where upper bits encode timestamp
        query = """
        WITH t AS (
            SELECT meta_snapshot_id,
                   hummock_version_id,
                   rw_version,
                   remarks,
                   (jsonb_each(state_table_info)).value->>'committedEpoch' AS mce
            FROM rw_catalog.rw_meta_snapshot
        )
        SELECT meta_snapshot_id,
               hummock_version_id,
               rw_version,
               remarks,
               to_timestamp((max(mce::bigint) >> 16) / 1000 + 1617235200) AS created_at
        FROM t
        GROUP BY meta_snapshot_id, hummock_version_id, rw_version, remarks
        ORDER BY meta_snapshot_id DESC
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting meta snapshot info: {str(e)}"

    # ==================== Compaction Group Summary ====================

    @mcp.tool
    def get_compaction_group_summary() -> str:
        """
        Get a summary of all compaction groups with their storage usage.

        Returns:
            Compaction groups with total SST counts, sizes, and member table counts.
        """
        rw = setup_risingwave_connection()
        query = """
        WITH sst_stats AS (
            SELECT compaction_group_id,
                   count(*) AS sst_count,
                   sum(file_size) / 1024.0 / 1024.0 / 1024.0 AS total_size_gb,
                   sum(total_key_count) AS total_keys
            FROM rw_catalog.rw_hummock_sstables
            GROUP BY compaction_group_id
        ),
        cg_info AS (
            SELECT id,
                   parent_id,
                   jsonb_array_length(member_tables) AS member_table_count,
                   compaction_config->>'level0StopWriteThresholdSubLevelNumber' AS l0_stop_threshold
            FROM rw_catalog.rw_hummock_compaction_group_configs
        )
        SELECT cg.id AS compaction_group_id,
               cg.parent_id,
               cg.member_table_count,
               cg.l0_stop_threshold,
               coalesce(s.sst_count, 0) AS sst_count,
               coalesce(round(s.total_size_gb::numeric, 4), 0) AS total_size_gb,
               coalesce(s.total_keys, 0) AS total_keys
        FROM cg_info cg
        LEFT JOIN sst_stats s ON cg.id = s.compaction_group_id
        ORDER BY cg.id
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting compaction group summary: {str(e)}"
