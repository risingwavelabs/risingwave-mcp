"""
Unit tests for RisingWave MCP tools.
Run with: pytest tests/test_tools.py -v
"""
import pytest
from unittest.mock import MagicMock, patch
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


class MockDataFrame:
    """Mock pandas DataFrame for testing"""
    def __init__(self, data=None):
        self.data = data or {}

    def to_json(self):
        import json
        return json.dumps(self.data)


@pytest.fixture
def mock_rw():
    """Create a mock RisingWave connection"""
    mock = MagicMock()
    mock.fetch.return_value = MockDataFrame({"test": "data"})
    mock.fetchone.return_value = MockDataFrame({"version": "RisingWave 2.0"})
    mock.execute.return_value = None
    mock.check_exist.return_value = True
    return mock


@pytest.fixture
def mock_connection(mock_rw):
    """Patch the connection setup to return our mock"""
    with patch('connection.setup_risingwave_connection', return_value=mock_rw):
        yield mock_rw


def get_tool_fn(mcp, tool_name):
    """Helper to get a tool function by name from FastMCP"""
    tool = mcp._tool_manager._tools.get(tool_name)
    return tool.fn if tool else None


# ==================== Schema Tools Tests ====================

class TestSchemaTools:
    def test_show_tables(self, mock_connection):
        from schema_tools import register_schema_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_schema_tools(mcp)

        show_tables = get_tool_fn(mcp, "show_tables")
        assert show_tables is not None
        result = show_tables()
        assert "test" in result
        mock_connection.fetch.assert_called_once()

    def test_list_schemas(self, mock_connection):
        from schema_tools import register_schema_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_schema_tools(mcp)

        list_schemas = get_tool_fn(mcp, "list_schemas")
        assert list_schemas is not None
        result = list_schemas()
        assert isinstance(result, str)


# ==================== Source Tools Tests ====================

class TestSourceTools:
    def test_list_sources(self, mock_connection):
        from source_tools import register_source_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_source_tools(mcp)

        list_sources = get_tool_fn(mcp, "list_sources")
        assert list_sources is not None
        result = list_sources()
        assert isinstance(result, str)
        mock_connection.fetch.assert_called()

    def test_alter_source_parallelism_valid(self, mock_connection):
        from source_tools import register_source_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_source_tools(mcp)

        alter_parallelism = get_tool_fn(mcp, "alter_source_parallelism")
        assert alter_parallelism is not None

        result = alter_parallelism("my_source", "4")
        assert "parallelism set to 4" in result

        result = alter_parallelism("my_source", "ADAPTIVE")
        assert "parallelism set to ADAPTIVE" in result

    def test_alter_source_parallelism_invalid(self, mock_connection):
        from source_tools import register_source_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_source_tools(mcp)

        alter_parallelism = get_tool_fn(mcp, "alter_source_parallelism")
        assert alter_parallelism is not None

        result = alter_parallelism("my_source", "invalid")
        assert "Error" in result


# ==================== Cluster Tools Tests ====================

class TestClusterTools:
    def test_show_cluster(self, mock_connection):
        from cluster_tools import register_cluster_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_cluster_tools(mcp)

        show_cluster = get_tool_fn(mcp, "show_cluster")
        assert show_cluster is not None
        result = show_cluster()
        assert isinstance(result, str)

    def test_cancel_jobs_valid(self, mock_connection):
        from cluster_tools import register_cluster_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_cluster_tools(mcp)

        cancel_jobs = get_tool_fn(mcp, "cancel_jobs")
        assert cancel_jobs is not None

        result = cancel_jobs("1010, 1012")
        assert "cancelled" in result.lower() or "error" in result.lower()

    def test_cancel_jobs_invalid(self, mock_connection):
        from cluster_tools import register_cluster_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_cluster_tools(mcp)

        cancel_jobs = get_tool_fn(mcp, "cancel_jobs")
        assert cancel_jobs is not None

        result = cancel_jobs("invalid")
        assert "Error" in result


# ==================== DDL Tools Tests ====================

class TestDDLTools:
    def test_create_schema(self, mock_connection):
        from ddl_tools import register_ddl_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_ddl_tools(mcp)

        create_schema = get_tool_fn(mcp, "create_schema")
        assert create_schema is not None

        result = create_schema("test_schema")
        assert "created successfully" in result

    def test_alter_mv_parallelism_validation(self, mock_connection):
        from ddl_tools import register_ddl_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_ddl_tools(mcp)

        alter_mv = get_tool_fn(mcp, "alter_mv_parallelism")
        assert alter_mv is not None

        # Valid inputs
        result = alter_mv("my_mv", "4")
        assert "parallelism set to 4" in result

        result = alter_mv("my_mv", "ADAPTIVE")
        assert "ADAPTIVE" in result

        # Invalid input
        result = alter_mv("my_mv", "invalid")
        assert "Error" in result


# ==================== DML Tools Tests ====================

class TestDMLTools:
    def test_insert_single_row(self, mock_connection):
        from dml_tools import register_dml_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_dml_tools(mcp)

        insert_row = get_tool_fn(mcp, "insert_single_row")
        assert insert_row is not None

        result = insert_row("test_table", '{"col1": "value1", "col2": 123}')
        assert "inserted successfully" in result or "Error" in result

    def test_insert_single_row_invalid_json(self, mock_connection):
        from dml_tools import register_dml_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_dml_tools(mcp)

        insert_row = get_tool_fn(mcp, "insert_single_row")
        assert insert_row is not None

        result = insert_row("test_table", 'not valid json')
        assert "Error" in result

    def test_update_rows(self, mock_connection):
        from dml_tools import register_dml_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_dml_tools(mcp)

        update_rows = get_tool_fn(mcp, "update_rows")
        assert update_rows is not None

        result = update_rows("test_table", "status = 'active'", "id = 1")
        assert "executed successfully" in result or "Error" in result

    def test_delete_rows(self, mock_connection):
        from dml_tools import register_dml_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_dml_tools(mcp)

        delete_rows = get_tool_fn(mcp, "delete_rows")
        assert delete_rows is not None

        result = delete_rows("test_table", "id = 1")
        assert "executed successfully" in result or "Error" in result


# ==================== Session Tools Tests ====================

class TestSessionTools:
    def test_set_session_variable(self, mock_connection):
        from session_tools import register_session_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_session_tools(mcp)

        set_var = get_tool_fn(mcp, "set_session_variable")
        assert set_var is not None

        result = set_var("query_mode", "local")
        assert "Successfully set" in result or "Error" in result


# ==================== Index Tools Tests ====================

class TestIndexTools:
    def test_list_indexes(self, mock_connection):
        from index_tools import register_index_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_index_tools(mcp)

        list_indexes = get_tool_fn(mcp, "list_indexes")
        assert list_indexes is not None

        result = list_indexes("test_table")
        assert isinstance(result, str)


# ==================== Iceberg Tools Tests ====================

class TestIcebergTools:
    def test_vacuum_table(self, mock_connection):
        from iceberg_tools import register_iceberg_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_iceberg_tools(mcp)

        vacuum = get_tool_fn(mcp, "vacuum_table")
        assert vacuum is not None

        result = vacuum("iceberg_table")
        assert "completed successfully" in result or "Error" in result

    def test_get_iceberg_snapshots(self, mock_connection):
        from iceberg_tools import register_iceberg_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_iceberg_tools(mcp)

        get_snapshots = get_tool_fn(mcp, "get_iceberg_snapshots")
        assert get_snapshots is not None

        result = get_snapshots("my_iceberg_source")
        assert isinstance(result, str)

    def test_get_iceberg_snapshots_invalid_name(self, mock_connection):
        from iceberg_tools import register_iceberg_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_iceberg_tools(mcp)

        get_snapshots = get_tool_fn(mcp, "get_iceberg_snapshots")
        result = get_snapshots("invalid; DROP TABLE")
        assert "Invalid source name" in result

    def test_query_iceberg_time_travel_timestamp(self, mock_connection):
        from iceberg_tools import register_iceberg_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_iceberg_tools(mcp)

        time_travel = get_tool_fn(mcp, "query_iceberg_time_travel")
        assert time_travel is not None

        result = time_travel(
            "my_source",
            "SELECT * FROM my_source",
            timestamp="2024-01-01 12:00:00"
        )
        assert isinstance(result, str)

    def test_query_iceberg_time_travel_both_params(self, mock_connection):
        from iceberg_tools import register_iceberg_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_iceberg_tools(mcp)

        time_travel = get_tool_fn(mcp, "query_iceberg_time_travel")
        result = time_travel(
            "my_source",
            "SELECT * FROM my_source",
            timestamp="2024-01-01",
            snapshot_id=123
        )
        assert "Specify either timestamp or snapshot_id" in result

    def test_query_iceberg_time_travel_no_params(self, mock_connection):
        from iceberg_tools import register_iceberg_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_iceberg_tools(mcp)

        time_travel = get_tool_fn(mcp, "query_iceberg_time_travel")
        result = time_travel("my_source", "SELECT * FROM my_source")
        assert "Must specify either timestamp or snapshot_id" in result

    def test_create_iceberg_sink_upsert_no_key(self, mock_connection):
        from iceberg_tools import register_iceberg_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_iceberg_tools(mcp)

        create_sink = get_tool_fn(mcp, "create_iceberg_sink")
        assert create_sink is not None

        result = create_sink(
            sink_name="test_sink",
            source_object="my_mv",
            warehouse_path="s3://bucket/warehouse",
            database_name="db",
            table_name="table",
            sink_type="upsert"
        )
        assert "primary_key is required" in result

    def test_create_iceberg_source(self, mock_connection):
        from iceberg_tools import register_iceberg_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_iceberg_tools(mcp)

        create_source = get_tool_fn(mcp, "create_iceberg_source")
        assert create_source is not None

        result = create_source(
            source_name="my_iceberg",
            warehouse_path="s3://bucket/warehouse",
            database_name="db",
            table_name="table"
        )
        assert "created successfully" in result or "Error" in result
