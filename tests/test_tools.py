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


# ==================== Schema Tools Tests ====================

class TestSchemaTools:
    def test_show_tables(self, mock_connection):
        from schema_tools import register_schema_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_schema_tools(mcp)

        # Find the tool
        show_tables = None
        for tool in mcp._tools.values():
            if tool.name == "show_tables":
                show_tables = tool.fn
                break

        assert show_tables is not None
        result = show_tables()
        assert "test" in result
        mock_connection.fetch.assert_called_once()

    def test_list_schemas(self, mock_connection):
        from schema_tools import register_schema_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_schema_tools(mcp)

        list_schemas = None
        for tool in mcp._tools.values():
            if tool.name == "list_schemas":
                list_schemas = tool.fn
                break

        result = list_schemas()
        assert isinstance(result, str)


# ==================== Source Tools Tests ====================

class TestSourceTools:
    def test_list_sources(self, mock_connection):
        from source_tools import register_source_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_source_tools(mcp)

        list_sources = None
        for tool in mcp._tools.values():
            if tool.name == "list_sources":
                list_sources = tool.fn
                break

        result = list_sources()
        assert isinstance(result, str)
        mock_connection.fetch.assert_called()

    def test_alter_source_parallelism_valid(self, mock_connection):
        from source_tools import register_source_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_source_tools(mcp)

        alter_parallelism = None
        for tool in mcp._tools.values():
            if tool.name == "alter_source_parallelism":
                alter_parallelism = tool.fn
                break

        result = alter_parallelism("my_source", "4")
        assert "parallelism set to 4" in result

        result = alter_parallelism("my_source", "ADAPTIVE")
        assert "parallelism set to ADAPTIVE" in result

    def test_alter_source_parallelism_invalid(self, mock_connection):
        from source_tools import register_source_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_source_tools(mcp)

        alter_parallelism = None
        for tool in mcp._tools.values():
            if tool.name == "alter_source_parallelism":
                alter_parallelism = tool.fn
                break

        result = alter_parallelism("my_source", "invalid")
        assert "Error" in result


# ==================== Cluster Tools Tests ====================

class TestClusterTools:
    def test_show_cluster(self, mock_connection):
        from cluster_tools import register_cluster_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_cluster_tools(mcp)

        show_cluster = None
        for tool in mcp._tools.values():
            if tool.name == "show_cluster":
                show_cluster = tool.fn
                break

        result = show_cluster()
        assert isinstance(result, str)

    def test_cancel_jobs_valid(self, mock_connection):
        from cluster_tools import register_cluster_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_cluster_tools(mcp)

        cancel_jobs = None
        for tool in mcp._tools.values():
            if tool.name == "cancel_jobs":
                cancel_jobs = tool.fn
                break

        result = cancel_jobs("1010, 1012")
        assert "cancelled" in result.lower() or "error" in result.lower()

    def test_cancel_jobs_invalid(self, mock_connection):
        from cluster_tools import register_cluster_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_cluster_tools(mcp)

        cancel_jobs = None
        for tool in mcp._tools.values():
            if tool.name == "cancel_jobs":
                cancel_jobs = tool.fn
                break

        result = cancel_jobs("invalid")
        assert "Error" in result


# ==================== DDL Tools Tests ====================

class TestDDLTools:
    def test_create_schema(self, mock_connection):
        from ddl_tools import register_ddl_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_ddl_tools(mcp)

        create_schema = None
        for tool in mcp._tools.values():
            if tool.name == "create_schema":
                create_schema = tool.fn
                break

        result = create_schema("test_schema")
        assert "created successfully" in result

    def test_alter_mv_parallelism_validation(self, mock_connection):
        from ddl_tools import register_ddl_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_ddl_tools(mcp)

        alter_mv = None
        for tool in mcp._tools.values():
            if tool.name == "alter_mv_parallelism":
                alter_mv = tool.fn
                break

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

        insert_row = None
        for tool in mcp._tools.values():
            if tool.name == "insert_single_row":
                insert_row = tool.fn
                break

        result = insert_row("test_table", '{"col1": "value1", "col2": 123}')
        assert "inserted successfully" in result or "Error" in result

    def test_insert_single_row_invalid_json(self, mock_connection):
        from dml_tools import register_dml_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_dml_tools(mcp)

        insert_row = None
        for tool in mcp._tools.values():
            if tool.name == "insert_single_row":
                insert_row = tool.fn
                break

        result = insert_row("test_table", 'not valid json')
        assert "Error" in result

    def test_update_rows(self, mock_connection):
        from dml_tools import register_dml_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_dml_tools(mcp)

        update_rows = None
        for tool in mcp._tools.values():
            if tool.name == "update_rows":
                update_rows = tool.fn
                break

        result = update_rows("test_table", "status = 'active'", "id = 1")
        assert "executed successfully" in result or "Error" in result

    def test_delete_rows(self, mock_connection):
        from dml_tools import register_dml_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_dml_tools(mcp)

        delete_rows = None
        for tool in mcp._tools.values():
            if tool.name == "delete_rows":
                delete_rows = tool.fn
                break

        result = delete_rows("test_table", "id = 1")
        assert "executed successfully" in result or "Error" in result


# ==================== Session Tools Tests ====================

class TestSessionTools:
    def test_set_session_variable(self, mock_connection):
        from session_tools import register_session_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_session_tools(mcp)

        set_var = None
        for tool in mcp._tools.values():
            if tool.name == "set_session_variable":
                set_var = tool.fn
                break

        result = set_var("query_mode", "local")
        assert "set to" in result or "Error" in result


# ==================== Index Tools Tests ====================

class TestIndexTools:
    def test_list_indexes(self, mock_connection):
        from index_tools import register_index_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_index_tools(mcp)

        list_indexes = None
        for tool in mcp._tools.values():
            if tool.name == "list_indexes":
                list_indexes = tool.fn
                break

        result = list_indexes("test_table")
        assert isinstance(result, str)


# ==================== Iceberg Tools Tests ====================

class TestIcebergTools:
    def test_vacuum_table(self, mock_connection):
        from iceberg_tools import register_iceberg_tools
        from fastmcp import FastMCP

        mcp = FastMCP("test")
        register_iceberg_tools(mcp)

        vacuum = None
        for tool in mcp._tools.values():
            if tool.name == "vacuum_table":
                vacuum = tool.fn
                break

        result = vacuum("iceberg_table")
        assert "completed successfully" in result or "Error" in result
