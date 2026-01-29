#!/usr/bin/env python3
"""
Integration tests for RisingWave MCP tools.
Requires a running RisingWave instance.

Usage:
    # Set connection string
    export RISINGWAVE_CONNECTION_STR="postgresql://root:root@localhost:4566/dev"

    # Run all tests
    python tests/integration_test.py

    # Run specific test category
    python tests/integration_test.py --category schema
"""
import sys
import os
import argparse
import json

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from fastmcp import FastMCP


def setup_mcp():
    """Initialize MCP with all tools"""
    from main import register_tools, mcp
    register_tools()
    return mcp


def get_tool(mcp, name):
    """Get a tool function by name"""
    for tool in mcp._tools.values():
        if tool.name == name:
            return tool.fn
    raise ValueError(f"Tool '{name}' not found")


def print_result(test_name, result, success=True):
    """Print test result with formatting"""
    status = "✓" if success else "✗"
    print(f"  {status} {test_name}")
    if not success or os.environ.get("VERBOSE"):
        print(f"    Result: {result[:200]}..." if len(str(result)) > 200 else f"    Result: {result}")


def test_schema_tools(mcp):
    """Test schema-related tools"""
    print("\n📋 Testing Schema Tools...")

    # Test show_tables
    try:
        result = get_tool(mcp, "show_tables")()
        print_result("show_tables", result, "Error" not in result)
    except Exception as e:
        print_result("show_tables", str(e), False)

    # Test list_databases
    try:
        result = get_tool(mcp, "list_databases")()
        print_result("list_databases", result, "Error" not in result)
    except Exception as e:
        print_result("list_databases", str(e), False)

    # Test list_schemas
    try:
        result = get_tool(mcp, "list_schemas")()
        print_result("list_schemas", result, "Error" not in result)
    except Exception as e:
        print_result("list_schemas", str(e), False)

    # Test list_materialized_views
    try:
        result = get_tool(mcp, "list_materialized_views")()
        print_result("list_materialized_views", result, "Error" not in result)
    except Exception as e:
        print_result("list_materialized_views", str(e), False)


def test_source_tools(mcp):
    """Test source-related tools"""
    print("\n📥 Testing Source Tools...")

    # Test list_sources
    try:
        result = get_tool(mcp, "list_sources")()
        print_result("list_sources", result, "Error" not in result)
    except Exception as e:
        print_result("list_sources", str(e), False)


def test_sink_tools(mcp):
    """Test sink-related tools"""
    print("\n📤 Testing Sink Tools...")

    # Test list_sinks
    try:
        result = get_tool(mcp, "list_sinks")()
        print_result("list_sinks", result, "Error" not in result)
    except Exception as e:
        print_result("list_sinks", str(e), False)


def test_cluster_tools(mcp):
    """Test cluster-related tools"""
    print("\n🖥️  Testing Cluster Tools...")

    # Test show_cluster
    try:
        result = get_tool(mcp, "show_cluster")()
        print_result("show_cluster", result, "Error" not in result)
    except Exception as e:
        print_result("show_cluster", str(e), False)

    # Test show_jobs
    try:
        result = get_tool(mcp, "show_jobs")()
        print_result("show_jobs", result, "Error" not in result)
    except Exception as e:
        print_result("show_jobs", str(e), False)

    # Test get_cluster_info
    try:
        result = get_tool(mcp, "get_cluster_info")()
        print_result("get_cluster_info", result, "Error" not in result)
    except Exception as e:
        print_result("get_cluster_info", str(e), False)


def test_management_tools(mcp):
    """Test management-related tools"""
    print("\n🔧 Testing Management Tools...")

    # Test get_database_version
    try:
        result = get_tool(mcp, "get_database_version")()
        print_result("get_database_version", result, "Error" not in result)
    except Exception as e:
        print_result("get_database_version", str(e), False)


def test_session_tools(mcp):
    """Test session-related tools"""
    print("\n⚙️  Testing Session Tools...")

    # Test show_all_session_variables
    try:
        result = get_tool(mcp, "show_all_session_variables")()
        print_result("show_all_session_variables", result, "Error" not in result)
    except Exception as e:
        print_result("show_all_session_variables", str(e), False)

    # Test show_session_variable
    try:
        result = get_tool(mcp, "show_session_variable")("query_mode")
        print_result("show_session_variable", result, "Error" not in result)
    except Exception as e:
        print_result("show_session_variable", str(e), False)


def test_streaming_tools(mcp):
    """Test streaming infrastructure tools"""
    print("\n🌊 Testing Streaming Tools...")

    # Test get_worker_nodes
    try:
        result = get_tool(mcp, "get_worker_nodes")()
        print_result("get_worker_nodes", result, "Error" not in result)
    except Exception as e:
        print_result("get_worker_nodes", str(e), False)

    # Test get_backfill_progress
    try:
        result = get_tool(mcp, "get_backfill_progress")()
        print_result("get_backfill_progress", result, "Error" not in result)
    except Exception as e:
        print_result("get_backfill_progress", str(e), False)

    # Test get_streaming_parallelism
    try:
        result = get_tool(mcp, "get_streaming_parallelism")()
        print_result("get_streaming_parallelism", result, "Error" not in result)
    except Exception as e:
        print_result("get_streaming_parallelism", str(e), False)


def test_storage_tools(mcp):
    """Test storage/Hummock tools"""
    print("\n💾 Testing Storage Tools...")

    # Test get_all_table_storage_stats
    try:
        result = get_tool(mcp, "get_all_table_storage_stats")()
        print_result("get_all_table_storage_stats", result, "Error" not in result)
    except Exception as e:
        print_result("get_all_table_storage_stats", str(e), False)

    # Test get_compaction_group_summary
    try:
        result = get_tool(mcp, "get_compaction_group_summary")()
        print_result("get_compaction_group_summary", result, "Error" not in result)
    except Exception as e:
        print_result("get_compaction_group_summary", str(e), False)


def test_catalog_tools(mcp):
    """Test system catalog tools"""
    print("\n📚 Testing Catalog Tools...")

    # Test get_system_tables_info
    try:
        result = get_tool(mcp, "get_system_tables_info")()
        print_result("get_system_tables_info", result, "Error" not in result)
    except Exception as e:
        print_result("get_system_tables_info", str(e), False)

    # Test get_streaming_job_status
    try:
        result = get_tool(mcp, "get_streaming_job_status")()
        print_result("get_streaming_job_status", result, "Error" not in result)
    except Exception as e:
        print_result("get_streaming_job_status", str(e), False)


def test_connection_tools(mcp):
    """Test connection tools"""
    print("\n🔗 Testing Connection Tools...")

    # Test list_connections
    try:
        result = get_tool(mcp, "list_connections")()
        print_result("list_connections", result, "Error" not in result)
    except Exception as e:
        print_result("list_connections", str(e), False)


def test_secret_tools(mcp):
    """Test secret tools"""
    print("\n🔐 Testing Secret Tools...")

    # Test list_secrets
    try:
        result = get_tool(mcp, "list_secrets")()
        print_result("list_secrets", result, "Error" not in result)
    except Exception as e:
        print_result("list_secrets", str(e), False)


def test_function_tools(mcp):
    """Test UDF tools"""
    print("\n📦 Testing Function Tools...")

    # Test list_functions
    try:
        result = get_tool(mcp, "list_functions")()
        print_result("list_functions", result, "Error" not in result)
    except Exception as e:
        print_result("list_functions", str(e), False)


def test_user_tools(mcp):
    """Test user/access control tools"""
    print("\n👤 Testing User Tools...")

    # Test list_users
    try:
        result = get_tool(mcp, "list_users")()
        print_result("list_users", result, "Error" not in result)
    except Exception as e:
        print_result("list_users", str(e), False)


def test_explain_tools(mcp):
    """Test explain tools"""
    print("\n🔍 Testing Explain Tools...")

    # Test explain_query
    try:
        result = get_tool(mcp, "explain_query")("SELECT 1")
        print_result("explain_query", result, "Error" not in result)
    except Exception as e:
        print_result("explain_query", str(e), False)


def test_query_tools(mcp):
    """Test query tools"""
    print("\n🔎 Testing Query Tools...")

    # Test run_select_query
    try:
        result = get_tool(mcp, "run_select_query")("SELECT 1 as test")
        print_result("run_select_query", result, "Error" not in result)
    except Exception as e:
        print_result("run_select_query", str(e), False)


def main():
    parser = argparse.ArgumentParser(description="Integration tests for RisingWave MCP")
    parser.add_argument("--category", type=str, help="Test specific category",
                       choices=["schema", "source", "sink", "cluster", "management",
                               "session", "streaming", "storage", "catalog", "connection",
                               "secret", "function", "user", "explain", "query", "all"])
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    args = parser.parse_args()

    if args.verbose:
        os.environ["VERBOSE"] = "1"

    print("🚀 RisingWave MCP Integration Tests")
    print("=" * 50)

    # Check for connection string
    conn_str = os.environ.get("RISINGWAVE_CONNECTION_STR")
    if not conn_str:
        print("⚠️  Warning: RISINGWAVE_CONNECTION_STR not set")
        print("   Using default: postgresql://root:root@localhost:4566/dev")
        os.environ["RISINGWAVE_CONNECTION_STR"] = "postgresql://root:root@localhost:4566/dev"

    try:
        mcp = setup_mcp()
        print(f"✓ MCP initialized with {len(mcp._tools)} tools")
    except Exception as e:
        print(f"✗ Failed to initialize MCP: {e}")
        sys.exit(1)

    category = args.category or "all"

    test_funcs = {
        "schema": test_schema_tools,
        "source": test_source_tools,
        "sink": test_sink_tools,
        "cluster": test_cluster_tools,
        "management": test_management_tools,
        "session": test_session_tools,
        "streaming": test_streaming_tools,
        "storage": test_storage_tools,
        "catalog": test_catalog_tools,
        "connection": test_connection_tools,
        "secret": test_secret_tools,
        "function": test_function_tools,
        "user": test_user_tools,
        "explain": test_explain_tools,
        "query": test_query_tools,
    }

    if category == "all":
        for func in test_funcs.values():
            try:
                func(mcp)
            except Exception as e:
                print(f"  ✗ Category failed: {e}")
    else:
        test_funcs[category](mcp)

    print("\n" + "=" * 50)
    print("✓ Integration tests completed")


if __name__ == "__main__":
    main()
