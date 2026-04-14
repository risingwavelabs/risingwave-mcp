import os

from fastmcp import FastMCP
from explain import explain_analyze, explain_query, explain_distsql

from query_tools import run_select_query, table_row_count, get_table_stats
from schema_tools import register_schema_tools
from ddl_tools import register_ddl_tools
from dml_tools import register_dml_tools
from management_tools import register_management_tools
from session_tools import register_session_tools
from streaming_tools import register_streaming_tools
from storage_tools import register_storage_tools
from source_tools import register_source_tools
from sink_tools import register_sink_tools
from connection_tools import register_connection_tools
from secret_tools import register_secret_tools
from index_tools import register_index_tools
from function_tools import register_function_tools
from user_tools import register_user_tools
from cluster_tools import register_cluster_tools
from iceberg_tools import register_iceberg_tools
from catalog_tools import register_catalog_tools
from monitoring_tools import register_monitoring_tools
from docs_tools import register_docs_tools

mcp = FastMCP("Risingwave MCP Server")


def register_tools():
    # Explain tools
    mcp.tool()(explain_analyze)
    mcp.tool()(explain_query)
    mcp.tool()(explain_distsql)

    # Query tools
    mcp.tool()(run_select_query)
    mcp.tool()(table_row_count)
    mcp.tool()(get_table_stats)

    # Schema management tools
    register_schema_tools(mcp)

    # DDL tools (create, alter, drop tables/MVs/schemas)
    register_ddl_tools(mcp)

    # DML tools (insert, update, delete)
    register_dml_tools(mcp)

    # Database management tools
    register_management_tools(mcp)

    # Session variable tools
    register_session_tools(mcp)

    # Streaming infrastructure tools (fragments, actors, backfill)
    register_streaming_tools(mcp)

    # Storage/Hummock tools (compaction, SST analysis)
    register_storage_tools(mcp)

    # Source management tools
    register_source_tools(mcp)

    # Sink management tools
    register_sink_tools(mcp)

    # Connection management tools
    register_connection_tools(mcp)

    # Secret management tools
    register_secret_tools(mcp)

    # Index management tools
    register_index_tools(mcp)

    # User-defined function tools
    register_function_tools(mcp)

    # User and access control tools
    register_user_tools(mcp)

    # Cluster management tools
    register_cluster_tools(mcp)

    # Iceberg/vacuum tools
    register_iceberg_tools(mcp)

    # System catalog query tools
    register_catalog_tools(mcp)

    # Monitoring and diagnostics tools
    register_monitoring_tools(mcp)

    # Documentation query tools
    register_docs_tools(mcp)


if __name__ == "__main__":
    register_tools()
    transport = os.getenv("MCP_TRANSPORT", "stdio")
    host = os.getenv("MCP_HOST", "127.0.0.1")
    port = int(os.getenv("MCP_PORT", "8000"))
    if transport == "stdio":
        mcp.run(transport="stdio")
    else:
        mcp.run(transport=transport, host=host, port=port)
