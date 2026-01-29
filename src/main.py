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

mcp = FastMCP("Risingwave MCP Server")


def register_tools():
    mcp.tool()(explain_analyze)
    mcp.tool()(explain_query)
    mcp.tool()(explain_distsql)

    mcp.tool()(run_select_query)
    mcp.tool()(table_row_count)
    mcp.tool()(get_table_stats)

    register_schema_tools(mcp)
    register_ddl_tools(mcp)
    register_dml_tools(mcp)
    register_management_tools(mcp)
    register_session_tools(mcp)
    register_streaming_tools(mcp)
    register_storage_tools(mcp)


if __name__ == "__main__":
    register_tools()
    mcp.run(transport="stdio")
