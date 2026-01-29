#!/usr/bin/env python3
"""
Interactive MCP Tool Inspector.
Lists all available tools and lets you test them interactively.

Usage:
    python tests/mcp_inspector.py
    python tests/mcp_inspector.py --list
    python tests/mcp_inspector.py --tool show_tables
"""
import sys
import os
import argparse
import json

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))


def setup_mcp():
    """Initialize MCP with all tools"""
    from main import register_tools, mcp
    register_tools()
    return mcp


def list_tools(mcp, category=None):
    """List all available tools, optionally filtered by category"""
    tools = sorted(mcp._tools.values(), key=lambda t: t.name)

    categories = {
        "query": ["run_select_query", "table_row_count", "get_table_stats"],
        "explain": ["explain_query", "explain_analyze", "explain_distsql"],
        "schema": ["show_tables", "list_databases", "describe_table", "list_schemas",
                   "list_materialized_views", "check_table_exists", "get_table_columns",
                   "show_create_table", "show_create_materialized_view", "describe_materialized_view",
                   "list_subscriptions", "list_table_privileges", "list_sinks", "show_create_sink",
                   "get_relation_info", "show_create_source"],
        "ddl": ["create_schema", "drop_table", "add_table_column", "drop_table_column",
                "rename_table", "alter_table_parallelism", "alter_table_source_rate_limit",
                "create_materialized_view", "drop_materialized_view", "alter_mv_parallelism",
                "alter_mv_backfill_rate_limit", "rename_materialized_view", "swap_materialized_views",
                "execute_ddl_statement", "create_kafka_table", "add_table_comment", "add_column_comment"],
        "dml": ["insert_single_row", "insert_multiple_rows", "update_rows", "delete_rows"],
        "source": ["list_sources", "describe_source", "alter_source_parallelism",
                   "alter_source_rate_limit", "refresh_source_schema", "rename_source", "drop_source"],
        "sink": ["describe_sink", "alter_sink_parallelism", "alter_sink_rate_limit",
                 "rename_sink", "drop_sink"],
        "connection": ["list_connections", "show_create_connection", "drop_connection"],
        "secret": ["list_secrets", "drop_secret"],
        "index": ["list_indexes", "describe_index", "show_create_index", "drop_index"],
        "function": ["list_functions", "show_create_function", "drop_function"],
        "user": ["list_users", "get_user_privileges", "get_schema_privileges", "get_database_privileges"],
        "cluster": ["show_cluster", "show_jobs", "cancel_jobs", "recover_cluster", "get_cluster_info"],
        "management": ["get_database_version", "show_running_queries", "flush_database"],
        "session": ["set_session_variable", "show_session_variable", "show_all_session_variables",
                    "alter_system_variable"],
        "iceberg": ["vacuum_table", "vacuum_full_table"],
        "streaming": ["get_worker_nodes", "get_actor_distribution", "get_fragment_stats",
                      "list_fragments", "list_actors", "get_backfill_progress"],
        "storage": ["get_table_storage_stats", "get_all_table_storage_stats", "get_sst_level_layout",
                    "get_compaction_group_configs", "get_compaction_group_summary"],
        "catalog": ["get_table_constraints", "get_views_info", "get_all_objects_in_schema",
                    "get_object_comments", "get_streaming_job_status", "get_system_tables_info",
                    "query_system_catalog"],
    }

    if category:
        if category not in categories:
            print(f"Unknown category: {category}")
            print(f"Available categories: {', '.join(categories.keys())}")
            return
        tool_names = categories[category]
        tools = [t for t in tools if t.name in tool_names]
        print(f"\n📦 {category.upper()} TOOLS ({len(tools)} tools)")
    else:
        print(f"\n📦 ALL TOOLS ({len(tools)} tools)")

    print("=" * 60)

    for tool in tools:
        # Get parameters
        import inspect
        sig = inspect.signature(tool.fn)
        params = []
        for name, param in sig.parameters.items():
            if param.default == inspect.Parameter.empty:
                params.append(f"{name}")
            else:
                params.append(f"{name}={param.default!r}")

        params_str = ", ".join(params) if params else "(no params)"
        print(f"\n{tool.name}")
        print(f"  Parameters: {params_str}")
        if tool.description:
            # First line of description
            desc = tool.description.split('\n')[0][:70]
            print(f"  Description: {desc}...")


def run_tool(mcp, tool_name, args_json=None):
    """Run a specific tool with optional JSON arguments"""
    tool = None
    for t in mcp._tools.values():
        if t.name == tool_name:
            tool = t
            break

    if not tool:
        print(f"Tool '{tool_name}' not found")
        return

    print(f"\n🔧 Running: {tool_name}")
    print("-" * 40)

    # Parse arguments
    kwargs = {}
    if args_json:
        try:
            kwargs = json.loads(args_json)
        except json.JSONDecodeError as e:
            print(f"Invalid JSON arguments: {e}")
            return

    try:
        result = tool.fn(**kwargs)
        print("Result:")
        # Pretty print JSON if possible
        try:
            parsed = json.loads(result)
            print(json.dumps(parsed, indent=2))
        except (json.JSONDecodeError, TypeError):
            print(result)
    except Exception as e:
        print(f"Error: {e}")


def interactive_mode(mcp):
    """Run in interactive mode"""
    print("\n🎮 Interactive MCP Inspector")
    print("Commands:")
    print("  list [category]  - List tools (categories: schema, ddl, dml, source, sink, etc.)")
    print("  run <tool>       - Run a tool")
    print("  run <tool> <json> - Run a tool with JSON arguments")
    print("  help <tool>      - Show tool details")
    print("  quit             - Exit")
    print()

    while True:
        try:
            cmd = input("mcp> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye!")
            break

        if not cmd:
            continue

        parts = cmd.split(maxsplit=2)
        action = parts[0].lower()

        if action == "quit" or action == "exit":
            print("Goodbye!")
            break
        elif action == "list":
            category = parts[1] if len(parts) > 1 else None
            list_tools(mcp, category)
        elif action == "run":
            if len(parts) < 2:
                print("Usage: run <tool> [json_args]")
                continue
            tool_name = parts[1]
            args_json = parts[2] if len(parts) > 2 else None
            run_tool(mcp, tool_name, args_json)
        elif action == "help":
            if len(parts) < 2:
                print("Usage: help <tool>")
                continue
            tool_name = parts[1]
            for t in mcp._tools.values():
                if t.name == tool_name:
                    print(f"\n{t.name}")
                    print("=" * 40)
                    print(t.description or "No description")
                    break
            else:
                print(f"Tool '{tool_name}' not found")
        else:
            print(f"Unknown command: {action}")


def main():
    parser = argparse.ArgumentParser(description="MCP Tool Inspector")
    parser.add_argument("--list", "-l", action="store_true", help="List all tools")
    parser.add_argument("--category", "-c", type=str, help="Filter by category")
    parser.add_argument("--tool", "-t", type=str, help="Run specific tool")
    parser.add_argument("--args", "-a", type=str, help="JSON arguments for tool")
    parser.add_argument("--interactive", "-i", action="store_true", help="Interactive mode")
    args = parser.parse_args()

    # Set default connection if not set
    if not os.environ.get("RISINGWAVE_CONNECTION_STR"):
        os.environ["RISINGWAVE_CONNECTION_STR"] = "postgresql://root:root@localhost:4566/dev"

    print("🚀 MCP Tool Inspector")
    print("=" * 50)

    try:
        mcp = setup_mcp()
        print(f"✓ Loaded {len(mcp._tools)} tools")
    except Exception as e:
        print(f"✗ Failed to initialize MCP: {e}")
        sys.exit(1)

    if args.list:
        list_tools(mcp, args.category)
    elif args.tool:
        run_tool(mcp, args.tool, args.args)
    elif args.interactive or (not args.list and not args.tool):
        interactive_mode(mcp)


if __name__ == "__main__":
    main()
