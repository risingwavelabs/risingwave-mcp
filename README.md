# RisingWave MCP Server

**RisingWave MCP Server** is a comprehensive [Model Context Protocol (MCP)](https://modelcontextprotocol.io/introduction) server that lets you query and manage your [RisingWave](https://risingwave.com) streaming database using natural language through AI assistants like VS Code Copilot and Claude Desktop.

---

## Features

- Real-time access to RisingWave tables, materialized views, and streaming data
- Complete management of sources, sinks, connections, indexes, and UDFs
- Streaming job monitoring with fragment, actor, and backfill tracking
- Storage analysis with Hummock/compaction insights
- Built on [`FastMCP`](https://github.com/jlowin/fastmcp) and [`risingwave-py`](https://github.com/risingwavelabs/risingwave-py) with high-performance STDIO transport
- Seamless integration with VS Code Copilot, Claude Desktop, and other MCP-compatible tools

---

## Installation

```bash
git clone https://github.com/risingwavelabs/risingwave-mcp.git
cd risingwave-mcp
pip install -r requirements.txt
```

---

## Setting Up

You'll need a running RisingWave instance—either locally or in the cloud.

### Option 1: Run RisingWave Locally

```bash
# Install RisingWave standalone
curl -L https://risingwave.com/sh | sh

# macOS
risingwave

# Linux
./risingwave
```

For Docker or other options, see the docs:
[https://docs.risingwave.com/get-started/quickstart](https://docs.risingwave.com/get-started/quickstart)

### Option 2: Use RisingWave Cloud

You can also spin up a free-tier cluster in seconds:
[https://cloud.risingwave.com/auth/signin](https://cloud.risingwave.com/auth/signin)

---

## Integration

### VS Code Copilot

1. In the **VS Code Chat** panel: Agent Mode → Select Tools → Create MCP Server.
2. Add the following to `.vscode/mcp.json`.

#### Option 1: Use a connection string

```json
{
  "servers": {
    "risingwave-mcp": {
      "type": "stdio",
      "command": "python",
      "args": ["path_to/risingwave-mcp/src/main.py"],
      "env": {
        "RISINGWAVE_CONNECTION_STR": "postgresql://root:root@localhost:4566/dev"
      }
    }
  }
}
```

**Explanation:**

- `postgresql://` — Use PostgreSQL protocol (RisingWave is compatible)
- `root:root@` — Username and password
- `localhost:4566` — Host and port
- `/dev` — Database name

#### Option 2: Use individual parameters

```json
{
  "servers": {
    "risingwave-mcp": {
      "type": "stdio",
      "command": "python",
      "args": ["path_to/risingwave-mcp/src/main.py"],
      "env": {
        "RISINGWAVE_HOST": "localhost",
        "RISINGWAVE_PORT": "4566",
        "RISINGWAVE_USER": "root",
        "RISINGWAVE_PASSWORD": "root",
        "RISINGWAVE_DATABASE": "dev",
        "RISINGWAVE_SSLMODE": "disable"
      }
    }
  }
}
```

3. Start chatting!

   Ask questions like:
   - _"List my tables"_
   - _"Create a materialized view that aggregates payments by minute"_
   - _"Show me the backfill progress for all streaming jobs"_
   - _"What's the cluster status?"_

---

### Claude Desktop

1. Add the MCP server to your `claude_desktop_config.json` under `mcpServers`:

```json
{
  "mcpServers": {
    "risingwave-mcp": {
      "command": "python",
      "args": ["path_to/risingwave-mcp/src/main.py"],
      "env": {
        "RISINGWAVE_CONNECTION_STR": "postgresql://root:root@localhost:4566/dev"
      }
    }
  }
}
```

2. Restart Claude Desktop to apply changes.

---

### Manual Testing (Dev / CI)

You can run the MCP server directly from the CLI:

```bash
python src/main.py
```

This will listen for MCP messages over STDIN/STDOUT.

---

## Available Tools

The MCP server provides **100+ tools** organized into the following categories:

### Query Tools

| Tool Name | Description |
|-----------|-------------|
| `run_select_query` | Execute a SELECT query against RisingWave |
| `table_row_count` | Get the row count for a specific table |
| `get_table_stats` | Get comprehensive statistics for a table |

### Explain Tools

| Tool Name | Description |
|-----------|-------------|
| `explain_query` | Get query execution plan without running it |
| `explain_analyze` | Execute EXPLAIN ANALYZE for detailed runtime statistics |
| `explain_distsql` | Get distributed execution plan for streaming statements |

### Schema Tools

| Tool Name | Description |
|-----------|-------------|
| `show_tables` | List all tables in the database |
| `list_databases` | List all databases in the cluster |
| `describe_table` | Describe table structure (columns, types) |
| `describe_materialized_view` | Describe materialized view structure |
| `show_create_table` | Show CREATE TABLE statement |
| `show_create_materialized_view` | Show CREATE MATERIALIZED VIEW statement |
| `check_table_exists` | Check if a table or MV exists |
| `list_schemas` | List all schemas in the database |
| `list_materialized_views` | List all materialized views in a schema |
| `get_table_columns` | Get detailed column information |
| `list_subscriptions` | List all subscriptions in a schema |
| `list_table_privileges` | List privileges for a table |
| `list_sinks` | List all sinks in the database |
| `show_create_sink` | Show CREATE SINK statement |
| `get_relation_info` | Get info about any relation by name |
| `show_create_source` | Show CREATE SOURCE statement |

### DDL Tools (Data Definition)

| Tool Name | Description |
|-----------|-------------|
| `create_schema` | Create a new schema |
| `drop_table` | Drop a table |
| `add_table_column` | Add a column to a table |
| `drop_table_column` | Remove a column from a table |
| `rename_table` | Rename a table |
| `alter_table_parallelism` | Change table parallelism |
| `alter_table_source_rate_limit` | Modify source rate limit for tables |
| `create_materialized_view` | Create a new materialized view |
| `drop_materialized_view` | Drop a materialized view |
| `alter_mv_parallelism` | Change MV parallelism |
| `alter_mv_backfill_rate_limit` | Modify MV backfill rate limit |
| `rename_materialized_view` | Rename a materialized view |
| `swap_materialized_views` | Atomically swap two MVs |
| `execute_ddl_statement` | Execute generic DDL statements |
| `create_kafka_table` | Create a table with Kafka connector |
| `add_table_comment` | Add comment to a table |
| `add_column_comment` | Add comment to a column |

### DML Tools (Data Manipulation)

| Tool Name | Description |
|-----------|-------------|
| `insert_single_row` | Insert a single row into a table |
| `insert_multiple_rows` | Batch insert multiple rows |
| `update_rows` | Update rows in a table |
| `delete_rows` | Delete rows from a table |

### Source Tools

| Tool Name | Description |
|-----------|-------------|
| `list_sources` | List all sources |
| `describe_source` | Describe source structure |
| `alter_source_parallelism` | Change source parallelism |
| `alter_source_rate_limit` | Modify source ingestion rate |
| `refresh_source_schema` | Refresh schema from registry |
| `rename_source` | Rename a source |
| `drop_source` | Drop a source |

### Sink Tools

| Tool Name | Description |
|-----------|-------------|
| `describe_sink` | Describe sink structure |
| `alter_sink_parallelism` | Change sink parallelism |
| `alter_sink_rate_limit` | Modify sink output rate |
| `rename_sink` | Rename a sink |
| `drop_sink` | Drop a sink |

### Connection Tools

| Tool Name | Description |
|-----------|-------------|
| `list_connections` | List all connections |
| `show_create_connection` | Show CREATE CONNECTION statement |
| `drop_connection` | Drop a connection |

### Secret Tools

| Tool Name | Description |
|-----------|-------------|
| `list_secrets` | List all secrets (values hidden) |
| `drop_secret` | Drop a secret |

### Index Tools

| Tool Name | Description |
|-----------|-------------|
| `list_indexes` | List indexes for a table |
| `describe_index` | Describe index structure |
| `show_create_index` | Show CREATE INDEX statement |
| `drop_index` | Drop an index |

### Function Tools (UDFs)

| Tool Name | Description |
|-----------|-------------|
| `list_functions` | List all user-defined functions |
| `show_create_function` | Show CREATE FUNCTION statement |
| `drop_function` | Drop a function |

### User & Access Control Tools

| Tool Name | Description |
|-----------|-------------|
| `list_users` | List all users |
| `get_user_privileges` | Get privileges for a user |
| `get_schema_privileges` | Get schema-level privileges |
| `get_database_privileges` | Get database-level privileges |

### Cluster Tools

| Tool Name | Description |
|-----------|-------------|
| `show_cluster` | Display cluster node details |
| `show_jobs` | List active streaming jobs |
| `cancel_jobs` | Cancel running jobs |
| `recover_cluster` | Trigger manual recovery |
| `get_cluster_info` | Get comprehensive cluster info |

### Database Management Tools

| Tool Name | Description |
|-----------|-------------|
| `get_database_version` | Get RisingWave version info |
| `show_running_queries` | Show currently running queries |
| `flush_database` | Force flush pending writes |

### Session Tools

| Tool Name | Description |
|-----------|-------------|
| `set_session_variable` | Set a session variable |
| `show_session_variable` | Show a session variable value |
| `show_all_session_variables` | Show all session variables |
| `alter_system_variable` | Alter system-wide defaults |

### Iceberg Tools

| Tool Name | Description |
|-----------|-------------|
| `vacuum_table` | Expire old snapshots on Iceberg table |
| `vacuum_full_table` | Full compaction + vacuum |

### Streaming Infrastructure Tools

| Tool Name | Description |
|-----------|-------------|
| `get_worker_nodes` | Get all worker nodes with status |
| `get_actor_distribution` | Show actor count per worker |
| `get_fragment_stats` | Get fragment statistics |
| `get_fragment_communication_cost` | Calculate shuffle overhead |
| `list_fragments` | List all fragments |
| `get_fragment_details` | Get detailed fragment info |
| `list_actors` | List all actors |
| `get_actors_for_fragment` | Get actors for a fragment |
| `get_object_id_by_name` | Get internal ID by name |
| `get_mv_by_fragment_id` | Find MV by fragment ID |
| `get_mv_by_actor_id` | Trace actor to MV |
| `get_worker_for_actor` | Find worker for actor |
| `get_workers_for_fragment` | Find workers for fragment |
| `get_streaming_job_fragments` | Get fragments for a job |
| `get_streaming_job_actors` | Get actors for a job |
| `get_backfill_progress` | Get backfill progress |
| `get_fragment_backfill_progress` | Get fragment-level backfill |
| `get_upstream_fragments` | Get upstream fragments |
| `get_downstream_fragments` | Get downstream fragments |
| `get_fragments_by_state_table` | Find fragments by state table |
| `get_streaming_parallelism` | Get parallelism settings |
| `get_mv_worker_distribution` | Get MV actor distribution |
| `get_job_by_state_table_id` | Find job by state table |
| `get_internal_tables` | Get internal state tables |
| `check_vnode_distribution` | Check for data skew |
| `get_downstream_dependents` | Find dependent MVs |
| `get_upstream_dependents` | Find upstream dependencies |
| `get_sink_decouple_status` | Check sink decouple status |
| `get_sink_logstore_lag` | Monitor decoupled sink lag |

### Storage Tools (Hummock)

| Tool Name | Description |
|-----------|-------------|
| `get_table_storage_stats` | Get storage stats for a table |
| `get_all_table_storage_stats` | Get storage stats for all tables |
| `get_sst_level_layout` | Check SST layout per level |
| `get_compaction_group_configs` | Get compaction configs |
| `get_sst_delete_ratio` | Check SST delete ratios |
| `get_sst_vnode_distribution` | Check vnode distribution in SSTs |
| `get_large_state_tables_in_compaction_group` | Find large state tables |
| `get_l0_stats` | Get L0 level statistics |
| `get_jobs_in_compaction_group` | Find jobs in compaction group |
| `get_meta_snapshot_info` | Get meta snapshot info |
| `get_compaction_group_summary` | Get compaction group summary |

### System Catalog Tools

| Tool Name | Description |
|-----------|-------------|
| `get_table_constraints` | Get table constraints |
| `get_views_info` | Get views information |
| `get_all_objects_in_schema` | List all objects in schema |
| `get_object_comments` | Get object comments |
| `get_streaming_job_status` | Get streaming job status |
| `get_system_tables_info` | List system tables |
| `query_system_catalog` | Query any system catalog |

---

## Architecture

```
src/
├── main.py              # Entry point, registers all tools
├── connection.py        # Database connection setup
├── query_tools.py       # SELECT queries and table stats
├── explain.py           # EXPLAIN and EXPLAIN ANALYZE
├── schema_tools.py      # Schema inspection tools
├── ddl_tools.py         # CREATE, ALTER, DROP operations
├── dml_tools.py         # INSERT, UPDATE, DELETE operations
├── source_tools.py      # Source management
├── sink_tools.py        # Sink management
├── connection_tools.py  # Connection management
├── secret_tools.py      # Secret management
├── index_tools.py       # Index management
├── function_tools.py    # UDF management
├── user_tools.py        # User and access control
├── cluster_tools.py     # Cluster management
├── management_tools.py  # Database management
├── session_tools.py     # Session variables
├── iceberg_tools.py     # Iceberg/vacuum operations
├── streaming_tools.py   # Fragments, actors, backfill
├── storage_tools.py     # Hummock/compaction analysis
└── catalog_tools.py     # System catalog queries
```

---

## Example Conversations

### Query Data
```
User: Show me all tables in the public schema
Assistant: [calls show_tables]

User: What's in the orders table?
Assistant: [calls describe_table with table_name="orders"]

User: Get me the last 10 orders
Assistant: [calls run_select_query with query="SELECT * FROM orders ORDER BY created_at DESC LIMIT 10"]
```

### Monitor Streaming Jobs
```
User: What's the backfill progress?
Assistant: [calls get_backfill_progress]

User: Show me the cluster status
Assistant: [calls show_cluster]

User: Are there any running jobs?
Assistant: [calls show_jobs]
```

### Manage Objects
```
User: Create a materialized view that counts orders per customer
Assistant: [calls create_materialized_view with appropriate SQL]

User: Change the parallelism of mv_orders to 4
Assistant: [calls alter_mv_parallelism with mv_name="mv_orders", parallelism="4"]

User: Drop the old_source source
Assistant: [calls drop_source with source_name="old_source"]
```

### Debug Performance
```
User: Explain this query: SELECT * FROM orders WHERE status = 'pending'
Assistant: [calls explain_query]

User: What's the storage usage for each table?
Assistant: [calls get_all_table_storage_stats]

User: Check for data skew in the orders table
Assistant: [calls check_vnode_distribution with table_name="orders", distribution_key="customer_id"]
```

---

## Testing

The MCP server includes comprehensive testing tools:

### 1. Unit Tests (with mocked connections)

```bash
# Install test dependencies
pip install pytest

# Run all unit tests
pytest tests/test_tools.py -v

# Run specific test class
pytest tests/test_tools.py::TestSchemaTools -v
```

### 2. Integration Tests (requires running RisingWave)

```bash
# Set connection string
export RISINGWAVE_CONNECTION_STR="postgresql://root:root@localhost:4566/dev"

# Run all integration tests
python tests/integration_test.py

# Run specific category
python tests/integration_test.py --category schema
python tests/integration_test.py --category cluster
python tests/integration_test.py --category streaming

# Verbose output
python tests/integration_test.py -v
```

Available test categories: `schema`, `source`, `sink`, `cluster`, `management`, `session`, `streaming`, `storage`, `catalog`, `connection`, `secret`, `function`, `user`, `explain`, `query`

### 3. Interactive MCP Inspector

```bash
# Start interactive mode
python tests/mcp_inspector.py

# List all tools
python tests/mcp_inspector.py --list

# List tools by category
python tests/mcp_inspector.py --list --category schema

# Run a specific tool
python tests/mcp_inspector.py --tool show_tables

# Run a tool with arguments
python tests/mcp_inspector.py --tool describe_table --args '{"table_name": "orders"}'
```

Interactive mode commands:
```
mcp> list              # List all tools
mcp> list schema       # List schema tools
mcp> run show_tables   # Run a tool
mcp> run describe_table {"table_name": "orders"}  # Run with args
mcp> help show_cluster # Show tool details
mcp> quit              # Exit
```

### 4. Manual Testing with MCP CLI

If you have the MCP CLI installed:

```bash
# Test the server directly
echo '{"jsonrpc": "2.0", "method": "tools/list", "id": 1}' | python src/main.py
```

---

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.

---

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
