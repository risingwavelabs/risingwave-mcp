from fastmcp import FastMCP
from connection import setup_risingwave_connection
from sql_utils import is_valid_identifier


def register_ddl_tools(mcp: FastMCP):
    """Register all DDL-related MCP tools"""

    # ==================== Schema Operations ====================

    @mcp.tool
    def create_schema(schema_name: str, if_not_exists: bool = True) -> str:
        """
        Create a new schema in the database.

        Args:
            schema_name: Name of the schema to create
            if_not_exists: If True, do not error if schema already exists (default: True)

        Returns:
            Success or error message
        """
        rw = setup_risingwave_connection()
        if_not_exists_clause = "IF NOT EXISTS " if if_not_exists else ""
        query = f"CREATE SCHEMA {if_not_exists_clause}{schema_name}"
        try:
            rw.execute(query)
            return f"Schema '{schema_name}' created successfully"
        except Exception as e:
            return f"Error creating schema: {str(e)}"

    # ==================== Table Operations ====================

    @mcp.tool
    def drop_table(table_name: str, if_exists: bool = False, cascade: bool = False) -> str:
        """
        Drop a table from the database.

        Args:
            table_name: Name of the table to drop (can include schema prefix)
            if_exists: If True, do not error if table does not exist
            cascade: If True, also drop dependent objects

        Returns:
            Success or error message
        """
        rw = setup_risingwave_connection()
        if_exists_clause = "IF EXISTS " if if_exists else ""
        cascade_clause = " CASCADE" if cascade else ""
        query = f"DROP TABLE {if_exists_clause}{table_name}{cascade_clause}"
        try:
            rw.execute(query)
            return f"Table '{table_name}' dropped successfully"
        except Exception as e:
            return f"Error dropping table: {str(e)}"

    @mcp.tool
    def add_table_column(table_name: str, column_name: str, data_type: str,
                         default_value: str = None) -> str:
        """
        Add a new column to an existing table.

        Args:
            table_name: Name of the table
            column_name: Name of the new column
            data_type: Data type of the column (e.g., 'VARCHAR', 'INT', 'TIMESTAMP')
            default_value: Optional default value for the column

        Returns:
            Success or error message
        """
        rw = setup_risingwave_connection()
        default_clause = f" DEFAULT {default_value}" if default_value else ""
        query = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {data_type}{default_clause}"
        try:
            rw.execute(query)
            return f"Column '{column_name}' added to table '{table_name}'"
        except Exception as e:
            return f"Error adding column: {str(e)}"

    @mcp.tool
    def drop_table_column(table_name: str, column_name: str, if_exists: bool = False) -> str:
        """
        Remove a column from an existing table.
        Note: Cannot drop columns referenced by materialized views or indexes.

        Args:
            table_name: Name of the table
            column_name: Name of the column to drop
            if_exists: If True, do not error if column does not exist

        Returns:
            Success or error message
        """
        rw = setup_risingwave_connection()
        if_exists_clause = "IF EXISTS " if if_exists else ""
        query = f"ALTER TABLE {table_name} DROP COLUMN {if_exists_clause}{column_name}"
        try:
            rw.execute(query)
            return f"Column '{column_name}' dropped from table '{table_name}'"
        except Exception as e:
            return f"Error dropping column: {str(e)}"

    @mcp.tool
    def rename_table(table_name: str, new_name: str) -> str:
        """
        Rename a table.

        Args:
            table_name: Current name of the table
            new_name: New name for the table

        Returns:
            Success or error message
        """
        rw = setup_risingwave_connection()
        query = f"ALTER TABLE {table_name} RENAME TO {new_name}"
        try:
            rw.execute(query)
            return f"Table '{table_name}' renamed to '{new_name}'"
        except Exception as e:
            return f"Error renaming table: {str(e)}"

    @mcp.tool
    def alter_table_parallelism(table_name: str, parallelism: str) -> str:
        """
        Change the parallelism of a table (for tables with connectors).

        Args:
            table_name: Name of the table
            parallelism: Either 'ADAPTIVE' or a fixed number (e.g., '4')

        Returns:
            Success or error message
        """
        rw = setup_risingwave_connection()
        if parallelism.upper() != 'ADAPTIVE' and not parallelism.isdigit():
            return "Error: parallelism must be 'ADAPTIVE' or a positive integer"

        parallelism_value = parallelism.upper() if parallelism.upper() == 'ADAPTIVE' else parallelism
        query = f"ALTER TABLE {table_name} SET PARALLELISM = {parallelism_value}"
        try:
            rw.execute(query)
            return f"Table '{table_name}' parallelism set to {parallelism_value}"
        except Exception as e:
            return f"Error altering table parallelism: {str(e)}"

    @mcp.tool
    def alter_table_source_rate_limit(table_name: str, rate_limit: str) -> str:
        """
        Modify the source ingestion rate limit for a connector-based table.

        Args:
            table_name: Name of the table
            rate_limit: Either 'default' or a numeric value (records per second)

        Returns:
            Success or error message
        """
        rw = setup_risingwave_connection()
        if rate_limit.lower() != 'default' and not rate_limit.isdigit():
            return "Error: rate_limit must be 'default' or a positive integer"

        rate_value = 'default' if rate_limit.lower() == 'default' else rate_limit
        query = f"ALTER TABLE {table_name} SET SOURCE_RATE_LIMIT = {rate_value}"
        try:
            rw.execute(query)
            return f"Table '{table_name}' source rate limit set to {rate_value}"
        except Exception as e:
            return f"Error altering table source rate limit: {str(e)}"

    # ==================== Materialized View Operations ====================

    @mcp.tool
    def create_materialized_view(name: str, sql_statement: str, schema_name: str = "public") -> str:
        """
        Create a new materialized view.

        Args:
            name: Name of the materialized view
            sql_statement: SQL statement for the materialized view
            schema_name: Schema name (default: "public")

        Returns:
            Success message
        """
        rw = setup_risingwave_connection()
        try:
            rw.mv(name=name, stmt=sql_statement, schema_name=schema_name)
            return f"Materialized view '{name}' created successfully in schema '{schema_name}'"
        except Exception as e:
            return f"Error creating materialized view: {str(e)}"

    @mcp.tool
    def drop_materialized_view(name: str, schema_name: str = "public") -> str:
        """
        Drop a materialized view.

        Args:
            name: Name of the materialized view to drop
            schema_name: Schema name (default: "public")

        Returns:
            Success or error message
        """
        rw = setup_risingwave_connection()
        try:
            query = f"DROP MATERIALIZED VIEW {schema_name}.{name}"
            rw.execute(query)
            return f"Materialized view '{name}' dropped successfully from schema '{schema_name}'"
        except Exception as e:
            return f"Error dropping materialized view: {str(e)}"

    @mcp.tool
    def execute_ddl_statement(sql_statement: str) -> str:
        """
        Execute a DDL (Data Definition Language) statement like CREATE TABLE, CREATE SCHEMA, etc.

        Args:
            sql_statement: The DDL SQL statement to execute

        Returns:
            Success or error message
        """
        # Security check: only allow DDL statements
        sql_upper = sql_statement.strip().upper()
        allowed_ddl = ['CREATE', 'ALTER', 'DROP', 'TRUNCATE']

        if not any(sql_upper.startswith(keyword) for keyword in allowed_ddl):
            raise ValueError(
                "Only DDL statements (CREATE, ALTER, DROP, TRUNCATE) are allowed")

        # Additional security: prevent dangerous operations
        dangerous_keywords = ['DROP DATABASE', 'DROP SCHEMA', 'TRUNCATE']
        if any(keyword in sql_upper for keyword in dangerous_keywords):
            raise ValueError("Dangerous DDL operations are not allowed")

        rw = setup_risingwave_connection()
        try:
            rw.execute(sql_statement)
            return f"DDL statement executed successfully: {sql_statement[:100]}..."
        except Exception as e:
            return f"Error executing DDL statement: {str(e)}"

    @mcp.tool
    def alter_mv_parallelism(mv_name: str, parallelism: str) -> str:
        """
        Change the parallelism of a materialized view.

        Args:
            mv_name: Name of the materialized view
            parallelism: Either 'ADAPTIVE', '0' (for ADAPTIVE), or a fixed number (e.g., '4')

        Returns:
            Success or error message
        """
        rw = setup_risingwave_connection()
        if not is_valid_identifier(mv_name):
            return f"Error: Invalid materialized view name: '{mv_name}'"
        if parallelism.upper() != 'ADAPTIVE' and not parallelism.isdigit():
            return "Error: parallelism must be 'ADAPTIVE', '0' (for ADAPTIVE), or a positive integer"

        # Map '0' to ADAPTIVE as documented
        if parallelism == '0' or parallelism.upper() == 'ADAPTIVE':
            parallelism_value = 'ADAPTIVE'
        else:
            parallelism_value = parallelism
        query = f"ALTER MATERIALIZED VIEW {mv_name} SET PARALLELISM = {parallelism_value}"
        try:
            rw.execute(query)
            return f"Materialized view '{mv_name}' parallelism set to {parallelism_value}"
        except Exception as e:
            return f"Error altering MV parallelism: {str(e)}"

    @mcp.tool
    def alter_mv_backfill_rate_limit(mv_name: str, rate_limit: str) -> str:
        """
        Modify the backfill rate limit for a materialized view.

        Args:
            mv_name: Name of the materialized view
            rate_limit: Either 'default', '0' (to pause), or a numeric value

        Returns:
            Success or error message
        """
        rw = setup_risingwave_connection()
        if not is_valid_identifier(mv_name):
            return f"Error: Invalid materialized view name: '{mv_name}'"
        if rate_limit.lower() != 'default' and not rate_limit.isdigit():
            return "Error: rate_limit must be 'default' or a non-negative integer (use '0' to pause backfill)"

        rate_value = 'default' if rate_limit.lower() == 'default' else rate_limit
        query = f"ALTER MATERIALIZED VIEW {mv_name} SET BACKFILL_RATE_LIMIT = {rate_value}"
        try:
            rw.execute(query)
            return f"Materialized view '{mv_name}' backfill rate limit set to {rate_value}"
        except Exception as e:
            return f"Error altering MV backfill rate limit: {str(e)}"

    @mcp.tool
    def rename_materialized_view(mv_name: str, new_name: str) -> str:
        """
        Rename a materialized view.

        Args:
            mv_name: Current name of the materialized view
            new_name: New name for the materialized view

        Returns:
            Success or error message
        """
        rw = setup_risingwave_connection()
        query = f"ALTER MATERIALIZED VIEW {mv_name} RENAME TO {new_name}"
        try:
            rw.execute(query)
            return f"Materialized view '{mv_name}' renamed to '{new_name}'"
        except Exception as e:
            return f"Error renaming materialized view: {str(e)}"

    @mcp.tool
    def swap_materialized_views(mv_name: str, target_mv_name: str) -> str:
        """
        Atomically swap the names of two materialized views.

        Args:
            mv_name: Name of the first materialized view
            target_mv_name: Name of the second materialized view to swap with

        Returns:
            Success or error message
        """
        rw = setup_risingwave_connection()
        query = f"ALTER MATERIALIZED VIEW {mv_name} SWAP WITH {target_mv_name}"
        try:
            rw.execute(query)
            return f"Materialized views '{mv_name}' and '{target_mv_name}' swapped successfully"
        except Exception as e:
            return f"Error swapping materialized views: {str(e)}"

    # ==================== General DDL Operations ====================

    @mcp.tool
    def create_kafka_table(name: str, columns: str, topic: str) -> str:
        """
        Create a table in RisingWave, storing the streaming data from kafka into RisingWave

        Args:
            name: Name of the table
            columns: Schema/columns of the table (e.g., "id INT, name VARCHAR, timestamp TIMESTAMP")
            topic: Name of kafka topic being sourced from

        Returns:
            Success or error message
        """
        rw = setup_risingwave_connection()
        try:
            query = f"""CREATE TABLE IF NOT EXISTS {name} (
            {columns}
            ) WITH (
                connector='kafka',
                topic='{topic}',
                properties.bootstrap.server='localhost:9092'
            ) FORMAT PLAIN ENCODE JSON;"""
            rw.execute(query)
            return f"Kafka table '{name}' created successfully for topic '{topic}'"
        except Exception as e:
            return f"Error creating table '{name}': {str(e)}"

    @mcp.tool
    def add_table_comment(table_name: str, comment: str) -> str:
        """
        Add a comment/description to a table.

        Args:
            table_name: Name of the table
            comment: Comment text to add

        Returns:
            Success or error message
        """
        rw = setup_risingwave_connection()
        # Escape single quotes in comment
        escaped_comment = comment.replace("'", "''")
        query = f"COMMENT ON TABLE {table_name} IS '{escaped_comment}'"
        try:
            rw.execute(query)
            return f"Comment added to table '{table_name}'"
        except Exception as e:
            return f"Error adding table comment: {str(e)}"

    @mcp.tool
    def add_column_comment(table_name: str, column_name: str, comment: str) -> str:
        """
        Add a comment/description to a column.

        Args:
            table_name: Name of the table containing the column
            column_name: Name of the column
            comment: Comment text to add

        Returns:
            Success or error message
        """
        rw = setup_risingwave_connection()
        # Escape single quotes in comment
        escaped_comment = comment.replace("'", "''")
        query = f"COMMENT ON COLUMN {table_name}.{column_name} IS '{escaped_comment}'"
        try:
            rw.execute(query)
            return f"Comment added to column '{table_name}.{column_name}'"
        except Exception as e:
            return f"Error adding column comment: {str(e)}"
