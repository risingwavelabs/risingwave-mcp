from fastmcp import FastMCP
from connection import setup_risingwave_connection


def register_ddl_tools(mcp: FastMCP):
    """Register all DDL-related MCP tools"""

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
