from fastmcp import FastMCP
from risingwave import OutputFormat
from connection import setup_risingwave_connection
from sql_utils import escape_sql_string


def register_user_tools(mcp: FastMCP):
    """Register all user and access control related MCP tools"""

    @mcp.tool
    def list_users() -> str:
        """
        List all users in the RisingWave database.

        Returns:
            List of users with their properties
        """
        rw = setup_risingwave_connection()
        query = """
        SELECT usename as username,
               usesysid as user_id,
               usecreatedb as can_create_db,
               usesuper as is_superuser
        FROM pg_user
        ORDER BY usename
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error listing users: {str(e)}"

    @mcp.tool
    def get_user_privileges(username: str) -> str:
        """
        Get all privileges granted to a specific user.
        Uses RisingWave's system catalog rw_users.

        Args:
            username: Name of the user to check privileges for

        Returns:
            User details including privileges
        """
        rw = setup_risingwave_connection()
        safe_username = escape_sql_string(username)
        query = f"""
        SELECT id,
               name,
               is_super as is_superuser,
               create_db as can_create_db,
               create_user as can_create_user,
               can_login
        FROM rw_catalog.rw_users
        WHERE name = '{safe_username}'
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting user privileges: {str(e)}"

    @mcp.tool
    def get_schema_privileges(username: str) -> str:
        """
        Get schema-level information for a specific user.
        Shows schemas owned by the user.

        Args:
            username: Name of the user to check privileges for

        Returns:
            List of schemas owned by the user
        """
        rw = setup_risingwave_connection()
        safe_username = escape_sql_string(username)
        query = f"""
        SELECT s.id as schema_id,
               s.name as schema_name,
               u.name as owner
        FROM rw_catalog.rw_schemas s
        JOIN rw_catalog.rw_users u ON s.owner = u.id
        WHERE u.name = '{safe_username}'
        ORDER BY s.name
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting schema privileges: {str(e)}"

    @mcp.tool
    def get_database_privileges(username: str) -> str:
        """
        Get database-level information for a specific user.
        Shows databases owned by the user.

        Args:
            username: Name of the user to check privileges for

        Returns:
            List of databases owned by the user
        """
        rw = setup_risingwave_connection()
        safe_username = escape_sql_string(username)
        query = f"""
        SELECT d.id as database_id,
               d.name as database_name,
               u.name as owner
        FROM rw_catalog.rw_databases d
        JOIN rw_catalog.rw_users u ON d.owner = u.id
        WHERE u.name = '{safe_username}'
        ORDER BY d.name
        """
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error getting database privileges: {str(e)}"
