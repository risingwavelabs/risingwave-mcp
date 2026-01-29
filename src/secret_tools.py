from fastmcp import FastMCP
from risingwave import OutputFormat
from connection import setup_risingwave_connection


def register_secret_tools(mcp: FastMCP):
    """Register all secret-related MCP tools"""

    @mcp.tool
    def list_secrets(schema_name: str = None) -> str:
        """
        List all secrets in the database.
        Note: Secret values are never displayed for security.

        Args:
            schema_name: Optional schema name to filter secrets. If not specified,
                        lists secrets from all schemas in the current search_path.

        Returns:
            List of secret names (values are hidden)
        """
        rw = setup_risingwave_connection()
        query = f"SHOW SECRETS FROM {schema_name}" if schema_name else "SHOW SECRETS"
        try:
            result = rw.fetch(query, format=OutputFormat.DATAFRAME)
            return result.to_json()
        except Exception as e:
            return f"Error listing secrets: {str(e)}"

    @mcp.tool
    def drop_secret(secret_name: str) -> str:
        """
        Drop a secret from the database.

        Args:
            secret_name: Name of the secret to drop

        Returns:
            Success or error message
        """
        rw = setup_risingwave_connection()
        query = f"DROP SECRET {secret_name}"
        try:
            rw.execute(query)
            return f"Secret '{secret_name}' dropped successfully"
        except Exception as e:
            return f"Error dropping secret: {str(e)}"
