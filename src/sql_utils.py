"""SQL utility functions for safe query building."""

import re


def escape_sql_string(value: str) -> str:
    """
    Escape a string value for use in SQL queries.
    Escapes single quotes by doubling them.

    Args:
        value: The string value to escape

    Returns:
        The escaped string (without surrounding quotes)
    """
    if value is None:
        return None
    return value.replace("'", "''")


def is_valid_identifier(name: str) -> bool:
    """
    Check if a string is a valid SQL identifier.
    Valid identifiers contain only alphanumeric characters, underscores,
    and optionally a schema prefix (schema.name).

    Args:
        name: The identifier to validate

    Returns:
        True if valid, False otherwise
    """
    if not name:
        return False
    # Allow schema.name format
    pattern = r'^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?$'
    return bool(re.match(pattern, name))


def validate_identifier(name: str, param_name: str = "identifier") -> str:
    """
    Validate that a string is a safe SQL identifier.

    Args:
        name: The identifier to validate
        param_name: Name of the parameter (for error messages)

    Returns:
        The validated identifier

    Raises:
        ValueError: If the identifier is invalid
    """
    if not is_valid_identifier(name):
        raise ValueError(
            f"Invalid {param_name}: '{name}'. "
            f"Identifiers must contain only alphanumeric characters, underscores, "
            f"and optionally a schema prefix (e.g., 'schema.table')."
        )
    return name
