import os
from risingwave import RisingWave, RisingWaveConnOptions


# Cached connection instance
_rw_instance = None


def _get_connection_str():
    """Build connection string from environment variables."""
    connection_str = os.getenv("RISINGWAVE_CONNECTION_STR")
    if connection_str:
        return connection_str

    risingwave_host = os.getenv("RISINGWAVE_HOST")
    risingwave_user = os.getenv("RISINGWAVE_USER")
    risingwave_password = os.getenv("RISINGWAVE_PASSWORD")
    risingwave_port = os.getenv("RISINGWAVE_PORT", "4566")
    risingwave_database = os.getenv("RISINGWAVE_DATABASE", "dev")
    risingwave_sslmode = os.getenv("RISINGWAVE_SSLMODE", "require")
    risingwave_timeout = os.getenv("RISINGWAVE_TIMEOUT", "30")

    if not risingwave_host or not risingwave_user or not risingwave_password:
        raise ValueError(
            "RISINGWAVE_HOST, RISINGWAVE_USER, and RISINGWAVE_PASSWORD must be set in environment variables")

    return (f"postgresql://{risingwave_user}:{risingwave_password}@"
            f"{risingwave_host}:{risingwave_port}/{risingwave_database}"
            f"?sslmode={risingwave_sslmode}&connect_timeout={risingwave_timeout}")


def setup_risingwave_connection() -> RisingWave:
    """Set up a connection to the RisingWave database, reusing existing connection if possible."""
    global _rw_instance
    if _rw_instance is not None:
        return _rw_instance

    try:
        _rw_instance = RisingWave(
            RisingWaveConnOptions(_get_connection_str())
        )
        return _rw_instance
    except Exception as e:
        _rw_instance = None
        raise ValueError(f"Failed to connect to RisingWave: {str(e)}")
