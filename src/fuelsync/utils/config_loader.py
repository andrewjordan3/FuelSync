# fuelsync/utils/config_loader.py
"""
EFS Configuration Loader with Pydantic Validation

This module loads and validates configuration from a YAML file using Pydantic
for strong type checking and validation. It ensures that all required
configuration values are present and properly formatted before the application
attempts to use them.

Key Design Decisions:
- Pydantic models mirror the exact structure of config.yaml for maintainability
- Validation occurs at load time to fail fast if config is malformed
- Sensitive values (passwords) use SecretStr to prevent accidental logging
- Log levels support both string names ("DEBUG") and numeric values (10)
- File logging is optional; console logging is always enabled
"""

import logging
from datetime import date
from pathlib import Path
from typing import Any, Literal, cast

import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    SecretStr,
    ValidationError,
    field_validator,
    model_validator,
)

# Set up a logger for this module
logger: logging.Logger = logging.getLogger(__name__)

# =============================================================================
# Type Aliases for Clarity
# =============================================================================

# Valid logging level names recognized by Python's logging module
LogLevelName = Literal['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']

# Valid compression algorithms supported by pandas.to_parquet()
CompressionType = Literal['snappy', 'gzip', 'brotli', 'lz4', 'zstd'] | None

# =============================================================================
# Configuration Models (Schema)
# =============================================================================
# These models mirror the structure of config.yaml exactly.
# Each field includes validation rules to catch configuration errors early.


class EfsSection(BaseModel):
    """
    Schema for the 'efs' section of config.yaml.

    Validates API endpoint and authentication credentials for the EFS SOAP service.

    Security Notes:
    - SecretStr prevents passwords from appearing in logs or string representations
    - In production, consider loading credentials from environment variables or
      a secrets manager (AWS Secrets Manager, Azure Key Vault, etc.)
    """

    model_config = ConfigDict(extra='forbid')
    endpoint_url: HttpUrl = Field(
        ...,
        description='Full URL to the EFS SOAP API endpoint. Must use HTTPS in production.',
    )

    username: str = Field(
        ...,
        min_length=1,
        description='EFS API username. Must not be empty.',
    )

    password: SecretStr = Field(
        ...,
        description='EFS API password. Stored as SecretStr to prevent accidental exposure.',
    )

    @field_validator('password')
    @classmethod
    def password_not_empty(cls, v: SecretStr) -> SecretStr:
        """
        Ensure the password is not an empty string.

        Pydantic's SecretStr wraps the actual string value, so we need to
        access .get_secret_value() to inspect the actual content.
        """
        if not v.get_secret_value():
            raise ValueError('Password cannot be empty')
        return v


class ClientSection(BaseModel):
    """
    Schema for the 'client' section of config.yaml.

    Controls HTTP client behavior including timeouts, SSL verification, and retry logic.
    These settings directly impact reliability and performance when communicating with
    the EFS SOAP API.
    """

    model_config = ConfigDict(extra='forbid')
    request_timeout: tuple[float, float] = Field(
        default=(10.0, 30.0),
        description='HTTP timeouts in seconds: [connect_timeout, read_timeout]. '
        'Connect timeout is how long to wait for the server to accept the connection. '
        'Read timeout is how long to wait for the server to send response data.',
    )

    verify_ssl: bool = Field(
        default=True,
        description='Whether to verify SSL certificates. Should ALWAYS be True in production. '
        'Only set to False when debugging local SSL interception (e.g., ZScaler).',
    )

    max_retries: int = Field(
        default=3,
        ge=0,
        description='Maximum number of retry attempts for failed HTTP requests.',
    )

    retry_backoff_factor: float = Field(
        default=2.0,
        gt=0.0,
        description='Exponential backoff multiplier. Wait time = backoff_factor ^ attempt.',
    )

    @field_validator('request_timeout')
    @classmethod
    def validate_timeout_values(cls, v: tuple[float, float]) -> tuple[float, float]:
        """
        Validate that timeout values are positive and connect timeout is less than read timeout.

        The connect timeout should always be shorter than the read timeout because:
        1. Connection establishment should be quick (typically < 5 seconds)
        2. Reading response data can take much longer, especially for large datasets
        """
        connect_timeout: float
        read_timeout: float
        connect_timeout, read_timeout = v

        if connect_timeout <= 0:
            raise ValueError(f'Connect timeout must be positive, got {connect_timeout}')

        if read_timeout <= 0:
            raise ValueError(f'Read timeout must be positive, got {read_timeout}')

        if connect_timeout > read_timeout:
            raise ValueError(
                f'Connect timeout ({connect_timeout}s) should not exceed '
                f'read timeout ({read_timeout}s)'
            )

        return v


class PipelineSection(BaseModel):
    """
    Schema for the 'pipeline' section of config.yaml.

    Defines the operational parameters for the incremental data synchronization pipeline.
    These settings control how the pipeline chunks time windows, handles late-arriving
    data, and manages API rate limits.
    """

    model_config = ConfigDict(extra='forbid')
    default_start_date: str = Field(
        ...,
        description='ISO format date string (YYYY-MM-DD) representing the earliest date '
        'to query if no local history exists. This should be the inception date '
        'of the fuel card program.',
    )

    batch_size_days: int = Field(
        default=1,
        ge=1,
        description='Number of days to include in each API request batch. '
        'Keep this value small (1-7 days) to prevent SOAP response timeouts '
        'and excessive memory consumption during XML parsing.',
    )

    lookback_days: int = Field(
        default=7,
        ge=0,
        description='Number of days to overlap with existing data when resuming. '
        'This captures transactions that vendors posted late. For example, '
        'a transaction from Monday might not appear in the API until Thursday.',
    )

    request_delay_seconds: float = Field(
        default=0.5,
        ge=0.0,
        description='Time to wait between API requests (rate limiting). '
        'Helps prevent overwhelming the EFS API and being throttled or blocked.',
    )

    @field_validator('default_start_date')
    @classmethod
    def validate_date_format(cls, v: str) -> str:
        """
        Validate that the date string is in ISO format (YYYY-MM-DD).

        We could convert this to a datetime.date object here, but keeping it as a
        string allows the pipeline to handle the conversion with its own timezone
        logic (UTC, local, etc.).
        """
        try:
            date.fromisoformat(v)
        except ValueError as e:
            raise ValueError(
                f'default_start_date must be in YYYY-MM-DD format, got "{v}"'
            ) from e

        return v


class StorageSection(BaseModel):
    """
    Schema for the 'storage' section of config.yaml.

    Configures the local Parquet file that serves as the persistent data store
    for synchronized transactions. Parquet was chosen for:
    - Excellent compression (smaller file sizes)
    - Fast columnar read performance (for analytics)
    - Native datetime support (no string conversion needed)
    """

    model_config = ConfigDict(extra='forbid')
    parquet_file: Path = Field(
        ...,
        description='Local filesystem path where transaction data is stored. '
        'Can be absolute or relative. If relative, it resolves from the '
        'current working directory where the pipeline is executed.',
    )

    compression: CompressionType = Field(
        default='snappy',
        description='Compression algorithm for the Parquet file. '
        'Snappy provides good compression with fast read/write speeds. '
        'Gzip offers better compression but is slower. '
        'Brotli offers the best compression but is the slowest.',
    )


class LoggingSection(BaseModel):
    """
    Schema for the 'logging' section of config.yaml.

    Configures application logging behavior. Supports dual-destination logging:
    1. Console (stdout/stderr) - Always enabled, typically INFO level
    2. File (optional) - Detailed logs for troubleshooting, typically DEBUG level

    Log Levels (from least to most verbose):
    - CRITICAL (50): Severe errors causing application shutdown
    - ERROR (40): Serious problems that prevent specific operations
    - WARNING (30): Warning messages for potentially problematic situations
    - INFO (20): General informational messages about program execution
    - DEBUG (10): Detailed diagnostic information useful for debugging

    Location Convention:
    - For user-facing tools: Save logs in current working directory or project root
    - For system services: Use /var/log or /var/log/<app_name>/
    - For development: Often in project root or a dedicated logs/ folder

    This implementation uses relative paths by default, which resolve from wherever
    the user executes the pipeline command. If you want logs in a specific location
    regardless of execution context, use an absolute path.
    """

    model_config = ConfigDict(extra='forbid')
    console_level: LogLevelName | int = Field(
        default='INFO',
        description='Logging level for console output. Accepts either a string name '
        '(DEBUG, INFO, WARNING, ERROR, CRITICAL) or an integer (10, 20, 30, 40, 50).',
    )

    file_path: Path | None = Field(
        default=None,
        description='Optional path to a log file. If provided, logs will be written to '
        'this file in addition to console output. If None, file logging is disabled.',
    )

    file_level: LogLevelName | int | None = Field(
        default=None,
        description='Logging level for file output. Only relevant if file_path is provided. '
        'Accepts either a string name or an integer.',
    )

    @field_validator('console_level', 'file_level')
    @classmethod
    def validate_log_level(
        cls, v: LogLevelName | int | None
    ) -> LogLevelName | int | None:
        """
        Validate that log levels are either valid string names or valid numeric values.

        Python's logging module uses these numeric values internally:
        - DEBUG: 10
        - INFO: 20
        - WARNING: 30
        - ERROR: 40
        - CRITICAL: 50

        We accept both formats and validate them here.
        """
        if v is None:
            return v

        # If it's a string, Pydantic's Literal type already validated it's one of the allowed names
        if isinstance(v, str):
            return v

        # Not None or str, so must be integer, validate it's a standard logging level
        valid_levels: set[int] = {10, 20, 30, 40, 50}
        if v not in valid_levels:
            raise ValueError(
                f'Numeric log level must be one of {valid_levels}, got {v}'
            )
        return v

    @model_validator(mode='after')
    def validate_file_logging_consistency(self) -> 'LoggingSection':
        """
        Ensure that if file_path is provided, file_level is also provided, and vice versa.

        This prevents misconfiguration where someone enables file logging but doesn't
        specify what level to log at (or vice versa).

        Model validators run after all field validators, so we can safely access
        multiple fields at once.
        """
        has_file_path: bool = self.file_path is not None
        has_file_level: bool = self.file_level is not None

        if has_file_path and not has_file_level:
            # Default to DEBUG if path is provided but level is missing
            self.file_level = 'DEBUG'
            logger.warning(
                'file_path provided without file_level. Defaulting to DEBUG for file logging.'
            )

        if has_file_level and not has_file_path:
            raise ValueError(
                'file_level is specified but file_path is missing. '
                'Both must be provided to enable file logging.'
            )

        return self

    def get_console_level_int(self) -> int:
        """
        Convert console_level to the integer value used by Python's logging module.

        Returns:
            The integer logging level (10, 20, 30, 40, or 50)
        """
        if isinstance(self.console_level, int):
            return self.console_level
        return cast(int, getattr(logging, self.console_level))

    def get_file_level_int(self) -> int | None:
        """
        Convert file_level to the integer value used by Python's logging module.

        Returns:
            The integer logging level, or None if file logging is disabled
        """
        if self.file_level is None:
            return None
        if isinstance(self.file_level, int):
            return self.file_level
        return cast(int, getattr(logging, self.file_level))


class FuelSyncConfig(BaseModel):
    """
    Root configuration model.

    Aggregates all configuration sections into a single, validated object.
    This is the primary interface for accessing configuration throughout the application.

    Usage:
        config = load_config()
        endpoint = config.efs.endpoint_url
        timeout = config.client.request_timeout
        log_level = config.logging.get_console_level_int()
    """

    model_config = ConfigDict(extra='forbid')
    efs: EfsSection
    client: ClientSection
    pipeline: PipelineSection
    storage: StorageSection
    logging: LoggingSection


# =============================================================================
# Loader Logic
# =============================================================================


def _get_default_config_path() -> Path:
    """
    Resolve the absolute path to the default config.yaml file.

    Strategy: Use __file__ to anchor to this module's location, then navigate
    the known directory structure to reach the config file.

    Directory Structure:
        FuelSync/
        └── src/
            └── fuelsync/
                ├── config/
                │   └── config.yaml       <-- Target file
                └── utils/
                    └── config_loader.py  <-- This file

    Why use __file__ instead of hardcoding?
    ========================================
    Using __file__ makes this code robust across different environments:

    1. Development: Works when running from project root or any subdirectory
    2. Installed Package: Works when installed via pip (editable or regular)
    3. Packaged Distribution: Works when bundled as a wheel or source distribution
    4. Testing: Works regardless of where tests are executed from

    The __file__ approach eliminates all these issues by calculating the path
    dynamically based on where the Python module is actually located at runtime.

    Returns:
        Absolute path to config.yaml
    """
    # Get the absolute path to this file (config_loader.py)
    current_file: Path = Path(__file__).resolve()

    # Navigate up two levels: utils/ -> fuelsync/
    fuelsync_package_root: Path = current_file.parent.parent

    # Navigate down into config/
    config_file_path: Path = fuelsync_package_root / 'config' / 'config.yaml'

    return config_file_path


def load_config(config_path: Path | str | None = None) -> FuelSyncConfig:
    """
    Load, parse, and validate the configuration file.

    This is the primary entry point for configuration loading. It handles the entire
    pipeline: file resolution -> YAML parsing -> Pydantic validation -> typed object.

    The function fails fast with descriptive errors if:
    - The config file doesn't exist
    - The YAML is malformed
    - Any required fields are missing
    - Any fields have invalid types or values

    Args:
        config_path: Optional explicit path to a config file. This is useful for:
                     - Testing with mock configurations
                     - Supporting multiple environments (dev, staging, prod)
                     - Allowing users to override the default config location

                     If None, uses the default location determined by
                     _get_default_config_path().

    Returns:
        A fully validated FuelSyncConfig object with strong typing and all fields
        guaranteed to be present and valid.

    Raises:
        FileNotFoundError: The specified config file does not exist on disk.

        yaml.YAMLError: The file exists but contains invalid YAML syntax.
                       This could be caused by:
                       - Incorrect indentation
                       - Missing colons
                       - Unquoted special characters

        ValidationError: The YAML is valid but the configuration is invalid.
                        Pydantic will provide detailed information about:
                        - Which field(s) failed validation
                        - What the expected type/format was
                        - What value was actually provided

    Example:
        # Use default config location
        config = load_config()

        # Use explicit config for testing
        test_config = load_config('/tmp/test_config.yaml')

        # Access configuration values with full type safety
        api_url = config.efs.endpoint_url
        timeout = config.client.request_timeout
    """
    # 1. Resolve Configuration File Path
    # -----------------------------------
    # Determine which config file to load. If the user provided a specific path,
    # use that. Otherwise, fall back to the default project location.
    if config_path:
        path_obj: Path = Path(config_path)
    else:
        path_obj = _get_default_config_path()

    logger.debug(f'Resolving configuration from: {path_obj}')

    # 2. Verify File Exists
    # ----------------------
    # Fail immediately if the file is missing rather than attempting to parse.
    # This provides a clearer error message than the generic YAML parsing error.
    if not path_obj.exists():
        error_msg: str = f'Configuration file not found at: {path_obj}'
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)

    # 3. Parse YAML into Python Dictionary
    # -------------------------------------
    # Read the file and parse it as YAML. At this stage, we have no type safety—
    # just a nested dictionary of raw Python objects (strings, ints, lists, etc.).
    try:
        with open(path_obj, encoding='utf-8') as f:
            raw_config: dict[str, Any] = yaml.safe_load(f)
    except yaml.YAMLError as e:
        logger.error(f'Failed to parse YAML config file: {e}')
        raise

    # 4. Validate with Pydantic
    # --------------------------
    # This is where the magic happens. Pydantic takes the raw dictionary and:
    # - Validates each field exists
    # - Validates types match what we declared in the models
    # - Runs all custom validators (like password_not_empty)
    # - Converts basic types to more specific ones (str -> Path, str -> HttpUrl)
    # - Applies default values where fields are missing
    #
    # If validation fails, Pydantic raises ValidationError with detailed information
    # about what went wrong and where.
    try:
        config = FuelSyncConfig(**raw_config)
        logger.debug('Configuration validated successfully.')
        return config
    except ValidationError as e:
        logger.error(f'Configuration validation failed: {e}')
        raise
