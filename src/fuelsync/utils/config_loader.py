# fuelsync/utils/config_loader.py
"""
EFS Configuration Loader with Pydantic Validation

This module loads and validates configuration from YAML files using Pydantic
for strong type checking and validation. It ensures that all required
configuration values are present and properly formatted before the application
attempts to use them.
"""

import logging
from pathlib import Path
from typing import Any, cast

import yaml
from pydantic import BaseModel, Field, HttpUrl, SecretStr, ValidationError
from pydantic_core import InitErrorDetails

# Set up a logger for this module
logger: logging.Logger = logging.getLogger(__name__)


class EfsConfig(BaseModel):
    """
    Pydantic model for validating EFS API configuration.

    This model defines the expected structure and types for the configuration
    file. Pydantic automatically validates all values against the specified
    types and constraints when the configuration is loaded.

    Field Aliases:
        The 'alias' parameter maps YAML keys (typically UPPER_CASE by convention)
        to Python-friendly snake_case attribute names. This allows the config
        file to follow YAML conventions while the code follows Python conventions.

    Attributes:
        efs_endpoint_url: The SOAP API endpoint URL. Must be a valid HTTP/HTTPS URL.
        efs_username: The username for API authentication.
        efs_password: The password for API authentication. Stored as SecretStr
                     to prevent accidental logging or printing of sensitive data.
        request_timeout: Timeout for API requests in seconds. Can be either:
                        - A single number (applied to both connect and read)
                        - A tuple of (connect_timeout, read_timeout)
        verify_ssl_certificate: Whether to verify SSL certificates. Should be
                               True in production, but can be False for testing
                               against self-signed certificates.
    """

    # Use HttpUrl for strong URL validation (ensures proper format and scheme)
    efs_endpoint_url: HttpUrl = Field(..., alias='EFS_ENDPOINT_URL')

    # Username for API authentication
    efs_username: str = Field(..., alias='EFS_USERNAME')

    # Use SecretStr to ensure the password isn't accidentally logged
    # or printed in plain text. Access the actual value with .get_secret_value()
    efs_password: SecretStr = Field(..., alias='EFS_PASSWORD')

    # Timeout can be a single value or a tuple of (connect, read) timeouts.
    # Using int | tuple allows flexibility for different timeout strategies.
    request_timeout: int | tuple[int, int] = Field(..., alias='REQUEST_TIMEOUT')

    # SSL certificate verification flag
    verify_ssl_certificate: bool = Field(..., alias='VERIFY_SSL_CERTIFICATE')

    class Config:
        """
        Pydantic model configuration.

        populate_by_name allows the model to accept both the alias names
        (e.g., 'EFS_ENDPOINT_URL') and the attribute names (e.g., 'efs_endpoint_url').
        This provides flexibility in how the configuration is constructed.
        """

        populate_by_name = True


def _find_project_root() -> Path:
    """
    Find the project root by walking up the directory tree.

    Starting from this file's location, walk up the directory tree until
    we find a directory named 'fuelsync'. This is the project root where
    the config directory should be located.

    This approach is more robust than using relative paths because it works
    regardless of:
    - The current working directory
    - Where the package is installed
    - How the module is imported

    Returns:
        The path to the 'fuelsync' project root directory.

    Raises:
        FileNotFoundError: If no 'fuelsync' directory is found in the
                          directory tree (shouldn't happen in normal use).
    """
    # Start from this file's directory
    current_path: Path = Path(__file__).parent.resolve()

    logger.debug(f'Searching for project root starting from: {current_path}')

    # Walk up the directory tree looking for 'fuelsync'
    for parent_dir in [current_path, *current_path.parents]:
        logger.debug(f'Checking directory: {parent_dir}')

        if parent_dir.name == 'fuelsync':
            logger.debug(f'Found project root at: {parent_dir}')
            return parent_dir

    # If we get here, we couldn't find the project root
    error_message: str = (
        f'Could not find "fuelsync" directory in path hierarchy starting '
        f'from {current_path}. This may indicate an installation problem.'
    )
    logger.error(error_message)
    raise FileNotFoundError(error_message)


def _get_default_config_path() -> Path:
    """
    Get the default configuration file path.

    This function locates the config file by:
    1. Finding the project root (the 'fuelsync' directory)
    2. Appending the config subdirectory and filename

    Returns:
        The path to the default config.yaml file.

    Raises:
        FileNotFoundError: If the project root cannot be found.
    """
    project_root: Path = _find_project_root()
    config_path: Path = project_root / 'config' / 'config.yaml'

    logger.debug(f'Default config path resolved to: {config_path}')

    return config_path


def load_config(config_path: Path | None = None) -> EfsConfig:
    """
    Load, parse, and validate the YAML configuration file.

    This function reads a YAML configuration file, validates its structure
    and types using the EfsConfig Pydantic model, and returns a validated
    configuration object. All validation happens automatically through Pydantic.

    If no config path is provided, the function automatically locates the
    config file by walking up the directory tree to find the project root.

    Args:
        config_path: Path to the configuration YAML file. If None, automatically
                    finds and uses 'config/config.yaml' relative to the project
                    root (the 'fuelsync' directory).

    Returns:
        A validated EfsConfig instance with all required configuration values.

    Raises:
        FileNotFoundError: If the configuration file doesn't exist at the
                          specified or default path, or if the project root
                          cannot be found.
        yaml.YAMLError: If the YAML file is malformed or cannot be parsed.
        ValidationError: If the configuration values fail Pydantic validation
                        (missing required fields, wrong types, invalid URLs, etc.).
        ValueError: If the configuration file is empty.

    Example:
        >>> # Load from default location (automatically found)
        >>> config = load_config()
        >>> print(config.efs_endpoint_url)
        >>>
        >>> # Load from custom location
        >>> config = load_config(Path('/etc/fuelsync/config.yaml'))
    """
    # ========================================================================
    # STEP 1: Determine Configuration Path
    # ========================================================================
    # If no path is provided, use intelligent path discovery to find the
    # config file relative to the project root
    if config_path is None:
        config_path = _get_default_config_path()

    logger.info(f'Loading configuration from: {config_path}')

    # ========================================================================
    # STEP 2: Read and Parse YAML File
    # ========================================================================
    try:
        # Verify the file exists before attempting to open it
        if not config_path.exists():
            error_message: str = f'Configuration file not found at: {config_path}'
            logger.error(error_message)
            raise FileNotFoundError(error_message)

        # Open and read the YAML file
        with open(config_path, encoding='utf-8') as config_file:
            logger.debug(f'Reading YAML from: {config_path}')
            config_data: dict[str, Any] | None = yaml.safe_load(config_file)

            # Check if the file was empty or contained only comments
            if config_data is None:
                error_message: str = f'Configuration file is empty or contains no valid data: {config_path}'
                logger.error(error_message)
                raise ValueError(error_message)

        logger.debug('YAML parsed successfully, validating structure...')

    except yaml.YAMLError as yaml_error:
        error_message: str = f'Failed to parse YAML file: {yaml_error}'
        logger.error(error_message)
        raise yaml.YAMLError(error_message) from yaml_error

    # ========================================================================
    # STEP 3: Validate Configuration with Pydantic
    # ========================================================================
    # Pydantic will automatically:
    # - Check that all required fields are present
    # - Validate types (URL format, boolean values, etc.)
    # - Map YAML keys to Python attributes using the aliases
    # - Protect sensitive data (passwords become SecretStr)
    try:
        logger.debug('Validating configuration with Pydantic model')
        validated_config: EfsConfig = EfsConfig(**config_data)
        logger.info('Configuration loaded and validated successfully')
        return validated_config

    except ValidationError as validation_error:
        # Pydantic provides detailed error messages showing exactly what failed
        error_message: str = (
            f'Configuration validation failed. Check that all required fields '
            f'are present and have valid values.\n'
            f'Details: {validation_error}'
        )
        logger.error(error_message)
        line_errors: list[InitErrorDetails] = cast(
            list[InitErrorDetails], validation_error.errors()
        )
        raise ValidationError.from_exception_data(
            title='EfsConfig',
            line_errors=line_errors,
        ) from validation_error
