# fuelsync/utils/logger.py
"""
Logging configuration for the fuelsync package.

Provides centralized logging setup to ensure consistent log formatting
and output across all modules in the package.
"""

import logging
from pathlib import Path
from sys import stdout


def setup_logger(
    logging_level: int = logging.INFO,
    log_file_path: Path | None = None,
) -> logging.Logger:
    """
    Set up logging for the fuelsync package.

    This function configures the package-level logger so that all modules
    inherit the same log level and handler configuration. This ensures
    consistent logging throughout the package.

    The function is idempotent - calling it multiple times will update
    the existing configuration rather than adding duplicate handlers.

    Args:
        logging_level: The logging level to use (e.g., logging.DEBUG,
                      logging.INFO, logging.WARNING). Defaults to INFO.
        log_file_path: Optional path to a log file. If provided, logs will
                      be written to this file. If None, logs will be written
                      to stdout (console).

    Returns:
        Logger instance for the calling module (via __name__).

    Example:
        >>> # Log to console at INFO level (default)
        >>> logger = setup_logger()
        >>>
        >>> # Log to console at DEBUG level
        >>> logger = setup_logger(logging_level=logging.DEBUG)
        >>>
        >>> # Log to file at INFO level
        >>> logger = setup_logger(log_file_path=Path('fuelsync.log'))
        >>>
        >>> # Log to file at DEBUG level
        >>> logger = setup_logger(
        ...     logging_level=logging.DEBUG,
        ...     log_file_path=Path('fuelsync_debug.log')
        ... )
    """
    # Get the package-level logger (parent of all module loggers)
    package_logger: logging.Logger = logging.getLogger('fuelsync')

    # Set the log level on the package logger
    # This will apply to all child loggers (e.g., fuelsync.efs_client, fuelsync.utils.login)
    package_logger.setLevel(logging_level)

    # Define consistent log format for all handlers
    log_format: logging.Formatter = logging.Formatter(
        fmt='%(asctime)s - %(levelname)-8s - [%(name)s] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    # Only add a handler if one doesn't already exist
    # This prevents duplicate log messages when setup_logger is called multiple times
    if not package_logger.handlers:
        # Create appropriate handler based on whether a log file was specified
        if log_file_path is not None:
            # Ensure parent directory exists
            log_file_path.parent.mkdir(parents=True, exist_ok=True)

            handler: logging.Handler = logging.FileHandler(
                filename=str(log_file_path),
                mode='a',  # Append mode
                encoding='utf-8',
            )
            logging.info(f'Logging to file: {log_file_path}')
        else:
            # Log to stdout (console)
            handler: logging.Handler = logging.StreamHandler(stdout)

        # Apply formatter to handler
        handler.setFormatter(log_format)
        handler.setLevel(logging_level)

        # Add handler to package logger
        package_logger.addHandler(handler)

    else:
        # Handler already exists - update its level to match new configuration
        # This handles the case where setup_logger is called multiple times
        # with different log levels
        for existing_handler in package_logger.handlers:
            existing_handler.setLevel(logging_level)

    # Return a module-specific logger for the caller
    # This uses __name__ from the calling module, not from this logger.py module
    return logging.getLogger(__name__)
