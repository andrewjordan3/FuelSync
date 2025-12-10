# fuelsync/utils/file_io.py
"""
File input/output utilities for the FuelSync package.

This module handles the low-level details of reading and writing data files,
abstracting storage formats (like Parquet) away from the core business logic.

Design philosophy:
- load() returns None on errors (treat missing data as recoverable)
- save() raises on errors (filesystem issues require explicit handling)
- Config injected at initialization defines the file path and compression
"""

import logging
from pathlib import Path

import pandas as pd
from pyarrow.lib import (
    ArrowInvalid,  # pyright: ignore[reportUnknownVariableType]
    ArrowIOError,  # pyright: ignore[reportUnknownVariableType]
)

from .config_loader import CompressionType, StorageSection

logger: logging.Logger = logging.getLogger(__name__)


class ParquetFileHandler:
    """
    Handles reading and writing the configured Parquet file.

    Operates on a single file path and compression setting defined in config.
    If you need to work with multiple files, instantiate multiple handlers.
    """

    def __init__(self, storage_config: StorageSection) -> None:
        """
        Initialize the Parquet file handler.

        Args:
            storage_config: Storage configuration containing file path and compression.

        Side Effects:
            - Stores reference to config
            - Creates parent directory for parquet_file if it doesn't exist
            - Logs initialization with configured path and compression

        Raises:
            OSError: If parent directory cannot be created.
        """
        self.storage_config: StorageSection = storage_config

        # Ensure parent directory exists at initialization
        # (File itself will be created on first save)
        self.storage_config.parquet_file.parent.mkdir(parents=True, exist_ok=True)

        logger.info(
            'Initialized ParquetFileHandler for %r (compression=%s)',
            self.storage_config.parquet_file,
            self.storage_config.compression,
        )

    def load(self) -> pd.DataFrame | None:
        """
        Load the configured Parquet file into a DataFrame.

        Returns None for missing or corrupt files rather than raising, allowing
        callers to treat this as "no cached data available" without try/except.
        File absence is expected on first run or when forcing fresh data fetch.

        Returns:
            The loaded DataFrame if file exists and is readable, None otherwise.

        Side Effects:
            - Reads from filesystem at configured parquet_file path
            - Logs at DEBUG level on successful load
            - Logs at ERROR level with traceback on read failure

        Raises:
            Does not raise - returns None on all errors (by design).
        """
        file_path: Path = self.storage_config.parquet_file

        if not file_path.exists():
            # No logging: file absence is expected behavior (first run or forced refresh)
            return None

        try:
            logger.debug('Loading cached data from %r', file_path)
            dataframe: pd.DataFrame = pd.read_parquet(file_path)
            record_count: int = len(dataframe)
            logger.debug('Loaded %d records from %r', record_count, file_path)
            return dataframe
        except (OSError, ArrowInvalid, ArrowIOError) as exception:  # pyright: ignore[reportUnknownVariableType]
            logger.exception(
                'Failed to read Parquet file at %r: %r',
                file_path,
                exception,
            )
            return None

    def save(self, dataframe: pd.DataFrame) -> None:
        """
        Save a DataFrame to the configured Parquet file.

        Uses compression setting from config. Raises on errors (unlike load)
        because save failures indicate serious issues (permissions, disk space)
        that the caller must handle.

        Args:
            dataframe: The DataFrame to save.

        Side Effects:
            - Writes to filesystem at configured parquet_file path
            - Logs at INFO level on success
            - Logs at ERROR level with traceback on failure

        Raises:
            OSError: If file cannot be written (permissions, disk space, etc).
            Exception: For other Parquet serialization errors.
        """
        if dataframe.empty:
            logger.warning(
                'Saving empty DataFrame to %r', self.storage_config.parquet_file
            )

        file_path: Path = self.storage_config.parquet_file
        compression: CompressionType = self.storage_config.compression

        try:
            record_count: int = len(dataframe)
            logger.info(
                'Saving %d records to %r (compression=%s)',
                record_count,
                file_path,
                compression,
            )

            dataframe.to_parquet(
                file_path,
                index=False,
                compression=compression,
            )
        except Exception as exception:
            logger.exception('Failed to save data to %r: %r', file_path, exception)
            raise
