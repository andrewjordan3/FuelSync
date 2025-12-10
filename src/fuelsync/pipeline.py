# fuelsync/pipeline.py
"""
Data pipeline for incremental synchronization of EFS transactions.

This module handles the logic for downloading transaction data over long periods,
managing incremental state via Parquet files, and deduplicating records.
"""

import logging
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pandas as pd
from requests import Response

from fuelsync.efs_client import EfsClient
from fuelsync.models import GetMCTransExtLocV2Request
from fuelsync.response_models import GetMCTransExtLocV2Response
from fuelsync.utils import FuelSyncConfig, ParquetFileHandler, load_config, setup_logger

# Set up module-level logger
logger: logging.Logger = logging.getLogger(__name__)


class FuelPipeline:
    """
    Manages the download, processing, and storage of EFS transaction data.

    This pipeline implements a robust incremental load strategy designed for
    long-running, scheduled execution (e.g., cron or systemd).

    Strategy:
    1.  **State Detection**: Checks the target Parquet file to find the timestamp
        of the most recent transaction downloaded.
    2.  **Time Window Calculation**: Determines the start date for the new run.
        - If data exists: Start = Last Timestamp - Lookback Buffer (defaults to 7 days).
        - If new: Start = Default Start Date (e.g., June 2025).
    3.  **Batch Processing**: Downloads data in small daily chunks. This prevents
        SOAP API timeouts and keeps memory usage low during XML parsing.
    4.  **Consolidation & Deduplication**: Merges new records with existing data,
        identifying duplicates via unique composite keys (Transaction ID + Line Number)
        and keeping the latest version of the record.
    5.  **Atomic-like Save**: writes the clean, deduplicated dataset back to Parquet.
        - Supports a 'dry run' mode to skip this final write step for testing.

    Attributes:
        parquet_file_path (Path): The absolute path to the local Parquet file where
            transaction data is persisted.
        client_config_path (Path | None): Optional path to a specific YAML config
            file. If None, the default config location is used.
        efs_client (EfsClient): Persistent authenticated client instance that is
            reused across all batches to avoid unnecessary login/logout cycles.
    """

    def __init__(
        self,
        client_config_path: Path | str | None = None,
    ) -> None:
        """
        Initialize the pipeline configuration.

        Args:
            client_config_path: Path to the fuelsync config.yaml. If not provided,
                                the client looks in the default project location.
        """

        # Load configuration immediately upon initialization
        # This ensures we fail fast if config is missing or invalid, and
        # prevents reloading it for every batch.
        self.config: FuelSyncConfig = load_config(client_config_path)

        # Configure the package-level logger (all module loggers inherit from it)
        setup_logger(config=self.config)
        self.parquet_file_path: Path = Path(self.config.storage.parquet_file)

        self.file_handler = ParquetFileHandler(self.config.storage)
        self.dataframe: pd.DataFrame | None = self.file_handler.load()

        # Initial instantiation of efs client
        self.efs_client = EfsClient(config=self.config)

        # Run State / Configuration
        # These are populated when run_synchronization is called
        self.run_start_date: datetime | None = None
        self.run_end_date: datetime | None = None
        self.batch_size_days: int = 1
        self.request_delay_seconds: float = 0.0

    def _recreate_client(self) -> None:
        """
        Recreate the EFS client with a fresh authentication session.

        This method is called when the existing session has failed or expired.
        It attempts to logout the old session (if possible), then creates a
        new authenticated client instance.

        Raises:
            Exception: If client recreation fails, indicating a fatal issue
                      that should abort the pipeline.
        """
        logger.warning('Recreating EFS client with fresh session')

        # Use our cleanup method to logout
        self.cleanup()

        # Create new authenticated client
        self.efs_client = EfsClient(config=self.config)
        logger.info('New EFS client session established')

    def _retrieve_latest_transaction_timestamp(self) -> datetime | None:
        """
        Inspect the existing Parquet file to find the most recent transaction date.

        Returns:
            datetime | None: The maximum transaction timestamp found in the file,
                             converted to a timezone-aware (UTC) Python datetime object.
                             Returns None if the file does not exist or is empty.
        """
        # 1. Check if we have data in memory (loaded via __init__)
        if self.dataframe is None or self.dataframe.empty:
            logger.debug(
                'No historical data currently loaded. Treating as initial run.'
            )
            return None

        try:
            # 2. Validate column existence
            if 'transaction_date' not in self.dataframe.columns:
                logger.warning(
                    'Historical data exists but is missing "transaction_date" column.'
                )
                return None

            # 3. Extract maximum date
            # Pandas max() on a datetime column returns a Timestamp
            max_timestamp: pd.Timestamp = self.dataframe['transaction_date'].max()

            if pd.isna(max_timestamp):
                logger.debug('Max date in history is NaT (Not a Time).')
                return None

            # 4. Convert to UTC datetime
            # The EFS API works in UTC, so we standardize on that.
            latest_datetime: datetime = pd.to_datetime(max_timestamp).to_pydatetime()
            if latest_datetime.tzinfo is None:
                latest_datetime = latest_datetime.replace(tzinfo=UTC)

            logger.debug('Latest transaction timestamp found: %s', latest_datetime)
            return latest_datetime

        except Exception as e:
            logger.warning(
                'Failed to read watermark from historical data: %r. Pipeline '
                'will default to initial start date.',
                e,
            )
            return None

    def _determine_start_timestamp(
        self,
        explicit_start_date: datetime | None,
    ) -> datetime:
        """
        Determine the effective start date for the synchronization window.

        This helper method handles the logic of choosing between a user-provided date,
        calculating a resume date based on existing data, or falling back to a default
        inception date.

        Args:
            explicit_start_date: Optional user override for start date.

        Returns:
            datetime: The calculated, timezone-aware (UTC) start timestamp.
        """
        if explicit_start_date is not None:
            start_date: datetime = explicit_start_date
        else:
            last_known_timestamp: datetime | None = (
                self._retrieve_latest_transaction_timestamp()
            )
            if last_known_timestamp:
                # Resume from history minus buffer
                start_date = last_known_timestamp - timedelta(
                    days=self.config.pipeline.lookback_days
                )
                logger.info(
                    'Resuming incremental sync. '
                    'Last known data: %s. '
                    'Lookback buffer: %d days. '
                    'Effective Start: %s',
                    last_known_timestamp.date(),
                    self.config.pipeline.lookback_days,
                    start_date.date(),
                )
            else:
                # Use config for the inception date
                start_date_str: str = self.config.pipeline.default_start_date

                # Parse ISO string (YYYY-MM-DD) into a date object
                # This matches the validation logic in config_loader.py
                inception_date: date = date.fromisoformat(start_date_str)

                # Convert to datetime at midnight UTC
                # We strictly need a datetime because our SOAP Request models
                # (GetMCTransExtLocV2Request) define beg_date as datetime.
                start_date = datetime.combine(
                    inception_date, datetime.min.time(), tzinfo=UTC
                )

                logger.info(
                    'No existing history found. Performing initial load from %s',
                    start_date.date(),
                )

        # Ensure UTC hygiene
        if start_date.tzinfo is None:
            return start_date.replace(tzinfo=UTC)
        return start_date

    def _fetch_single_batch(
        self,
        batch_start: datetime,
        batch_end: datetime,
    ) -> pd.DataFrame | None:
        """
        Fetch transaction data for a single time window using an authenticated client.

        This method handles the core business logic of making a single SOAP API call
        and converting the response into a structured DataFrame. It is designed to be
        called by the retry wrapper method, which manages client lifecycle and error
        recovery.

        Args:
            client: An authenticated EfsClient instance ready to execute operations.
            batch_start: The beginning timestamp of the batch window (inclusive).
            batch_end: The ending timestamp of the batch window (inclusive).

        Returns:
            pd.DataFrame | None: A DataFrame containing the transaction records if any
                                 were found, or None if the API returned zero transactions
                                 for this time window.

        Raises:
            Exception: Any errors from the SOAP call or XML parsing are propagated
                      to the caller for retry handling.
        """
        request = GetMCTransExtLocV2Request(
            beg_date=batch_start,
            end_date=batch_end,
        )

        # Execute SOAP call and parse the XML response
        raw_response: Response = self.efs_client.execute_operation(request)
        parsed_response: GetMCTransExtLocV2Response = (
            GetMCTransExtLocV2Response.from_soap_response(raw_response.text)
        )

        if parsed_response.transaction_count > 0:
            batch_df: pd.DataFrame = parsed_response.to_dataframe()
            logger.debug('  Batch retrieved %d records.', len(batch_df))
            return batch_df
        else:
            logger.debug('  Batch returned no records.')
            return None

    def _fetch_batch_with_retry(
        self,
        batch_start: datetime,
        batch_end: datetime,
    ) -> pd.DataFrame | None:
        """
        Fetch a batch with automatic retry logic and session recovery.

        This method implements a resilient data fetching strategy that handles
        transient failures (network issues, token expiration, etc.) by:
        1. Using a context manager to ensure proper client cleanup
        2. Recreating the client session on each retry attempt
        3. Applying exponential backoff between retries
        4. Bubbling up fatal errors after exhausting retries

        The context manager pattern eliminates the need for manual lifecycle management
        and ensures that resources are always properly released, even during errors.

        Args:
            batch_start: The beginning timestamp of the batch window (inclusive).
            batch_end: The ending timestamp of the batch window (inclusive).

        Returns:
            pd.DataFrame | None: A DataFrame containing transactions if found, or None
                                if no data exists for this time window.

        Raises:
            Exception: Re-raises the last exception encountered if all retry attempts
                      are exhausted, which signals the pipeline to abort.
        """
        logger.debug('Processing batch: %s -> %s', batch_start, batch_end)

        max_retries: int = self.config.client.max_retries
        backoff_factor: float = self.config.client.retry_backoff_factor

        for attempt in range(max_retries):
            try:
                return self._fetch_single_batch(batch_start, batch_end)

            except Exception as e:
                logger.warning(
                    'Error fetching batch %s (Attempt %d/%d): %r',
                    batch_start,
                    attempt + 1,
                    max_retries,
                    e,
                )

                # Check if we've exhausted all retry attempts
                if attempt == max_retries - 1:
                    logger.error(
                        'Max retries exceeded for batch starting %s. '
                        'Aborting pipeline.',
                        batch_start,
                    )
                    raise

                # Defensive session recovery: Since EFS returns generic 500 errors
                # for everything, we can't distinguish session failures from other
                # issues. Recreate the client on ANY error to ensure session problems
                # are resolved. This adds ~2 seconds per failed batch, which is
                # acceptable for error cases.
                logger.info(
                    'Recreating EFS client before retry (defensive recovery '
                    'due to generic SOAP error responses)'
                )
                try:
                    self._recreate_client()
                except Exception as recreate_error:
                    logger.error(
                        'Failed to recreate client: %r. '
                        'Will retry with existing client after backoff.',
                        recreate_error,
                    )

                # Exponential backoff: wait progressively longer between retries
                # (1s, 2s, 4s for default max_retries=3)
                sleep_seconds: float = backoff_factor**attempt
                logger.info(
                    'Backing off for %d seconds before retrying with fresh session...',
                    sleep_seconds,
                )
                time.sleep(sleep_seconds)

        # This line should never be reached due to the raise above, but it satisfies
        # type checkers that expect all code paths to return a value.
        return None

    def _fetch_batches(
        self,
        start_timestamp: datetime,
        end_timestamp: datetime,
    ) -> list[pd.DataFrame]:
        """
        Execute the batch extraction loop with retry logic and session recovery.

        This method orchestrates the download of transaction data across a potentially
        large time window by:
        1. Splitting the window into manageable daily (or custom-sized) chunks
        2. Delegating each chunk to the retry-enabled fetch method
        3. Applying rate limiting between successful requests
        4. Collecting all successful results into a single list

        The heavy lifting of error handling and client lifecycle management is
        delegated to the helper methods, keeping this orchestration logic clean
        and focused on iteration.

        Args:
            start_timestamp: The beginning of the total window (inclusive).
            end_timestamp: The end of the total window (inclusive).

        Returns:
            list[pd.DataFrame]: A list of DataFrames, one for each batch that returned
                               data. Returns an empty list if no transactions were found
                               in the entire time window.

        Raises:
            Exception: Propagates any fatal errors from the retry logic, which will
                      abort the pipeline run.
        """
        downloaded_dataframes: list[pd.DataFrame] = []
        batch_start_cursor: datetime = start_timestamp

        delay_seconds: float = self.config.pipeline.request_delay_seconds

        while batch_start_cursor < end_timestamp:
            # Calculate batch end, clamping to the total end timestamp to avoid
            # requesting data beyond the intended window
            batch_end_cursor: datetime = min(
                batch_start_cursor
                + timedelta(days=self.config.pipeline.batch_size_days),
                end_timestamp,
            )

            # Fetch the batch with automatic retry and session management
            batch_df: pd.DataFrame | None = self._fetch_batch_with_retry(
                batch_start_cursor,
                batch_end_cursor,
            )

            if batch_df is not None:
                downloaded_dataframes.append(batch_df)

            # Apply rate limiting between batches to be a good API citizen
            if delay_seconds > 0:
                time.sleep(delay_seconds)

            # Advance the cursor for the next batch
            batch_start_cursor = batch_end_cursor

        return downloaded_dataframes

    def _merge_deduplicate_and_save(
        self,
        new_dataframes: list[pd.DataFrame],
        dry_run: bool,
    ) -> None:
        """
        Consolidate new data, merge with history, deduplicate, and save to disk.

        This method implements the "Load" phase of the ETL process. It ensures
        data integrity by handling duplicates created by the lookback buffer.

        Args:
            new_dataframes: List of DataFrames downloaded from the API.
        """
        if not new_dataframes:
            logger.info('Pipeline run finished. No new data found.')
            return

        logger.debug('Consolidating downloaded batches...')
        newly_retrieved_data: pd.DataFrame = pd.concat(
            new_dataframes, ignore_index=True
        )

        # Use cached historical data (State held in self.dataframe)
        if self.dataframe is not None:
            logger.debug('Merging new data with existing history.')
            final_dataset: pd.DataFrame = pd.concat(
                [self.dataframe, newly_retrieved_data], ignore_index=True
            )
        else:
            logger.debug('No history found. Creating new dataset from retrieved data.')
            final_dataset = newly_retrieved_data

        # Deduplication Logic
        # Sort by date/id first to ensure deterministic order.
        row_count_before: int = len(final_dataset)

        final_dataset.sort_values(
            by=['transaction_date', 'transaction_id'], inplace=True
        )

        final_dataset.drop_duplicates(
            subset=['transaction_id', 'line_number'], keep='last', inplace=True
        )

        row_count_after: int = len(final_dataset)
        duplicates_removed: int = row_count_before - row_count_after

        logger.debug(
            'Deduplication complete. Removed %d duplicate records.', duplicates_removed
        )

        # Update the cache with the final result
        # This ensures subsequent calls in the same session see the new state
        self.dataframe = final_dataset

        # Save to disk (or skip if dry run)
        if dry_run:
            logger.info(
                '[DRY RUN] Would have saved %d total records '
                'to %r. Skipping write operation.',
                row_count_after,
                self.config.storage.parquet_file,
            )
        else:
            # Delegate saving to the storage handler
            # Note: Logging for the save operation happens inside the handler
            self.file_handler.save(final_dataset)

    def cleanup(self) -> None:
        """
        Clean up resources, specifically the EFS client session.

        This method should be called when the pipeline is completely finished,
        either after successful completion or as part of error cleanup.
        It ensures the EFS session is properly terminated.
        """
        logger.debug('Cleaning up current EFS client session')
        try:
            self.efs_client.logout()
            logger.info('EFS client session terminated')
        except Exception as cleanup_error:
            logger.warning(
                'Error during cleanup (session may already be invalid): %r',
                cleanup_error,
            )

    def run_synchronization(
        self,
        explicit_start_date: datetime | None = None,
        explicit_end_date: datetime | None = None,
        dry_run: bool = False,
    ) -> None:
        """
        Execute the main synchronization workflow.

        This method orchestrates the entire ETL (Extract, Transform, Load) process
        by calling specialized helper methods.

        Args:
            explicit_start_date: If provided, forces the pipeline to start from this
                                 date, ignoring existing data.
            explicit_end_date: If provided, stops downloading at this date.
                               Defaults to the current UTC time.
            dry_run: If True, performs all operations EXCEPT saving the Parquet file.
        """
        logger.info(
            'Starting EFS transaction synchronization pipeline (Dry Run: %s).', dry_run
        )

        try:
            # 1. Determine Time Window
            end_timestamp: datetime = explicit_end_date or datetime.now(UTC)
            start_timestamp: datetime = self._determine_start_timestamp(
                explicit_start_date
            )

            if start_timestamp >= end_timestamp:
                logger.info(
                    'Start date is in the future or after end date. No synchronization needed.'
                )
                return

            # 2. Fetch Data (Extract)
            downloaded_data: list[pd.DataFrame] = self._fetch_batches(
                start_timestamp,
                end_timestamp,
            )

            # 3. Process and Persist (Transform & Load)
            self._merge_deduplicate_and_save(downloaded_data, dry_run)

            logger.info('Pipeline synchronization completed successfully.')

        finally:
            # Always cleanup the client session, even if an error occurred
            self.cleanup()
