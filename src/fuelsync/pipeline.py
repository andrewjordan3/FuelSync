# fuelsync/pipeline.py
"""
Data pipeline for incremental synchronization of EFS transactions.

This module handles the logic for downloading transaction data over long periods,
managing incremental state via Parquet files, and deduplicating records.
"""

import logging
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
from requests import Response

from .efs_client import EfsClient
from .models import GetMCTransExtLocV2Request
from .response_models import GetMCTransExtLocV2Response

# Module-level logger
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
    """

    def __init__(
        self,
        parquet_path: str | Path,
        client_config_path: Path | None = None,
    ) -> None:
        """
        Initialize the pipeline configuration.

        Args:
            parquet_path: The file system path where the Parquet file should be
                          stored. If it doesn't exist, it will be created.
            client_config_path: Path to the fuelsync config.yaml. If not provided,
                                the client looks in the default project location.
        """
        self.parquet_file_path: Path = Path(parquet_path)
        self.client_config_path: Path | None = client_config_path

        # Run State / Configuration
        # These are populated when run_synchronization is called
        self.run_start_date: datetime | None = None
        self.run_end_date: datetime | None = None
        self.batch_size_days: int = 1
        self.request_delay_seconds: float = 0.0

    def _retrieve_latest_transaction_timestamp(self) -> datetime | None:
        """
        Inspect the existing Parquet file to find the most recent transaction date.

        This method is optimized to read only the 'transaction_date' column
        to minimize I/O overhead on large datasets.

        Returns:
            datetime | None: The maximum transaction timestamp found in the file,
                             converted to a timezone-aware (UTC) Python datetime object.
                             Returns None if the file does not exist or is empty.
        """
        if not self.parquet_file_path.exists():
            logger.debug(
                f'Parquet file not found at {self.parquet_file_path}. Treating as initial run.'
            )
            return None

        try:
            # Read only the specific column needed to determine the watermark
            logger.debug(f'Reading existing data schema from {self.parquet_file_path}')
            existing_data_date_column: pd.DataFrame = pd.read_parquet(
                self.parquet_file_path, columns=['transaction_date']
            )

            if existing_data_date_column.empty:
                logger.debug('Parquet file exists but contains no rows.')
                return None

            # Extract the maximum date
            max_timestamp: pd.Timestamp = existing_data_date_column[
                'transaction_date'
            ].max()

            if pd.isna(max_timestamp):
                logger.debug('Max date in Parquet file is NaT (Not a Time).')
                return None

            # Convert pandas Timestamp to Python datetime and ensure UTC
            # The EFS API works in UTC, so we standardize on that.
            latest_datetime: datetime = pd.to_datetime(max_timestamp).to_pydatetime()
            if latest_datetime.tzinfo is None:
                latest_datetime = latest_datetime.replace(tzinfo=UTC)

            logger.debug(f'Latest transaction timestamp found: {latest_datetime}')
            return latest_datetime

        except Exception as e:
            # Log warning but do not crash; returning None triggers a full/default load
            logger.warning(
                f'Failed to read watermark from existing Parquet file: {e}. '
                f'Pipeline will default to initial start date.'
            )
            return None

    def _determine_start_timestamp(
        self,
        explicit_start_date: datetime | None,
        lookback_buffer_days: int,
    ) -> datetime:
        """
        Determine the effective start date for the synchronization window.

        This helper method handles the logic of choosing between a user-provided date,
        calculating a resume date based on existing data, or falling back to a default
        inception date.

        Args:
            explicit_start_date: Optional user override for start date.
            lookback_buffer_days: Number of days to overlap if resuming.

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
                start_date = last_known_timestamp - timedelta(days=lookback_buffer_days)
                logger.info(
                    f'Resuming incremental sync. '
                    f'Last known data: {last_known_timestamp.date()}. '
                    f'Lookback buffer: {lookback_buffer_days} days. '
                    f'Effective Start: {start_date.date()}'
                )
            else:
                # There are no Wex transactions for us before August 1, 2025
                start_date = datetime(2025, 8, 1, tzinfo=UTC)
                logger.info(
                    f'No existing history found. Performing initial load from {start_date.date()}'
                )

        # Ensure UTC hygiene
        if start_date.tzinfo is None:
            return start_date.replace(tzinfo=UTC)
        return start_date

    def _fetch_single_batch(
        self,
        client: EfsClient,
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
        raw_response: Response = client.execute_operation(request)
        parsed_response: GetMCTransExtLocV2Response = (
            GetMCTransExtLocV2Response.from_soap_response(raw_response.text)
        )

        if parsed_response.transaction_count > 0:
            batch_df: pd.DataFrame = parsed_response.to_dataframe()
            logger.debug(f'  Batch retrieved {len(batch_df)} records.')
            return batch_df
        else:
            logger.debug('  Batch returned no records.')
            return None

    def _fetch_batch_with_retry(
        self,
        batch_start: datetime,
        batch_end: datetime,
        max_retries: int = 3,
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
            max_retries: Maximum number of attempts before giving up. Each attempt
                        gets a fresh client instance. Default is 3 attempts.

        Returns:
            pd.DataFrame | None: A DataFrame containing transactions if found, or None
                                if no data exists for this time window.

        Raises:
            Exception: Re-raises the last exception encountered if all retry attempts
                      are exhausted, which signals the pipeline to abort.
        """
        logger.debug(f'Processing batch: {batch_start} -> {batch_end}')

        for attempt in range(max_retries):
            try:
                # Context manager handles authentication, logout, and cleanup automatically
                with EfsClient(config_path=self.client_config_path) as client:
                    return self._fetch_single_batch(client, batch_start, batch_end)

            except Exception as e:
                logger.warning(
                    f'Error fetching batch {batch_start} '
                    f'(Attempt {attempt + 1}/{max_retries}): {e}'
                )

                # Check if we've exhausted all retry attempts
                if attempt == max_retries - 1:
                    logger.error(
                        f'Max retries exceeded for batch starting {batch_start}. '
                        f'Aborting pipeline.'
                    )
                    raise

                # Exponential backoff: wait progressively longer between retries
                # (1s, 2s, 4s for default max_retries=3)
                sleep_seconds: int = 2**attempt
                logger.info(
                    f'Backing off for {sleep_seconds} seconds before '
                    f'retrying with fresh session...'
                )
                time.sleep(sleep_seconds)

        # This line should never be reached due to the raise above, but it satisfies
        # type checkers that expect all code paths to return a value.
        return None

    def _fetch_batches(
        self,
        start_timestamp: datetime,
        end_timestamp: datetime,
        batch_size_days: int,
        request_delay_seconds: float,
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
            batch_size_days: Size of each chunk in days. Smaller batches reduce
                           memory pressure and prevent SOAP timeouts.
            request_delay_seconds: Time to sleep between successful requests to
                                 respect API rate limits.

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

        while batch_start_cursor < end_timestamp:
            # Calculate batch end, clamping to the total end timestamp to avoid
            # requesting data beyond the intended window
            batch_end_cursor: datetime = min(
                batch_start_cursor + timedelta(days=batch_size_days),
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
            if request_delay_seconds > 0:
                time.sleep(request_delay_seconds)

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

        # Load history if it exists
        if self.parquet_file_path.exists():
            logger.debug(
                f'Loading existing historical data from {self.parquet_file_path}'
            )
            historical_data: pd.DataFrame = pd.read_parquet(self.parquet_file_path)
            final_dataset: pd.DataFrame = pd.concat(
                [historical_data, newly_retrieved_data], ignore_index=True
            )
        else:
            logger.debug(
                'No history file found. Creating new dataset from retrieved data.'
            )
            final_dataset = newly_retrieved_data

        # Deduplication Logic
        # Sort by date/id first to ensure deterministic order.
        # keep='last' ensures we retain the most recently downloaded version of a record.
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
            f'Deduplication complete. Removed {duplicates_removed} duplicate records.'
        )

        # Save to disk (or skip if dry run)
        if dry_run:
            logger.info(
                f'[DRY RUN] Would have saved {row_count_after} total records '
                f'to {self.parquet_file_path}. Skipping write operation.'
            )
        else:
            logger.info(
                f'Saving synchronized dataset ({row_count_after} total records) '
                f'to {self.parquet_file_path}'
            )
            self.parquet_file_path.parent.mkdir(parents=True, exist_ok=True)
            final_dataset.to_parquet(
                self.parquet_file_path, index=False, compression='snappy'
            )

    def run_synchronization(
        self,
        explicit_start_date: datetime | None = None,
        explicit_end_date: datetime | None = None,
        lookback_buffer_days: int = 7,
        batch_size_days: int = 1,
        dry_run: bool = False,
        request_delay_seconds: float = 0.0,
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
            lookback_buffer_days: Number of days to overlap with existing data.
                                  This catches transactions that were posted late
                                  by vendors. Default is 7 days.
            batch_size_days: The duration of each API call window. Kept small (1 day)
                             to ensure SOAP responses remain manageable in size.
            dry_run: If True, performs all operations EXCEPT saving the Parquet file.
            request_delay_seconds: Time to wait between API requests (rate limiting).
        """
        logger.info(
            f'Starting EFS transaction synchronization pipeline (Dry Run: {dry_run}).'
        )

        # 1. Determine Time Window
        end_timestamp: datetime = explicit_end_date or datetime.now(UTC)
        start_timestamp: datetime = self._determine_start_timestamp(
            explicit_start_date, lookback_buffer_days
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
            batch_size_days,
            request_delay_seconds,
        )

        # 3. Process and Persist (Transform & Load)
        self._merge_deduplicate_and_save(downloaded_data, dry_run)

        logger.info('Pipeline synchronization completed successfully.')
