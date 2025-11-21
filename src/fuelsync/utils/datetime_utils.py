# fuelsync/utils/datetime_utils.py
"""
Datetime utilities for EFS SOAP API.

Provides functions to format date and datetime objects according to the
EFS API specification.
"""

from datetime import UTC, date, datetime


def format_for_soap(dt: date | datetime) -> str:
    """
    Format a date or datetime object for EFS SOAP API requests.

    The EFS API expects ISO 8601 format with timezone offset:
    YYYY-MM-DDThh:mm:ss.ccc-hh:mm

    Where:
    - YYYY-MM-DD is the date
    - T is the literal separator
    - hh:mm:ss.ccc is the time with milliseconds
    - -hh:mm or +hh:mm is the timezone offset from UTC

    Args:
        dt: A date or datetime object. If a date is provided, it will be
            converted to datetime at midnight UTC. If a datetime without
            timezone info is provided, UTC timezone is assumed.

    Returns:
        ISO 8601 formatted string suitable for EFS SOAP API.

    Examples:
        >>> from datetime import datetime, timezone, timedelta
        >>>
        >>> # Datetime with timezone
        >>> dt = datetime(2025, 11, 14, 15, 30, 45, 123000,
        ...               tzinfo=timezone(timedelta(hours=-6)))
        >>> format_for_soap(dt)
        '2025-11-14T15:30:45.123-06:00'
        >>>
        >>> # Date object (converted to midnight UTC)
        >>> d = date(2025, 11, 14)
        >>> format_for_soap(d)
        '2025-11-14T00:00:00.000+00:00'
        >>>
        >>> # Naive datetime (assumes UTC)
        >>> dt = datetime(2025, 11, 14, 15, 30, 45)
        >>> format_for_soap(dt)
        '2025-11-14T15:30:45.000+00:00'
    """
    # Convert date to datetime at midnight UTC if needed
    if isinstance(dt, date) and not isinstance(dt, datetime): # pyright: ignore[reportUnnecessaryIsInstance]
        dt = datetime.combine(dt, datetime.min.time(), tzinfo=UTC)

    # If datetime is naive (no timezone), assume UTC
    if isinstance(dt, datetime) and dt.tzinfo is None: # pyright: ignore[reportUnnecessaryIsInstance]
        dt = dt.replace(tzinfo=UTC)

    # Format the main datetime part with milliseconds
    # strftime doesn't directly support milliseconds in the format we need,
    # so we build it manually
    date_time_part: str = dt.strftime('%Y-%m-%dT%H:%M:%S')

    # Add milliseconds (3 digits)
    milliseconds: int = dt.microsecond // 1000
    date_time_with_ms: str = f'{date_time_part}.{milliseconds:03d}'

    # Format timezone offset as ±hh:mm
    # strftime %z gives ±hhmm, we need to insert the colon
    tz_offset: str = dt.strftime('%z')  # e.g., '-0600' or '+0000'

    if tz_offset:
        # Insert colon: '-0600' -> '-06:00'
        tz_formatted: str = f'{tz_offset[:3]}:{tz_offset[3:]}'
    else:
        # Fallback to UTC if somehow no timezone
        tz_formatted = '+00:00'

    return f'{date_time_with_ms}{tz_formatted}'
