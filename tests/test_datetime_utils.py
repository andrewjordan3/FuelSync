"""Tests for datetime utilities."""

from datetime import UTC, date, datetime, timedelta, timezone

from fuelsync.utils.datetime_utils import format_for_soap


class TestFormatForSoap:
    """Tests for the format_for_soap function."""

    def test_datetime_with_utc_timezone(self) -> None:
        """Test formatting datetime with UTC timezone."""
        dt = datetime(2025, 11, 14, 15, 30, 45, 123000, tzinfo=UTC)
        result: str = format_for_soap(dt)
        assert result == '2025-11-14T15:30:45.123+00:00'

    def test_datetime_with_negative_timezone_offset(self) -> None:
        """Test formatting datetime with negative timezone offset."""
        tz = timezone(timedelta(hours=-6))
        dt = datetime(2025, 11, 14, 15, 30, 45, 123000, tzinfo=tz)
        result: str = format_for_soap(dt)
        assert result == '2025-11-14T15:30:45.123-06:00'

    def test_datetime_with_positive_timezone_offset(self) -> None:
        """Test formatting datetime with positive timezone offset."""
        tz = timezone(timedelta(hours=5, minutes=30))
        dt = datetime(2025, 11, 14, 15, 30, 45, 123000, tzinfo=tz)
        result: str = format_for_soap(dt)
        assert result == '2025-11-14T15:30:45.123+05:30'

    def test_naive_datetime_assumes_utc(self) -> None:
        """Test that naive datetime (no timezone) is assumed to be UTC."""
        dt = datetime(2025, 11, 14, 15, 30, 45, 123000)  # noqa: DTZ001
        result: str = format_for_soap(dt)
        assert result == '2025-11-14T15:30:45.123+00:00'

    def test_date_object_converts_to_midnight_utc(self) -> None:
        """Test that date object is converted to datetime at midnight UTC."""
        d = date(2025, 11, 14)
        result: str = format_for_soap(d)
        assert result == '2025-11-14T00:00:00.000+00:00'

    def test_milliseconds_zero_padded(self) -> None:
        """Test that milliseconds are zero-padded to 3 digits."""
        dt = datetime(2025, 11, 14, 15, 30, 45, 1000, tzinfo=UTC)
        result: str = format_for_soap(dt)
        assert result == '2025-11-14T15:30:45.001+00:00'

    def test_midnight_time(self) -> None:
        """Test formatting datetime at midnight."""
        dt = datetime(2025, 11, 14, 0, 0, 0, 0, tzinfo=UTC)
        result: str = format_for_soap(dt)
        assert result == '2025-11-14T00:00:00.000+00:00'

    def test_end_of_day(self) -> None:
        """Test formatting datetime at end of day."""
        dt = datetime(2025, 11, 14, 23, 59, 59, 999000, tzinfo=UTC)
        result: str = format_for_soap(dt)
        assert result == '2025-11-14T23:59:59.999+00:00'

    def test_new_years_eve(self) -> None:
        """Test formatting datetime on New Year's Eve."""
        dt = datetime(2025, 12, 31, 23, 59, 59, 999000, tzinfo=UTC)
        result: str = format_for_soap(dt)
        assert result == '2025-12-31T23:59:59.999+00:00'

    def test_leap_year_date(self) -> None:
        """Test formatting datetime on leap year date."""
        dt = datetime(2024, 2, 29, 12, 0, 0, 0, tzinfo=UTC)
        result: str = format_for_soap(dt)
        assert result == '2024-02-29T12:00:00.000+00:00'
