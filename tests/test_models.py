"""Tests for request models."""

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from fuelsync.models import GetMCTransExtLocV2Request


class TestGetMCTransExtLocV2Request:
    """Tests for GetMCTransExtLocV2Request model."""

    def test_valid_request_creation(self) -> None:
        """Test creating a valid request."""
        beg_date = datetime(2024, 11, 1, tzinfo=UTC)
        end_date = datetime(2024, 11, 14, tzinfo=UTC)

        request = GetMCTransExtLocV2Request(beg_date=beg_date, end_date=end_date)

        assert request.beg_date == beg_date
        assert request.end_date == end_date

    def test_operation_name_is_correct(self) -> None:
        """Test that operation_name class attribute is set correctly."""
        beg_date = datetime(2024, 11, 1, tzinfo=UTC)
        end_date = datetime(2024, 11, 14, tzinfo=UTC)

        request = GetMCTransExtLocV2Request(beg_date=beg_date, end_date=end_date)

        assert request.operation_name == 'getMCTransExtLocV2'

    def test_template_name_is_correct(self) -> None:
        """Test that template_name class attribute is set correctly."""
        beg_date = datetime(2024, 11, 1, tzinfo=UTC)
        end_date = datetime(2024, 11, 14, tzinfo=UTC)

        request = GetMCTransExtLocV2Request(beg_date=beg_date, end_date=end_date)

        assert request.template_name == 'getMCTransExtLocV2.xml'

    def test_end_date_before_beg_date_raises_error(self) -> None:
        """Test that end_date before beg_date raises ValidationError."""
        beg_date = datetime(2024, 11, 14, tzinfo=UTC)
        end_date = datetime(2024, 11, 1, tzinfo=UTC)

        with pytest.raises(ValidationError, match='end_date must be after or equal'):
            GetMCTransExtLocV2Request(beg_date=beg_date, end_date=end_date)

    def test_equal_dates_allowed(self) -> None:
        """Test that equal beg_date and end_date is allowed."""
        date = datetime(2024, 11, 14, tzinfo=UTC)

        request = GetMCTransExtLocV2Request(beg_date=date, end_date=date)

        assert request.beg_date == date
        assert request.end_date == date

    def test_missing_beg_date_raises_error(self) -> None:
        """Test that missing beg_date raises ValidationError."""
        with pytest.raises(ValidationError):
            GetMCTransExtLocV2Request(end_date=datetime(2024, 11, 14, tzinfo=UTC)) # pyright: ignore[reportCallIssue]

    def test_missing_end_date_raises_error(self) -> None:
        """Test that missing end_date raises ValidationError."""
        with pytest.raises(ValidationError):
            GetMCTransExtLocV2Request(beg_date=datetime(2024, 11, 1, tzinfo=UTC)) # pyright: ignore[reportCallIssue]

    def test_to_soap_format_returns_dict(self) -> None:
        """Test that to_soap_format returns a dictionary."""
        beg_date = datetime(2024, 11, 1, 12, 30, 45, 123000, tzinfo=UTC)
        end_date = datetime(2024, 11, 14, 15, 30, 45, 456000, tzinfo=UTC)

        request = GetMCTransExtLocV2Request(beg_date=beg_date, end_date=end_date)
        soap_format: dict[str, str | None] = request.to_soap_format()

        assert isinstance(soap_format, dict)
        assert 'begDate' in soap_format
        assert 'endDate' in soap_format

    def test_to_soap_format_correct_format(self) -> None:
        """Test that to_soap_format produces correctly formatted strings."""
        beg_date = datetime(2024, 11, 1, 12, 30, 45, 123000, tzinfo=UTC)
        end_date = datetime(2024, 11, 14, 15, 30, 45, 456000, tzinfo=UTC)

        request = GetMCTransExtLocV2Request(beg_date=beg_date, end_date=end_date)
        soap_format: dict[str, str | None] = request.to_soap_format()

        assert soap_format['begDate'] == '2024-11-01T12:30:45.123+00:00'
        assert soap_format['endDate'] == '2024-11-14T15:30:45.456+00:00'

    def test_naive_datetime_converted_to_utc(self) -> None:
        """Test that naive datetime is converted to UTC in SOAP format."""
        beg_date = datetime(2024, 11, 1, 12, 30, 45)  # noqa: DTZ001
        end_date = datetime(2024, 11, 14, 15, 30, 45)  # noqa: DTZ001

        request = GetMCTransExtLocV2Request(beg_date=beg_date, end_date=end_date)
        soap_format: dict[str, str | None] = request.to_soap_format()

        # Should have +00:00 timezone
        assert '+00:00' in soap_format['begDate'] # pyright: ignore[reportOperatorIssue]
        assert '+00:00' in soap_format['endDate'] # pyright: ignore[reportOperatorIssue]
