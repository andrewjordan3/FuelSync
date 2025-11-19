# fuelsync/models.py
"""
Pydantic models for EFS SOAP API operations.

Each model represents the input parameters for a specific SOAP operation
and includes validation to ensure data conforms to the API specification
before sending requests.
"""

from abc import ABC, abstractmethod
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator

from .utils import format_for_soap


class EfsOperationRequest(BaseModel, ABC):
    """
    Abstract base class for all EFS SOAP operation requests.

    All operation-specific request models inherit from this base class,
    which provides:
    - Standard interface for SOAP formatting
    - Operation name and template name as class attributes
    - Type safety for execute_operation method

    Subclasses must define:
    - operation_name: The SOAP operation name (class attribute)
    - template_name: The Jinja2 template filename (class attribute)
    - to_soap_format(): Method to convert fields to SOAP-compliant format

    This design allows execute_operation to accept any operation request
    and automatically determine the correct operation and template to use.
    """

    # Subclasses must override these class attributes
    operation_name: str = ''
    template_name: str = ''

    @abstractmethod
    def to_soap_format(self) -> dict[str, str | None]:
        """
        Convert model fields to SOAP-compliant format.

        Must be implemented by each operation-specific model to handle
        its specific field types and formatting requirements.

        Returns:
            Dictionary mapping parameter names to formatted string values.
        """
        pass

class GetMCTransExtLocV2Request(EfsOperationRequest):
    """
    Request model for getMCTransExtLocV2 operation.

    This operation returns transactions for a specified date range.
    This is the preferred version for getting transaction data and
    should be used for all new development.

    Attributes:
        beg_date: Start date for transaction search (inclusive).
        end_date: End date for transaction search (inclusive).
    """
    # Define operation-specific class attributes
    operation_name: str = 'getMCTransExtLocV2'
    template_name: str = 'getMCTransExtLocV2.xml'

    model_config = ConfigDict(populate_by_name=True)

    beg_date: datetime = Field(
        ...,
        description='Start date for search (inclusive)',
    )
    end_date: datetime = Field(
        ...,
        description='End date for search (inclusive)',
    )

    @field_validator('end_date')
    @classmethod
    def validate_date_range(cls, end_date: datetime, info: ValidationInfo) -> datetime:
        """Ensure end_date is not before beg_date."""
        if 'beg_date' in info.data and end_date < info.data['beg_date']:
            raise ValueError('end_date must be after or equal to beg_date')
        return end_date

    def to_soap_format(self) -> dict[str, str | None]:
        """
        Convert datetime objects to SOAP-compliant string format.

        Returns:
            Dictionary with formatted datetime strings.
        """
        return {
            'begDate': format_for_soap(self.beg_date),
            'endDate': format_for_soap(self.end_date),
        }

    class Config:
        populate_by_name = True


class TransSummaryRequest(EfsOperationRequest):
    """
    Request model for transSummaryRequest operation.

    Returns a summary of transactions for a specified date range.

    Attributes:
        beg_date: Start date for transaction summary (inclusive).
        end_date: End date for transaction summary (inclusive).
    """

    # Define operation-specific class attributes
    operation_name: str = 'transSummaryRequest'
    template_name: str = 'transSummaryRequest.xml'

    model_config = ConfigDict(populate_by_name=True)

    beg_date: datetime = Field(
        ...,
        description='Start date for summary (inclusive)',
    )
    end_date: datetime = Field(
        ...,
        description='End date for summary (inclusive)',
    )

    @field_validator('end_date')
    @classmethod
    def validate_date_range(cls, end_date: datetime, info: ValidationInfo) -> datetime:
        """Ensure end_date is not before beg_date."""
        if 'beg_date' in info.data and end_date < info.data['beg_date']:
            raise ValueError('end_date must be after or equal to beg_date')
        return end_date

    def to_soap_format(self) -> dict[str, str | None]:
        """
        Convert datetime objects to SOAP-compliant string format.

        Returns:
            Dictionary with formatted datetime strings.
        """
        return {
            'begDate': format_for_soap(self.beg_date),
            'endDate': format_for_soap(self.end_date),
        }

    class Config:
        populate_by_name = True


class WSTranRejectSearch(EfsOperationRequest):
    """
    Request model for searching rejected transactions.

    This model allows searching for rejected transactions with various
    optional filters including card number, invoice, and location.

    Attributes:
        start_date: Start date for reject search (inclusive).
        end_date: End date for reject search (inclusive).
        card_num: Optional card number filter (nullable).
        invoice: Optional invoice number filter (nullable).
        location_id: Optional location ID filter (nullable).
    """

    # Define operation-specific class attributes
    operation_name: str = 'getTranRejects'
    template_name: str = 'getTranRejects.xml'

    model_config = ConfigDict(populate_by_name=True)

    start_date: datetime = Field(
        ...,
        description='Start date for reject search (inclusive)',
    )
    end_date: datetime = Field(
        ...,
        description='End date for reject search (inclusive)',
    )
    card_num: str | None = Field(
        None,
        description='Optional card number filter',
        alias='cardNum',
    )
    invoice: str | None = Field(
        None,
        description='Optional invoice number filter',
    )
    location_id: int | None = Field(
        None,
        description='Optional location ID filter',
        alias='locationId',
    )

    @field_validator('end_date')
    @classmethod
    def validate_date_range(cls, end_date: datetime, info: ValidationInfo) -> datetime:
        """Ensure end_date is not before start_date."""
        if 'start_date' in info.data and end_date < info.data['start_date']:
            raise ValueError('end_date must be after or equal to start_date')
        return end_date

    def to_soap_format(self) -> dict[str, str | None]:
        """
        Convert fields to SOAP-compliant format.

        Returns:
            Dictionary with formatted values.
        """
        return {
            'startDate': format_for_soap(self.start_date),
            'endDate': format_for_soap(self.end_date),
            'cardNum': self.card_num,
            'invoice': self.invoice,
            'locationId': str(self.location_id) if self.location_id else None,
        }

