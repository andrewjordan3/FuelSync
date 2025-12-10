# fuelsync/models.py
"""
Pydantic models for EFS SOAP API operations.

Each model represents the input parameters for a specific SOAP operation
and includes validation to ensure data conforms to the API specification
before sending requests.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator

from fuelsync.utils import format_for_soap


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


class GetCardSummariesRequest(EfsOperationRequest):
    """
    Request model for getCardSummaries operation.

    Returns a list of cards and card data. You can limit the search by type,
    search parameters, or payroll use fields, or retrieve all card details.

    Args:
        search_type: The field type to search on (e.g., 'STATUS', 'UNIT', 'NUMBER').
        search_param: The value to search for within the specified search_type field.
                      For STATUS searches: 'A' (Active), 'H' (Hold), 'U' (Fraud), 'I' (Inactive).
        payroll_use: Filter cards by payroll usage type.
                     'P' = Payroll Only, 'B' = Both payroll and company, 'N' = Company side only.

    Returns:
        A GetCardSummariesResponse containing a list of matching card summaries.

    Raises:
        ValidationError: If field validation fails (e.g., invalid search_type or payroll_use).
        SOAPFault: If the EFS API returns a SOAP fault.

    Example:
        >>> # Get all active cards
        >>> request = GetCardSummariesRequest(
        ...     search_type='STATUS',
        ...     search_param='A'
        ... )
        >>> # Get cards for a specific unit
        >>> request = GetCardSummariesRequest(
        ...     search_type='UNIT',
        ...     search_param='TRUCK001'
        ... )
        >>> # Get all cards (no filters)
        >>> request = GetCardSummariesRequest()
    """

    operation_name: str = 'getCardSummaries'
    template_name: str = 'getCardSummaries.xml'

    model_config = ConfigDict(populate_by_name=True, extra='forbid')

    search_type: (
        Literal[
            'NUMBER',
            'XREF',
            'UNIT',
            'DRIVERID',
            'DRIVERNAME',
            'POLICY',
            'GPSID',
            'VIN',
            'STATUS',
        ]
        | None
    ) = Field(
        None,
        alias='type',
        max_length=10,  # Longest value is 'DRIVERNAME' (10 chars)
        description='Field type to search on',
    )

    search_param: str | None = Field(
        None,
        alias='searchParam',
        max_length=24,  # Per API docs, card fields are max 24 chars
        description='Search value for the specified search_type field',
    )

    payroll_use: Literal['B', 'P', 'N'] | None = Field(
        None,
        alias='payrUse',
        description='Payroll usage filter: (B)oth, (P)ayroll only, (N)o payroll/company only',
    )

    @field_validator('search_param')
    @classmethod
    def validate_search_param_requires_type(
        cls, value: str | None, info: ValidationInfo
    ) -> str | None:
        """
        Ensure search_param is only provided when search_type is also specified.

        Args:
            value: The search_param value being validated.
            info: Pydantic validation context containing other field values.

        Returns:
            The validated search_param value.

        Raises:
            ValueError: If search_param is provided without search_type.
        """
        if value is not None and info.data.get('type') is None:
            raise ValueError(
                'search_param cannot be provided without specifying search_type'
            )
        return value

    def to_soap_format(self) -> dict[str, str | None]:
        """
        Convert request fields to SOAP-compliant dictionary format.

        This method maps Python field names to the XML tag names expected by
        the EFS SOAP API. It only includes fields that have non-None values.

        Returns:
            Dictionary with SOAP-compatible field names as keys and field values.
            Keys use the API's expected names (camelCase with 'type', 'searchParam', 'payrUse').

        Example:
            >>> request = GetCardSummariesRequest(search_type='STATUS', search_param='A')
            >>> request.to_soap_format()
            {'type': 'STATUS', 'searchParam': 'A', 'payrUse': None}
        """
        return {
            'type': self.search_type,
            'searchParam': self.search_param,
            'payrUse': self.payroll_use,
        }
