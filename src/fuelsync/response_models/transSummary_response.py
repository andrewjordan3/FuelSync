# fuelsync/response_models/transSummary_response.py
"""
Pydantic models for parsing transSummary SOAP API response.

The transSummary operation returns a simple summary with transaction
count and total amount for a given date range.
"""

import logging
from typing import Any

from lxml import etree
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from ..utils import (
    check_for_soap_fault,
    extract_soap_body,
    parse_soap_response,
    parse_xml_to_dict,
)

logger: logging.Logger = logging.getLogger(__name__)


class WSTransSummary(BaseModel):
    """
    Transaction summary containing count and total amount.

    This is the response for the transSummary operation, providing
    aggregate statistics for transactions in a date range.

    Attributes:
        tran_count: Number of transactions in the period.
        tran_total: Total dollar amount across all transactions.
    """

    model_config = ConfigDict(populate_by_name=True)

    tran_count: int | None = Field(None, alias='tranCount')
    tran_total: float | None = Field(None, alias='tranTotal')

    @classmethod
    def from_xml_element(cls, element: etree.Element) -> 'WSTransSummary':
        """
        Parse a <value> XML element into a WSMCTransExtLocV2 instance.

        Uses the generic parse_xml_to_dict utility which automatically handles:
        - Simple fields (via Pydantic field aliases)
        - Nested single models (WSFleetMemo)
        - Nested list models (lineItems, infos, etc.)
        """
        # Extract raw data from XML using introspection
        data: dict[str, Any] = parse_xml_to_dict(element, cls)

        try:
            # Validate and convert types using Pydantic
            return cls.model_validate(data)
        except ValidationError as e:
            logger.error(f'Failed to validate parsed XML data: {e}\nData: {data}')
            raise ValueError(f'Pydantic validation failed for transaction: {e}') from e

    def __repr__(self) -> str:
        return f'WSTransSummary(count={self.tran_count}, total=${self.tran_total:.2f})'


class TransSummaryResponse(BaseModel):
    """
    Response model for transSummary operation.

    Contains a single summary object with transaction count and total amount.
    """

    summary: WSTransSummary | None = None

    @classmethod
    def from_soap_response(cls, xml_string: str) -> 'TransSummaryResponse':
        """
        Parse a SOAP XML response into a TransSummaryResponse instance.

        Args:
            xml_string: The raw XML response from the SOAP API.

        Returns:
            A validated TransSummaryResponse with the summary data.

        Raises:
            RuntimeError: If the response contains a SOAP Fault.
            ValueError: If the response structure is invalid.

        Example:
            >>> from fuelsync import EfsClient
            >>> from fuelsync.models import TransSummaryRequest
            >>> from fuelsync.response_models.transSummary_response import TransSummaryResponse
            >>> from datetime import datetime, timezone
            >>>
            >>> with EfsClient() as client:
            >>>     request = TransSummaryRequest(
            >>>         beg_date=datetime(2025, 11, 1, tzinfo=timezone.utc),
            >>>         end_date=datetime(2025, 11, 14, tzinfo=timezone.utc)
            >>>     )
            >>>     response = client.execute_operation(request)
            >>>     parsed = TransSummaryResponse.from_soap_response(response.text)
            >>>     print(f'Count: {parsed.summary.tran_count}')
            >>>     print(f'Total: ${parsed.summary.tran_total:,.2f}')
        """
        logger.debug('Parsing SOAP response for TransSummaryResponse')

        # 1. Parse string to XML
        root: etree.Element = parse_soap_response(xml_string)

        # 2. Check for SOAP:Fault
        check_for_soap_fault(root)

        # 3. Get the <soap:Body>
        body: etree.Element = extract_soap_body(root)

        # 4. Find the <result> element containing the summary
        result_element: etree.Element | None = body.find('.//result')

        if result_element is None:
            logger.warning('No <result> element found in the response body.')
            return cls(summary=None)

        logger.info('Found <result> element, parsing summary.')

        try:
            summary: WSTransSummary = WSTransSummary.from_xml_element(result_element)
            logger.info(
                f'Successfully parsed summary: '
                f'{summary.tran_count} transactions, '
                f'${summary.tran_total:.2f} total'
            )
            return cls(summary=summary)
        except Exception as e:
            failed_xml: str = etree.tostring(
                result_element, pretty_print=True, encoding='unicode'
            )
            logger.error(
                f'Failed to parse summary element: {e}\n'
                f'--- Failing XML Snippet ---\n{failed_xml}\n'
                f'--- End Snippet ---'
            )
            raise ValueError(f'Failed to parse transaction summary: {e}') from e

    def __repr__(self) -> str:
        if self.summary:
            return (
                f'TransSummaryResponse('
                f'count={self.summary.tran_count}, '
                f'total=${self.summary.tran_total:.2f})'
            )
        return 'TransSummaryResponse(summary=None)'
