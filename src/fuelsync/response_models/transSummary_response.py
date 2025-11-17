# fuelsync/response_models/transSummary_response.py
"""
Pydantic models for parsing transSummary SOAP API response.

The transSummary operation returns a simple summary with transaction
count and total amount for a given date range.
"""

import logging
from datetime import datetime

from lxml import (
    etree,  # pyright: ignore[reportUnknownVariableType, reportAttributeAccessIssue]
)
from pydantic import BaseModel, ConfigDict, Field

from ..utils import (
    check_for_soap_fault,
    extract_soap_body,
    parse_soap_response,
    safe_convert,
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
    def from_xml_element(cls, element: etree._Element) -> 'WSTransSummary':
        """
        Parse a <result> XML element into a WSTransSummary instance.

        Args:
            element: The XML element containing the summary data.

        Returns:
            A validated WSTransSummary instance.
        """
        data: dict[str, int | float | bool | datetime | str | None] = {
            'tranCount': safe_convert(element, 'tranCount', int),
            'tranTotal': safe_convert(element, 'tranTotal', float),
        }
        return cls.model_validate(data)

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
        root: etree._Element = parse_soap_response(xml_string)

        # 2. Check for SOAP:Fault
        check_for_soap_fault(root)

        # 3. Get the <soap:Body>
        body: etree._Element = extract_soap_body(root)

        # 4. Find the <result> element containing the summary
        result_element = body.find('.//result')

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
