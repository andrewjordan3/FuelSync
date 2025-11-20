# fuelsync/response_models/getTranRejects_response.py
"""
Pydantic models for parsing getTranRejects SOAP API response.

The getTranRejects operation returns an array of rejected transactions
with details about why each transaction was rejected.
"""

import logging
from datetime import datetime
from typing import Any

import pandas as pd
from lxml import (
    etree,  # pyright: ignore[reportUnknownVariableType, reportAttributeAccessIssue]
)
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from ..utils import (
    check_for_soap_fault,
    extract_soap_body,
    parse_soap_response,
    parse_xml_to_dict,
)

logger: logging.Logger = logging.getLogger(__name__)


class WSTranReject(BaseModel):
    """
    Represents a single rejected transaction.

    Contains information about a transaction that was rejected,
    including the reason for rejection and location details.

    Attributes:
        tran_date: Date and time when the transaction was attempted.
        card_num: Card number used for the transaction.
        invoice: Invoice number associated with the transaction.
        loc_id: Location ID where the transaction was attempted.
        loc_name: Name of the location.
        loc_city: City of the location.
        loc_state: State of the location.
        error_code: Numeric error code indicating the rejection reason.
        error_desc: Human-readable description of the error.
        unit: Unit/vehicle identifier associated with the card.
    """

    model_config = ConfigDict(populate_by_name=True)

    tran_date: datetime | None = Field(None, alias='tranDate')
    card_num: str | None = Field(None, alias='cardNum')
    invoice: str | None = Field(None, alias='invoice')
    loc_id: int | None = Field(None, alias='locId')
    loc_name: str | None = Field(None, alias='locName')
    loc_city: str | None = Field(None, alias='locCity')
    loc_state: str | None = Field(None, alias='locState')
    error_code: int | None = Field(None, alias='errorCode')
    error_desc: str | None = Field(None, alias='errorDesc')
    unit: str | None = Field(None, alias='unit')

    @classmethod
    def from_xml_element(cls, element: etree._Element) -> 'WSTranReject':
        """
        Parse a <value> XML element into a WSTranReject instance.

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
        return (
            f'WSTranReject('
            f'card={self.card_num}, '
            f'date={self.tran_date}, '
            f'error={self.error_code}: {self.error_desc})'
        )


class GetTranRejectsResponse(BaseModel):
    """
    Response model for getTranRejects operation.

    Contains an array of rejected transactions for the specified search criteria.
    """

    rejects: list[WSTranReject] = Field(default_factory=list)

    @classmethod
    def from_soap_response(cls, xml_string: str) -> 'GetTranRejectsResponse':
        """
        Parse a SOAP XML response into a GetTranRejectsResponse instance.

        Args:
            xml_string: The raw XML response from the SOAP API.

        Returns:
            A validated GetTranRejectsResponse with all parsed rejected transactions.

        Raises:
            RuntimeError: If the response contains a SOAP Fault.
            ValueError: If the response structure is invalid.

        Example:
            >>> from fuelsync import EfsClient
            >>> from fuelsync.models import WSTranRejectSearch
            >>> from fuelsync.response_models.getTranRejects_response import GetTranRejectsResponse
            >>> from datetime import datetime, timezone
            >>>
            >>> with EfsClient() as client:
            >>>     request = WSTranRejectSearch(
            >>>         start_date=datetime(2025, 11, 1, tzinfo=timezone.utc),
            >>>         end_date=datetime(2025, 11, 14, tzinfo=timezone.utc),
            >>>         card_num='1234567890'  # Optional filter
            >>>     )
            >>>     response = client.execute_operation(request)
            >>>     parsed = GetTranRejectsResponse.from_soap_response(response.text)
            >>>
            >>>     for reject in parsed.rejects:
            >>>         print(f'Card: {reject.card_num}')
            >>>         print(f'Error: {reject.error_desc}')
            >>>         print(f'Location: {reject.loc_name}')
        """
        logger.debug('Parsing SOAP response for GetTranRejectsResponse')

        # 1. Parse string to XML
        root: etree._Element = parse_soap_response(xml_string)

        # 2. Check for SOAP:Fault
        check_for_soap_fault(root)

        # 3. Get the <soap:Body>
        body: etree._Element = extract_soap_body(root)

        # 4. Find all rejected transaction elements
        rejects: list[WSTranReject] = []

        # Based on the schema, the repeating element is <value>
        result_elements = body.findall('.//value')

        if not result_elements:
            logger.warning('No <value> elements found in the response body.')
            # This isn't an error, just an empty list
            return cls(rejects=[])

        logger.info(f'Found {len(result_elements)} <value> elements to parse.')

        for reject_elem in result_elements:
            try:
                rejects.append(WSTranReject.from_xml_element(reject_elem))
            except Exception as e:
                # Log the error and the specific XML that failed
                failed_xml: str = etree.tostring(
                    reject_elem, pretty_print=True, encoding='unicode'
                )
                logger.error(
                    f'Failed to parse one reject <value> element: {e}\n'
                    f'--- Failing XML Snippet ---\n{failed_xml}\n'
                    f'--- End Snippet ---'
                )
                # Continue parsing other rejects

        logger.info(f'Successfully parsed {len(rejects)} rejected transactions.')
        return cls(rejects=rejects)

    def to_dataframe(self) -> pd.DataFrame:
        """
        Convert rejected transactions to a pandas DataFrame for analysis.

        Returns:
            DataFrame with one row per rejected transaction.

        Raises:
            ImportError: If pandas is not installed.

        Example:
            >>> response = GetTranRejectsResponse.from_soap_response(xml)
            >>> df = response.to_dataframe()
            >>> print(df[['card_num', 'error_desc', 'loc_name']])
        """
        return pd.DataFrame(
            [
                {
                    'tran_date': r.tran_date,
                    'card_num': r.card_num,
                    'invoice': r.invoice,
                    'loc_id': r.loc_id,
                    'loc_name': r.loc_name,
                    'loc_city': r.loc_city,
                    'loc_state': r.loc_state,
                    'error_code': r.error_code,
                    'error_desc': r.error_desc,
                    'unit': r.unit,
                }
                for r in self.rejects
            ]
        )

    @property
    def reject_count(self) -> int:
        """
        Number of rejected transactions in the response.

        Returns:
            Count of rejected transactions.
        """
        return len(self.rejects)

    def __repr__(self) -> str:
        return f'GetTranRejectsResponse(rejects={self.reject_count})'
