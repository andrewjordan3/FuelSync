# fuelsync/response_models/card_summary_response.py
"""
Pydantic models for parsing getCardSummaries SOAP API response.

This module defines the data structures for card summary records returned by
the EFS getCardSummaries operation. Each WSCardSummary represents a single
fleet card with its associated metadata (unit, driver, policy, status, etc.).
"""

import logging
from typing import Any

import pandas as pd
from lxml import etree  # pyright: ignore[reportAttributeAccessIssue]
from pydantic import BaseModel, ConfigDict, Field, field_validator

from ..utils import (
    check_for_soap_fault,
    extract_soap_body,
    parse_soap_response,
    parse_xml_to_dict,
)

logger: logging.Logger = logging.getLogger(__name__)


# --- Module-level constants ---

# Valid card status values per EFS API documentation
VALID_CARD_STATUSES: set[str] = {'Active', 'Hold', 'Fraud', 'Inactive'}

# Valid payroll status values
VALID_PAYROLL_STATUSES: set[str] = {'Active', 'Inactive', 'Follows'}

# Valid payroll use flags
VALID_PAYROLL_USE: set[str] = {'B', 'P', 'N'}

# Valid infosrc values (where prompt infos are defined)
VALID_INFOSRC: set[str] = {'POLICY', 'CARD', 'BOTH'}

# Mapping of model field names to DataFrame column names
CARD_FIELD_MAP: dict[str, str] = {
    'card_number': 'card_number',
    'policy_number': 'policy_number',
    'company_xref': 'company_xref',
    'unit_number': 'unit_number',
    'driver_id': 'driver_id',
    'driver_name': 'driver_name',
    'override': 'override',
    'being_overridden': 'being_overridden',
    'status': 'status',
    'payroll_status': 'payroll_status',
    'payroll_use': 'payroll_use',
    'gpsid': 'gpsid',
    'vin': 'vin',
    'zid': 'zid',
    'infosrc': 'infosrc',
    'policy_subfleet': 'policy_subfleet',
    'card_subfleet': 'card_subfleet',
}


class WSCardSummary(BaseModel):
    """
    Represents a single fleet card summary record.

    Contains metadata about a fleet card including its number, assigned unit/driver,
    status, policy information, and various identifiers used for GPS tracking and
    secure fuel operations.

    Attributes:
        card_number: The card number (up to 25 characters).
        policy_number: The policy number the card belongs to (1-99 are valid).
        company_xref: Company cross-reference identifier.
        unit_number: The unit/vehicle number assigned to the card.
        driver_id: The driver ID assigned to the card.
        driver_name: The driver name assigned to the card.
        override: Override flag (0 = no override, 1 = has override).
        being_overridden: Whether the card is currently being overridden.
        status: Card status (Active, Hold, Fraud, Inactive).
        payroll_status: Payroll status for SmartFunds/Universal cards
                       (Active, Inactive, Follows).
        payroll_use: Payroll usage type (B/P/N).
        gpsid: GPS ID for Secure Fuel cards.
        vin: Vehicle Identification Number for Secure Fuel.
        zid: Zone ID for Secure Fuel.
        infosrc: Where prompt infos are defined (POLICY, CARD, or BOTH).
        policy_subfleet: Subfleet ID assigned to the policy.
        card_subfleet: Subfleet ID assigned directly to the card.
    """

    model_config = ConfigDict(populate_by_name=True, extra='forbid')

    card_number: str = Field(
        ...,
        alias='cardNumber',
        max_length=25,
        description='The card number',
    )

    policy_number: int = Field(
        ...,
        alias='policyNumber',
        ge=1,
        le=99,
        description='The policy number (1-99)',
    )

    company_xref: str | None = Field(
        None,
        alias='companyXRef',
        max_length=15,
        description='Company cross-reference identifier',
    )

    unit_number: str | None = Field(
        None,
        alias='unitNumber',
        max_length=24,
        description='Unit/vehicle number assigned to the card',
    )

    driver_id: str | None = Field(
        None,
        alias='driverId',
        max_length=24,
        description='Driver ID assigned to the card',
    )

    driver_name: str | None = Field(
        None,
        alias='driverName',
        max_length=24,
        description='Driver name assigned to the card',
    )

    override: int = Field(
        ...,
        alias='override',
        ge=0,
        le=1,
        description='Override flag: 0 = no override, 1 = has override',
    )

    being_overridden: bool = Field(
        ...,
        alias='beingOverridden',
        description='Whether the card is currently being overridden',
    )

    status: str = Field(
        ...,
        alias='status',
        max_length=8,
        description='Card status: Active, Hold, Fraud, or Inactive',
    )

    payroll_status: str = Field(
        ...,
        alias='payrollStatus',
        max_length=8,
        description='Payroll status: Active, Inactive, or Follows (follows card status)',
    )

    payroll_use: str | None = Field(
        None,
        alias='payrollUse',
        max_length=1,
        description='Payroll usage: P (Payroll only), B (Both), N (Company only)',
    )

    gpsid: str | None = Field(
        None,
        alias='gpsid',
        max_length=10,
        description='GPS ID for Secure Fuel cards',
    )

    vin: str | None = Field(
        None,
        alias='vin',
        max_length=17,
        description='Vehicle Identification Number for Secure Fuel',
    )

    zid: str | None = Field(
        None,
        alias='zid',
        max_length=17,
        description='Zone ID for Secure Fuel',
    )

    infosrc: str | None = Field(
        None,
        alias='infosrc',
        max_length=6,
        description='Prompt info source: POLICY, CARD, or BOTH',
    )

    policy_subfleet: str | None = Field(
        None,
        alias='policySubfleet',
        max_length=24,
        description='Subfleet ID assigned to the policy',
    )

    card_subfleet: str | None = Field(
        None,
        alias='cardSubfleet',
        max_length=24,
        description='Subfleet ID assigned to the card',
    )

    @field_validator('status')
    @classmethod
    def validate_status(cls, value: str) -> str:
        """
        Validate that card status is one of the known valid values.

        Args:
            value: The status string to validate.

        Returns:
            The validated status value.

        Raises:
            ValueError: If status is not in VALID_CARD_STATUSES.
        """
        if value not in VALID_CARD_STATUSES:
            logger.warning(
                f"Unexpected card status '{value}'. "
                f'Expected one of {VALID_CARD_STATUSES}'
            )
        return value

    @field_validator('payroll_status')
    @classmethod
    def validate_payroll_status(cls, value: str) -> str:
        """
        Validate that payroll status is one of the known valid values.

        Args:
            value: The payroll status string to validate.

        Returns:
            The validated payroll status value.

        Raises:
            ValueError: If payroll status is not in VALID_PAYROLL_STATUSES.
        """
        if value not in VALID_PAYROLL_STATUSES:
            logger.warning(
                f"Unexpected payroll status '{value}'. "
                f'Expected one of {VALID_PAYROLL_STATUSES}'
            )
        return value

    @classmethod
    def from_xml_element(cls, element: etree._Element) -> 'WSCardSummary':
        """
        Parse a <value> XML element into a WSCardSummary instance.

        Uses the generic parse_xml_to_dict utility which automatically handles
        field extraction based on Pydantic field definitions.

        Args:
            element: The XML element to parse (typically a <value> element).

        Returns:
            A validated WSCardSummary instance.

        Raises:
            ValidationError: If the XML data fails Pydantic validation.
        """
        data: dict[str, Any] = parse_xml_to_dict(element, cls)
        return cls.model_validate(data)

    def __repr__(self) -> str:
        return (
            f'WSCardSummary('
            f'card_number={self.card_number}, '
            f'unit={self.unit_number}, '
            f'driver={self.driver_name}, '
            f'status={self.status})'
        )


class GetCardSummariesResponse(BaseModel):
    """
    Response model for getCardSummaries operation.

    Contains a list of card summary records returned by the EFS API,
    plus convenience methods for data transformation and analysis.

    Attributes:
        cards: List of WSCardSummary objects representing fleet cards.
    """

    cards: list[WSCardSummary] = Field(
        default_factory=list,
        description='List of card summary records',
    )

    @classmethod
    def from_soap_response(cls, xml_string: str) -> 'GetCardSummariesResponse':
        """
        Parse a SOAP XML response into a GetCardSummariesResponse instance.

        Args:
            xml_string: The raw XML response from the SOAP API.

        Returns:
            A validated GetCardSummariesResponse with all parsed card summaries.

        Raises:
            RuntimeError: If the response contains a SOAP Fault.
            ValueError: If the response structure is invalid.

        Example:
            >>> response = GetCardSummariesResponse.from_soap_response(xml)
            >>> active_cards = [c for c in response.cards if c.status == 'Active']
        """
        logger.debug('Parsing SOAP response for GetCardSummariesResponse')

        # Parse and validate SOAP envelope
        root: etree._Element = parse_soap_response(xml_string)
        check_for_soap_fault(root)
        body: etree._Element = extract_soap_body(root)

        cards: list[WSCardSummary] = []

        # Find the result element containing card data
        result_element: etree._Element = body.find('.//result')
        if result_element is None:
            logger.warning('No <result> element found in response body.')
            return cls(cards=[])

        # Get all card value elements
        card_elements: list[etree._Element] = result_element.findall('./value')

        if not card_elements:
            logger.warning('No <value> elements found in response body.')
            return cls(cards=[])

        logger.info(f'Found {len(card_elements)} card summary elements to parse.')

        # Parse each card element, continuing on individual failures
        for card_elem in card_elements:
            try:
                cards.append(WSCardSummary.from_xml_element(card_elem))
            except Exception as e:
                # Log the error and the specific XML that failed
                failed_xml: str = etree.tostring(
                    card_elem, pretty_print=True, encoding='unicode'
                )
                logger.error(
                    f'Failed to parse card summary <value> element: {e}\n'
                    f'--- Failing XML Snippet ---\n{failed_xml}\n'
                    f'--- End Snippet ---'
                )
                # Continue parsing other cards rather than failing entirely

        logger.info(f'Successfully parsed {len(cards)} card summaries.')
        return cls(cards=cards)

    def to_dataframe(self) -> pd.DataFrame:
        """
        Convert card summaries to a pandas DataFrame for analysis.

        Returns:
            DataFrame with one row per card. All fields are properly typed,
            with string columns for IDs and categorical data.

        Example:
            >>> response = GetCardSummariesResponse.from_soap_response(xml)
            >>> df = response.to_dataframe()
            >>> active_units = df[df['status'] == 'Active']['unit_number'].unique()
            >>> print(df[['card_number', 'unit_number', 'driver_name', 'status']])
        """
        if not self.cards:
            # Return empty DataFrame with proper schema
            return pd.DataFrame(columns=list(CARD_FIELD_MAP.values()))

        # Convert all cards to dictionaries using field mapping
        rows: list[dict[str, Any]] = []
        for card in self.cards:
            row: dict[str, Any] = {
                col_name: getattr(card, field_name)
                for field_name, col_name in CARD_FIELD_MAP.items()
            }
            rows.append(row)

        return pd.DataFrame(rows)

    @property
    def card_count(self) -> int:
        """
        Number of cards in the response.

        Returns:
            Count of card summary records.
        """
        return len(self.cards)

    @property
    def active_card_count(self) -> int:
        """
        Number of cards with 'Active' status.

        Returns:
            Count of active cards.
        """
        return sum(1 for card in self.cards if card.status == 'Active')

    @property
    def unit_numbers(self) -> list[str]:
        """
        List of all unique unit numbers (excluding None).

        Returns:
            Sorted list of unique unit numbers assigned to cards.
        """
        units: set[str] = {card.unit_number for card in self.cards if card.unit_number}
        return sorted(units)

    def __repr__(self) -> str:
        return (
            f'GetCardSummariesResponse('
            f'cards={self.card_count}, '
            f'active={self.active_card_count})'
        )
