# fuelsync/response_models/trans_ext_loc_response.py
"""
Pydantic models for parsing EFS SOAP API *responses*.

Each model represents a data structure returned by the EFS API.
They are responsible for parsing the XML response elements into
type-safe Python objects.
"""
# pyright: reportUnknownVariableType=false

import logging
import re
from datetime import datetime
from typing import Any, Self

import pandas as pd
from lxml import etree

# from lxml import ET
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

# Import your robust parser utilities
from fuelsync.utils import (
    check_for_soap_fault,
    extract_soap_body,
    parse_soap_response,
    parse_xml_to_dict,
)

logger: logging.Logger = logging.getLogger(__name__)


# --- Module-level constants ---
# Wex sends the string 'null'
_NULL_TOKEN_PATTERN: re.Pattern[str] = re.compile(
    r'^\s*(?:null|add\s*value)?\s*$',
    re.IGNORECASE,
)

DATETIME_COLUMNS: list[str] = ['transaction_date', 'pos_date']

FLOAT64_COLUMNS: list[str] = [
    'location_latitude',
    'location_longitude',
    'pref_total',
    'disc_amount',
    'carrier_fee',
    'conversion_rate',
    'quantity',
    'ppu',
    'retail_ppu',
    'line_amount',
    'retail_amount',
    'line_disc_amount',
    'fed_tax',
    'state_fuel_tax',
    'total_line_tax',
]

INT64_COLUMNS: list[str] = [
    'transaction_id',
    'contract_id',
    'location_country',
    'disc_type',
    'line_item_count',
    'line_number',
    'fuel_type',
    'use_type',
    'odometer',
    'class_code',
    'branch_code',
    'business_id',
    'driver_id',
]

STRING_COLUMNS: list[str] = [
    'card_number',
    'invoice',
    'auth_code',
    'ar_number',
    'location_name',
    'location_city',
    'location_state',
    'location_zip',
    'location_address',
    'billing_currency',
    'location_currency',
    'unit',
    'vehicle_type',
    'driver_name',
    'gl_code',
    'category',
    'fuel_type_name',
    'region',
]


# Define fuel type mapping once at the top of the file, after imports
FUEL_TYPE_MAP: dict[int, str] = {
    1: 'Diesel #1',
    2: 'Diesel #2',
    4: 'Unleaded Regular',
    8: 'Unleaded Midgrade',
    16: 'Unleaded Premium',
    32: 'Propane',
    128: 'Marked/Dyed Diesel',
    256: 'Bio Diesel',
    512: 'Car Diesel',
    1024: 'Agricultural Fuel',
    2048: 'Gasohol',
    4096: 'Natural Gas',
    8192: 'ULSD',
    16384: 'Aviation Fuel',
    65536: 'Unleaded Card Lock',
    2097152: 'Furnace Oil',
    8388608: 'CNG',
    16777216: 'LNG',
}

# Info fields we want to extract from the infos array
INFO_FIELDS: list[str] = [
    'UNIT',
    'VHTP',
    'ODRD',
    'NAME',
    'CLCD',
    'LCCD',
    'CNTN',
    'BLID',
    'DRID',
    'DMLC',
]

# Mapping of model field names to DataFrame column names
TRANSACTION_FIELD_MAP: dict[str, str] = {
    'transaction_id': 'transaction_id',
    'transaction_date': 'transaction_date',
    'pos_date': 'pos_date',
    'card_number': 'card_number',
    'invoice': 'invoice',
    'auth_code': 'auth_code',
    'ar_number': 'ar_number',
    'contract_id': 'contract_id',
    'location_name': 'location_name',
    'location_city': 'location_city',
    'location_state': 'location_state',
    'location_zip': 'location_zip',
    'location_address': 'location_address',
    'location_latitude': 'location_latitude',
    'location_longitude': 'location_longitude',
    'location_country': 'location_country',
    'pref_total': 'pref_total',
    'disc_amount': 'disc_amount',
    'carrier_fee': 'carrier_fee',
    'billing_currency': 'billing_currency',
    'location_currency': 'location_currency',
    'conversion_rate': 'conversion_rate',
    'hand_entered': 'hand_entered',
    'override': 'override',
    'disc_type': 'disc_type',
}

INFO_FIELD_MAP: dict[str, str] = {
    'UNIT': 'unit',
    'VHTP': 'vehicle_type',
    'ODRD': 'odometer',
    'NAME': 'driver_name',
    'CLCD': 'class_code',
    'LCCD': 'branch_code',
    'CNTN': 'gl_code',
    'BLID': 'business_id',
    'DRID': 'driver_id',
    'DMLC': 'region',
}

LINE_ITEM_FIELD_MAP: dict[str, str] = {
    'line_number': 'line_number',
    'category': 'category',
    'fuel_type': 'fuel_type',
    'use_type': 'use_type',
    'quantity': 'quantity',
    'ppu': 'ppu',
    'retail_ppu': 'retail_ppu',
    'amount': 'line_amount',
    'retail_amount': 'retail_amount',
    'disc_amount': 'line_disc_amount',
}


# --- Utility Functions ---
def _normalize_null_like_value(raw_value: Any) -> Any:
    """
    Normalize common null-like API values.

    Treats:
      - None -> None
      - '' / whitespace -> None
      - 'null' (any case, surrounding whitespace) -> None

    Args:
        raw_value: Incoming raw value from API (often str|None|int|float).

    Returns:
        None if the value represents missingness; otherwise returns raw_value unchanged.
    """
    if raw_value is None:
        return None
    if isinstance(raw_value, str) and _NULL_TOKEN_PATTERN.match(raw_value):
        return None
    return raw_value


def _coerce_optional_int(raw_value: Any) -> int | None:
    """
    Coerce an incoming value to an optional int using structural pattern matching.

    Accepts strings like '047078' and returns 47078. Treats null-like tokens as None.
    Explicitly rejects booleans and non-integer floats to prevent data silent corruption.

    Args:
        raw_value: The raw data input (typically from an API or CSV).

    Returns:
        The parsed integer or None if the input is null-like or invalid.

    Side Effects:
        Emits a logger.warning for inputs that are not null-like but fail coercion.
    """
    normalized_value: Any = _normalize_null_like_value(raw_value)
    result: int | None = None

    match normalized_value:
        case None:
            result = None

        case bool():
            logger.warning(
                'Expected int but got bool (%r). Returning None', normalized_value
            )
            result = None

        case int():
            result = normalized_value

        case float() if normalized_value.is_integer():
            result = int(normalized_value)

        case float():
            logger.warning(
                'Expected int but got non-integer float (%r). Returning None',
                normalized_value,
            )
            result = None

        case str():
            stripped_value: str = normalized_value.strip()
            try:
                result = int(stripped_value)
            except ValueError:
                logger.warning(
                    'Expected numeric string but got %r. Returning None',
                    normalized_value,
                )
                result = None

        case _:
            logger.warning(
                'Unsupported type for int coercion: %s (Value: %r)',
                type(normalized_value).__name__,
                normalized_value,
            )
            result = None

    return result  # Fallback, should not reach here due to logging above


def _coerce_optional_float(raw_value: Any) -> float | None:
    """
    Coerce an incoming value to an optional float.

    Treats null-like tokens as None.

    Args:
        raw_value: Raw value from API.

    Returns:
        Parsed float, or None.

    Side Effects:
        Emits logger.warning on invalid, non-missing inputs.
    """
    normalized_value: Any = _normalize_null_like_value(raw_value)
    if normalized_value is None:
        return None

    if isinstance(normalized_value, bool):
        logger.warning('Expected float but got bool. Returning None')
        return None

    if isinstance(normalized_value, (int, float)):
        return float(normalized_value)

    if isinstance(normalized_value, str):
        stripped_value: str = normalized_value.strip()
        try:
            return float(stripped_value)
        except ValueError:
            logger.warning('Expected float but got non-numeric string. Returning None')
            return None

    logger.warning(
        'Unsupported type for float coercion: %r', type(normalized_value).__name__
    )
    return None  # Fallback, should not reach here due to logging above


def _coerce_dataframe_schema(transaction_dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Coerce DataFrame columns to the canonical FuelSync schema dtypes.

    Args:
        transaction_dataframe: DataFrame produced by to_dataframe() before dtype normalization.

    Returns:
        A new DataFrame with dtype conversions applied (does not mutate input).

    Side Effects:
        None.

    Raises:
        None. (Uses safe coercion; unexpected values become missing.)
    """
    coerced_dataframe: pd.DataFrame = transaction_dataframe.copy()

    # Apply datetime conversion to columns
    for column_name in DATETIME_COLUMNS:
        if column_name in coerced_dataframe.columns:
            coerced_dataframe[column_name] = pd.to_datetime(
                coerced_dataframe[column_name], utc=True
            )

    for column_name in FLOAT64_COLUMNS:
        if column_name in coerced_dataframe.columns:
            coerced_dataframe[column_name] = pd.to_numeric(
                coerced_dataframe[column_name], errors='coerce'
            ).astype(pd.Float64Dtype())

    for column_name in INT64_COLUMNS:
        if column_name in coerced_dataframe.columns:
            coerced_dataframe[column_name] = pd.to_numeric(
                coerced_dataframe[column_name], errors='coerce'
            ).astype(pd.Int64Dtype())

    for column_name in STRING_COLUMNS:
        if column_name in coerced_dataframe.columns:
            coerced_dataframe[column_name] = coerced_dataframe[column_name].astype(
                pd.StringDtype()
            )

    return coerced_dataframe


# --- Nested Response Models ---


class WSTransactionCarmsStmt(BaseModel):
    """
    CARMS statement data.

    Contains the statement ID associated with a CARMS (Card Account Reconciliation
    and Management System) transaction.
    """

    model_config = ConfigDict(populate_by_name=True)

    statement_id: str | None = Field(None, alias='statementId')

    def __repr__(self) -> str:
        return f'WSTransactionCarmsStmt(statement_id={self.statement_id})'


class WSFleetMemo(BaseModel):
    """
    Fleet memo associated with a transaction.

    Contains detailed information about fleet card transactions, including
    merchant information, ATM details, and posting amounts.
    """

    model_config = ConfigDict(populate_by_name=True)

    card_number: str | None = Field(None, alias='cardNumber')
    acceptor_name: str | None = Field(None, alias='acceptorName')
    auth_code: str | None = Field(None, alias='authCode')
    tch_memo_date: datetime | None = Field(None, alias='tchMemoDate')
    amount: int | None = Field(None, alias='amount')
    contract_id: int | None = Field(None, alias='contractId')
    msg_num_type: int | None = Field(None, alias='msgNumType')
    memo_type: int | None = Field(None, alias='type')
    bin: int | None = Field(None, alias='bin')
    mc_card_num: str | None = Field(None, alias='mcCardNum')
    posted_amount: int | None = Field(None, alias='postedAmount')
    atm_fee: int | None = Field(None, alias='atmFee')
    pin_sent: int | None = Field(None, alias='pinSent')
    atm_id: str | None = Field(None, alias='atmId')
    merch_type: int | None = Field(None, alias='merchType')
    stan: int | None = Field(None, alias='stan')
    entry_mode: int | None = Field(None, alias='entryMode')
    never_cleared_date: datetime | None = Field(None, alias='neverClearedDate')
    merc_name: str | None = Field(None, alias='mercName')
    merc_addr: str | None = Field(None, alias='mercAddr')
    merc_zip: str | None = Field(None, alias='mercZip')
    merc_phone: str | None = Field(None, alias='mercPhone')
    merc_state: str | None = Field(None, alias='mercState')
    merc_cat: str | None = Field(None, alias='mercCat')
    merc_city: str | None = Field(None, alias='mercCity')

    def __repr__(self) -> str:
        return (
            f'WSFleetMemo('
            f'card_number={self.card_number}, '
            f'merchant={self.merc_name}, '
            f'amount={self.amount})'
        )


class WSTransactionInfo(BaseModel):
    """
    Represents a simple key-value pair for transaction info.

    Example XML:
        <infos><type>UNIT</type><value>404706</value></infos>
    """

    model_config = ConfigDict(populate_by_name=True)

    info_type: str | None = Field(None, alias='type')
    info_value: str | None = Field(None, alias='value')

    def __repr__(self) -> str:
        return f'WSTransactionInfo(type={self.info_type}, value={self.info_value})'


class ExtractedInfoFields(BaseModel):
    """
    Typed container for info fields extracted from WSTransactionInfo list.

    Applies proper type coercion to fields that should be numeric,
    ensuring 'null' strings and whitespace are converted to None.
    """

    model_config = ConfigDict(extra='ignore')

    unit: str | None = None
    vehicle_type: str | None = None
    odometer: int | None = None
    driver_name: str | None = None
    class_code: int | None = None
    branch_code: int | None = None
    gl_code: str | None = None
    business_id: int | None = None
    driver_id: int | None = None
    region: str | None = None

    @field_validator(
        'odometer',
        'class_code',
        'branch_code',
        'business_id',
        'driver_id',
        mode='before',
    )
    @classmethod
    def _coerce_int_fields(cls, raw_value: Any) -> int | None:
        return _coerce_optional_int(raw_value)

    @field_validator(
        'unit', 'vehicle_type', 'driver_name', 'gl_code', 'region', mode='before'
    )
    @classmethod
    def _normalize_string_fields(cls, raw_value: Any) -> str | None:
        normalized: Any = _normalize_null_like_value(raw_value)
        if normalized is None:
            return None
        return str(normalized).strip() or None

    @classmethod
    def from_info_list(cls, infos: list[WSTransactionInfo]) -> Self:
        """
        Build typed info fields from raw WSTransactionInfo list.

        Args:
            infos: List of key-value info pairs from transaction.

        Returns:
            ExtractedInfoFields with proper type coercion applied.
        """
        # Build lookup dict: API field code -> raw value
        raw_lookup: dict[str, str | None] = {
            info.info_type: info.info_value
            for info in infos
            if info.info_type is not None
        }

        # Map API codes to model field names
        field_data: dict[str, Any] = {
            'unit': raw_lookup.get('UNIT'),
            'vehicle_type': raw_lookup.get('VHTP'),
            'odometer': raw_lookup.get('ODRD'),
            'driver_name': raw_lookup.get('NAME'),
            'class_code': raw_lookup.get('CLCD'),
            'branch_code': raw_lookup.get('LCCD'),
            'gl_code': raw_lookup.get('CNTN'),
            'business_id': raw_lookup.get('BLID'),
            'driver_id': raw_lookup.get('DRID'),
            'region': raw_lookup.get('DMLC'),
        }

        return cls.model_validate(field_data)


class WSTransTaxes(BaseModel):
    """
    Represents a single tax line item within a main line item.

    Example XML:
        <lineTaxes>
            <taxDescription>SFTX</taxDescription>
            <grossNetFlag>N</grossNetFlag>
            <amount>16.58</amount>
            ...
        </lineTaxes>
    """

    model_config = ConfigDict(populate_by_name=True)

    tax_description: str | None = Field(None, alias='taxDescription')
    gross_net_flag: str | None = Field(None, alias='grossNetFlag')
    amount: float | None = Field(None, alias='amount')
    tax_code: str | None = Field(None, alias='taxCode')
    exempt_flag: str | None = Field(None, alias='exemptFlag')
    pst_exempt_adjust: float | None = Field(None, alias='pstExemptAdjust')
    gst_exempt_adjust: float | None = Field(None, alias='gstExemptAdjust')

    def __repr__(self) -> str:
        return f'WSTransTaxes(description={self.tax_description}, amount={self.amount})'


class WSTransactionLineItemExt(BaseModel):
    """
    Represents a single line item (e.g., ULSD fuel, DEF fluid).

    Example XML:
        <lineItems>
            <amount>332.11</amount>
            <category>ULSD</category>
            <quantity>44.68</quantity>
            ...
        </lineItems>
    """

    model_config = ConfigDict(populate_by_name=True)

    amount: float | None = Field(None, alias='amount')
    billing_flag: int | None = Field(None, alias='billingFlag')
    category: str | None = Field(None, alias='category')
    cmpt_amount: float | None = Field(None, alias='cmptAmount')
    cmpt_ppu: float | None = Field(None, alias='cmptPPU')
    disc_amount: float | None = Field(None, alias='discAmount')
    fuel_type: int | None = Field(None, alias='fuelType')
    group_category: str | None = Field(None, alias='groupCategory')
    group_number: int | None = Field(None, alias='groupNumber')
    issuer_deal: float | None = Field(None, alias='issuerDeal')
    issuer_deal_ppu: float | None = Field(None, alias='issuerDealPPU')
    line_number: int | None = Field(None, alias='lineNumber')
    number: int | None = Field(None, alias='number')
    ppu: float | None = Field(None, alias='ppu')
    quantity: float | None = Field(None, alias='quantity')
    retail_ppu: float | None = Field(None, alias='retailPPU')
    retail_amount: float | None = Field(None, alias='retailAmount')
    service_type: int | None = Field(None, alias='serviceType')
    use_type: int | None = Field(None, alias='useType')

    line_taxes: list[WSTransTaxes] = Field(default_factory=list, alias='lineTaxes')

    @field_validator(
        'billing_flag',
        'fuel_type',
        'group_number',
        'line_number',
        'number',
        'service_type',
        'use_type',
        mode='before',
    )
    @classmethod
    def _coerce_int(cls, raw_value: Any) -> int | None:
        return _coerce_optional_int(raw_value)

    def __repr__(self) -> str:
        return (
            f'WSTransactionLineItemExt('
            f'category={self.category}, '
            f'quantity={self.quantity}, '
            f'amount={self.amount})'
        )


class WSMetaData(BaseModel):
    """
    Represents a metadata key-value pair.

    Example XML:
        <metaData>
            <typeId>LocTerminalId</typeId>
            <metaData>PUMP1020</metaData>
            <description>Location Terminal ID</description>
        </metaData>
    """

    model_config = ConfigDict(populate_by_name=True)

    type_id: str | None = Field(None, alias='typeId')
    meta_data: str | None = Field(None, alias='metaData')
    description: str | None = Field(None, alias='description')

    def __repr__(self) -> str:
        return f'WSMetaData(type={self.type_id}, data={self.meta_data})'


# --- Main Transaction Model ---


class WSMCTransExtLocV2(BaseModel):
    """
    Extended transaction with location data (Version 2).

    Represents a single transaction returned by the getMCTransExtLocV2 operation.
    This model parses the content of a <value> tag.
    """

    model_config = ConfigDict(populate_by_name=True, extra='ignore')

    # If the API fails to provide a transactionId, the record is corrupt.
    transaction_id: int = Field(..., alias='transactionId')

    # The rest are optional
    ar_batch_number: int | None = Field(None, alias='ARBatchNumber')
    cpnr_delivery_tp: str | None = Field(None, alias='CPNRDeliveryTP')
    mc_multi_currency: bool | None = Field(None, alias='MCMultiCurrency')
    op_data_source: str | None = Field(None, alias='OPDataSource')
    pos_date: datetime | None = Field(None, alias='POSDate')
    account_type: str | None = Field(None, alias='accountType')
    ar_number: str | None = Field(None, alias='arNumber')
    auth_code: str | None = Field(None, alias='authCode')
    billing_country: int | None = Field(None, alias='billingCountry')
    billing_currency: str | None = Field(None, alias='billingCurrency')
    card_number: str | None = Field(None, alias='cardNumber')
    carms_statements: list[WSTransactionCarmsStmt] = Field(
        default_factory=list, alias='carmsStatements'
    )
    carrier_fee: float | None = Field(None, alias='carrierFee')
    carrier_id: int | None = Field(None, alias='carrierId')
    company_x_ref: str | None = Field(None, alias='companyXRef')
    contract_id: int | None = Field(None, alias='contractId')
    conversion_rate: float | None = Field(None, alias='conversionRate')
    country: int | None = Field(None, alias='country')
    disc_amount: float | None = Field(None, alias='discAmount')
    disc_type: int | None = Field(None, alias='discType')
    fleet_memo: WSFleetMemo | None = Field(None, alias='fleetMemo')
    funded_total: float | None = Field(None, alias='fundedTotal')
    hand_entered: bool | None = Field(None, alias='handEntered')
    in_address: str | None = Field(None, alias='inAddress')
    infos: list[WSTransactionInfo] = Field(default_factory=list, alias='infos')
    invoice: str | None = Field(None, alias='invoice')
    issuer_deal: float | None = Field(None, alias='issuerDeal')
    issuer_fee: float | None = Field(None, alias='issuerFee')
    issuer_id: int | None = Field(None, alias='issuerId')
    language: int | None = Field(None, alias='language')
    line_items: list[WSTransactionLineItemExt] = Field(
        default_factory=list, alias='lineItems'
    )
    location_chain_id: int | None = Field(None, alias='locationChainId')
    location_country: int | None = Field(None, alias='locationCountry')
    location_currency: str | None = Field(None, alias='locationCurrency')
    location_id: int | None = Field(None, alias='locationId')
    location_name: str | None = Field(None, alias='locationName')
    location_city: str | None = Field(None, alias='locationCity')
    location_state: str | None = Field(None, alias='locationState')
    location_zip: str | None = Field(None, alias='locationZip')
    location_address: str | None = Field(None, alias='locationAddress')
    location_longitude: float | None = Field(None, alias='locationLongitude')
    location_latitude: float | None = Field(None, alias='locationLatitude')
    message_dlvd: str | None = Field(None, alias='messageDLVD')
    meta_data: list[WSMetaData] = Field(default_factory=list, alias='metaData')
    net_total: float | None = Field(None, alias='netTotal')
    non_area_fee: float | None = Field(None, alias='nonAreaFee')
    override: bool | None = Field(None, alias='override')
    original_trans_id: int | None = Field(None, alias='originalTransId')
    post_disc_tax: float | None = Field(None, alias='postDiscTax')
    pre_disc_tax: float | None = Field(None, alias='preDiscTax')
    pref_total: float | None = Field(None, alias='prefTotal')
    reported_carrier: datetime | None = Field(None, alias='reportedCarrier')
    settle_amount: float | None = Field(None, alias='settleAmount')
    settle_id: int | None = Field(None, alias='settleId')
    split_billing: bool | None = Field(None, alias='splitBilling')
    statement_id: int | None = Field(None, alias='statementId')
    supplier_id: int | None = Field(None, alias='supplierId')
    supr_fee: float | None = Field(None, alias='suprFee')
    tax_exempt_amount: float | None = Field(None, alias='taxExemptAmount')
    terminal_id: str | None = Field(None, alias='terminalId')
    terminal_type: str | None = Field(None, alias='terminalType')
    trans_reported: datetime | None = Field(None, alias='transReported')
    transaction_date: datetime | None = Field(None, alias='transactionDate')
    transaction_type: int | None = Field(None, alias='transactionType')
    trans_taxes: list[WSTransTaxes] = Field(default_factory=list, alias='transTaxes')

    @field_validator('location_latitude', 'location_longitude', mode='before')
    @classmethod
    def _coerce_float(cls, raw_value: Any) -> float | None:
        return _coerce_optional_float(raw_value)

    @field_validator(
        'ar_batch_number',
        'billing_country',
        'carrier_id',
        'country',
        'issuer_id',
        'location_chain_id',
        'location_country',
        'location_id',
        'original_trans_id',
        'settle_id',
        'statement_id',
        'supplier_id',
        mode='before',
    )
    @classmethod
    def _coerce_int(cls, raw_value: Any) -> int | None:
        return _coerce_optional_int(raw_value)

    @classmethod
    def from_xml_element(cls, element: etree.Element) -> Self:
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
            logger.error('Failed to validate parsed XML data: %r\nData: %r', e, data)
            raise ValueError(f'Pydantic validation failed for transaction: {e}') from e

    def __repr__(self) -> str:
        return (
            f'WSMCTransExtLocV2('
            f'transaction_id={self.transaction_id}, '
            f'card_number={self.card_number}, '
            f'net_total={self.net_total}, '
            f'location={self.location_name})'
        )


# --- Response Container ---


class GetMCTransExtLocV2Response(BaseModel):
    """
    Response model for getMCTransExtLocV2 operation.

    Contains an array of transactions for the requested date range,
    plus convenience methods for common operations.
    """

    transactions: list[WSMCTransExtLocV2] = Field(default_factory=list)

    @classmethod
    def from_soap_response(cls, xml_string: str) -> 'GetMCTransExtLocV2Response':
        """
        Parse a SOAP XML response into a GetMCTransExtLocV2Response instance.

        Args:
            xml_string: The raw XML response from the SOAP API.

        Returns:
            A validated GetMCTransExtLocV2Response with all parsed transactions.

        Raises:
            RuntimeError: If the response contains a SOAP Fault.
            ValueError: If the response structure is invalid.
        """
        logger.debug('Parsing SOAP response for GetMCTransExtLocV2Response')

        # 1. Parse string to XML
        root: etree.Element = parse_soap_response(xml_string)

        # 2. Check for SOAP:Fault
        check_for_soap_fault(root)

        # 3. Get the <soap:Body>
        body: etree.Element = extract_soap_body(root)

        # 4. Find all transaction elements and parse them
        transactions: list[WSMCTransExtLocV2] = []

        # Based on the XML structure, the repeating transaction element is <value>
        # First find the result element, then get its direct value children
        result_element: etree.Element | None = body.find('.//result')
        if result_element is None:
            logger.warning('No <result> element found in the response body.')
            return cls(transactions=[])

        # Get only the direct children named 'value'
        result_elements: list[etree.Element] = result_element.findall('./value')

        if not result_elements:
            logger.warning('No <value> elements found in the response body.')
            # This isn't an error, just an empty list
            return cls(transactions=[])

        logger.info('Found %d <value> elements to parse.', len(result_elements))

        for trans_elem in result_elements:
            try:
                transactions.append(WSMCTransExtLocV2.from_xml_element(trans_elem))
            except Exception as e:
                # Log the error and the specific XML that failed
                failed_xml = etree.tostring(
                    trans_elem, pretty_print=True, encoding='unicode'
                )
                logger.error(
                    'Failed to parse one transaction <value> element: %r\n'
                    '--- Failing XML Snippet ---\n%s\n'
                    '--- End Snippet ---',
                    e,
                    failed_xml,
                )
                # Continue parsing other transactions

        logger.info('Successfully parsed %d transactions.', len(transactions))
        return cls(transactions=transactions)

    def to_dataframe(self) -> pd.DataFrame:
        """
        Convert transactions to a pandas DataFrame for analysis.

        Creates one row per line item, with transaction-level data duplicated
        across line items within the same transaction. This structure is optimized
        for fuel fraud detection and per-product analysis.

        Returns:
            DataFrame with one row per line item. Datetime columns are properly
            typed as pandas datetime. Fuel types are decoded to readable names.

        Example:
            >>> response = GetMCTransExtLocV2Response.from_soap_response(xml)
            >>> df = response.to_dataframe()
            >>> fuel_only = df[df['use_type'] == 1]  # Filter to fuel purchases
            >>> print(df[['transaction_id', 'unit', 'category', 'quantity']])
        """
        rows: list[dict[str, Any]] = []

        for t in self.transactions:
            # Build transaction-level data once
            trans_data: dict[str, Any] = {
                col_name: getattr(t, field_name)
                for field_name, col_name in TRANSACTION_FIELD_MAP.items()
            }

            # Extract and type-coerce info fields via Pydantic model
            extracted_info: ExtractedInfoFields = ExtractedInfoFields.from_info_list(
                t.infos
            )
            trans_data.update(extracted_info.model_dump())

            # Add line item count
            trans_data['line_item_count'] = len(t.line_items)

            # If no line items, create single row with None for line item fields
            if not t.line_items:
                row: dict[str, Any] = trans_data.copy()
                row.update(dict.fromkeys(LINE_ITEM_FIELD_MAP.values()))
                row.update(
                    {
                        'fuel_type_name': None,
                        'fed_tax': None,
                        'state_fuel_tax': None,
                        'total_line_tax': None,
                    }
                )
                rows.append(row)
            else:
                # Create one row per line item
                for line_item in t.line_items:
                    row = trans_data.copy()

                    # Add line item fields
                    row.update(
                        {
                            col_name: getattr(line_item, field_name)
                            for field_name, col_name in LINE_ITEM_FIELD_MAP.items()
                        }
                    )

                    # Extract tax information
                    fed_tax: float | None = None
                    state_fuel_tax: float | None = None
                    total_line_tax: float = 0.0

                    for tax in line_item.line_taxes:
                        if tax.amount:
                            total_line_tax += tax.amount
                        if tax.tax_code == 'FED':
                            fed_tax = tax.amount
                        elif tax.tax_code == 'SFTX':
                            state_fuel_tax = tax.amount

                    row['fed_tax'] = fed_tax
                    row['state_fuel_tax'] = state_fuel_tax
                    row['total_line_tax'] = (
                        total_line_tax if total_line_tax > 0 else None
                    )

                    rows.append(row)

        # Build DataFrame
        df = pd.DataFrame(rows)

        # Apply fuel type name mapping to column
        if 'fuel_type' in df.columns:
            df['fuel_type_name'] = df['fuel_type'].map(FUEL_TYPE_MAP)

        # Normalize DataFrame schema to canonical FuelSync types
        df: pd.DataFrame = _coerce_dataframe_schema(df)

        return df

    @property
    def total_amount(self) -> float:
        """
        Sum of all transaction net_total amounts.

        Returns:
            Total dollar amount across all transactions.
        """
        return sum(t.net_total or 0.0 for t in self.transactions)

    @property
    def transaction_count(self) -> int:
        """
        Number of transactions in the response.

        Returns:
            Count of transactions.
        """
        return len(self.transactions)

    def __repr__(self) -> str:
        return (
            f'GetMCTransExtLocV2Response('
            f'transactions={self.transaction_count}, '
            f'total_amount=${self.total_amount:,.2f})'
        )
