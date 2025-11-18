# fuelsync/response_models/TransExtLocV2_response.py
"""
Pydantic models for parsing EFS SOAP API *responses*.

Each model represents a data structure returned by the EFS API.
They are responsible for parsing the XML response elements into
type-safe Python objects.
"""

# pyright: reportUnknownVariableType=false, reportUnknownMemberType=false, reportAttributeAccessIssue=false, reportUnknownParameterType=false, reportUnknownArgumentType=false
import logging
from datetime import datetime
from typing import Any

import pandas as pd
from lxml import etree

# from lxml import ET
from pydantic import BaseModel, ConfigDict, Field, ValidationError

# Import your robust parser utilities
from ..utils import (
    check_for_soap_fault,
    extract_soap_body,
    is_nil,
    parse_soap_response,
    safe_convert,
)

logger: logging.Logger = logging.getLogger(__name__)


# --- Module-level constants ---
# Datetime columns for conversion
datetime_cols: list[str] = ['transaction_date', 'pos_date']

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

# --- Nested Response Models ---


class WSTransactionCarmsStmt(BaseModel):
    """
    CARMS statement data.

    Contains the statement ID associated with a CARMS (Card Account Reconciliation
    and Management System) transaction.
    """

    model_config = ConfigDict(populate_by_name=True)

    statement_id: str | None = Field(None, alias='statementId')

    @classmethod
    def from_xml_element(cls, element: etree._Element) -> 'WSTransactionCarmsStmt':
        """Parse a <carmsStatements> XML element into a WSTransactionCarmsStmt instance."""
        data: dict[str, int | float | bool | datetime | str | None] = {
            'statementId': safe_convert(element, 'statementId', str),
        }
        return cls.model_validate(data)

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

    @classmethod
    def from_xml_element(cls, element: etree._Element) -> 'WSFleetMemo':
        """Parse a <fleetMemo> XML element into a WSFleetMemo instance."""
        data: dict[str, int | float | bool | datetime | str | None] = {
            'cardNumber': safe_convert(element, 'cardNumber', str),
            'acceptorName': safe_convert(element, 'acceptorName', str),
            'authCode': safe_convert(element, 'authCode', str),
            'tchMemoDate': safe_convert(element, 'tchMemoDate', datetime),
            'amount': safe_convert(element, 'amount', int),
            'contractId': safe_convert(element, 'contractId', int),
            'msgNumType': safe_convert(element, 'msgNumType', int),
            'type': safe_convert(element, 'type', int),
            'bin': safe_convert(element, 'bin', int),
            'mcCardNum': safe_convert(element, 'mcCardNum', str),
            'postedAmount': safe_convert(element, 'postedAmount', int),
            'atmFee': safe_convert(element, 'atmFee', int),
            'pinSent': safe_convert(element, 'pinSent', int),
            'atmId': safe_convert(element, 'atmId', str),
            'merchType': safe_convert(element, 'merchType', int),
            'stan': safe_convert(element, 'stan', int),
            'entryMode': safe_convert(element, 'entryMode', int),
            'neverClearedDate': safe_convert(element, 'neverClearedDate', datetime),
            'mercName': safe_convert(element, 'mercName', str),
            'mercAddr': safe_convert(element, 'mercAddr', str),
            'mercZip': safe_convert(element, 'mercZip', str),
            'mercPhone': safe_convert(element, 'mercPhone', str),
            'mercState': safe_convert(element, 'mercState', str),
            'mercCat': safe_convert(element, 'mercCat', str),
            'mercCity': safe_convert(element, 'mercCity', str),
        }
        return cls.model_validate(data)

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

    @classmethod
    def from_xml_element(cls, element: etree._Element) -> 'WSTransactionInfo':
        data: dict[str, int | float | bool | datetime | str | None] = {
            'type': safe_convert(element, 'type', str),
            'value': safe_convert(element, 'value', str),
        }
        return cls.model_validate(data)

    def __repr__(self) -> str:
        return f'WSTransactionInfo(type={self.info_type}, value={self.info_value})'


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

    @classmethod
    def from_xml_element(cls, element: etree._Element) -> 'WSTransTaxes':
        data: dict[str, int | float | bool | datetime | str | None] = {
            'taxDescription': safe_convert(element, 'taxDescription', str),
            'grossNetFlag': safe_convert(element, 'grossNetFlag', str),
            'amount': safe_convert(element, 'amount', float),
            'taxCode': safe_convert(element, 'taxCode', str),
            'exemptFlag': safe_convert(element, 'exemptFlag', str),
            'pstExemptAdjust': safe_convert(element, 'pstExemptAdjust', float),
            'gstExemptAdjust': safe_convert(element, 'gstExemptAdjust', float),
        }
        return cls.model_validate(data)

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

    @classmethod
    def from_xml_element(cls, element: etree._Element) -> 'WSTransactionLineItemExt':
        data: dict[str, Any] = {
            'amount': safe_convert(element, 'amount', float),
            'billingFlag': safe_convert(element, 'billingFlag', int),
            'category': safe_convert(element, 'category', str),
            'cmptAmount': safe_convert(element, 'cmptAmount', float),
            'cmptPPU': safe_convert(element, 'cmptPPU', float),
            'discAmount': safe_convert(element, 'discAmount', float),
            'fuelType': safe_convert(element, 'fuelType', int),
            'groupCategory': safe_convert(element, 'groupCategory', str),
            'groupNumber': safe_convert(element, 'groupNumber', int),
            'issuerDeal': safe_convert(element, 'issuerDeal', float),
            'issuerDealPPU': safe_convert(element, 'issuerDealPPU', float),
            'lineNumber': safe_convert(element, 'lineNumber', int),
            'number': safe_convert(element, 'number', int),
            'ppu': safe_convert(element, 'ppu', float),
            'quantity': safe_convert(element, 'quantity', float),
            'retailPPU': safe_convert(element, 'retailPPU', float),
            'retailAmount': safe_convert(element, 'retailAmount', float),
            'serviceType': safe_convert(element, 'serviceType', int),
            'useType': safe_convert(element, 'useType', int),
        }

        # Parse nested list of lineTaxes
        data['lineTaxes'] = [
            WSTransTaxes.from_xml_element(item) for item in element.findall('lineTaxes')
        ]

        return cls.model_validate(data)

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

    @classmethod
    def from_xml_element(cls, element: etree._Element) -> 'WSMetaData':
        data: dict[str, int | float | bool | datetime | str | None] = {
            'typeId': safe_convert(element, 'typeId', str),
            'metaData': safe_convert(element, 'metaData', str),
            'description': safe_convert(element, 'description', str),
        }
        return cls.model_validate(data)

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
    location_longitude: str | None = Field(None, alias='locationLongitude')
    location_latitude: str | None = Field(None, alias='locationLatitude')
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
    transaction_id: int | None = Field(None, alias='transactionId')
    transaction_type: int | None = Field(None, alias='transactionType')
    trans_taxes: list[WSTransTaxes] = Field(default_factory=list, alias='transTaxes')

    @classmethod
    def from_xml_element(cls, element: etree._Element) -> 'WSMCTransExtLocV2':
        """
        Parse a <value> XML element into a WSMCTransExtLocV2 instance.

        This is an explicit parser that handles all fields individually.
        """
        data: dict[str, Any] = {}

        # --- Simple Types ---
        data['ARBatchNumber'] = safe_convert(element, 'ARBatchNumber', int)
        data['CPNRDeliveryTP'] = safe_convert(element, 'CPNRDeliveryTP', str)
        data['MCMultiCurrency'] = safe_convert(element, 'MCMultiCurrency', bool)
        data['OPDataSource'] = safe_convert(element, 'OPDataSource', str)
        data['POSDate'] = safe_convert(element, 'POSDate', datetime)
        data['accountType'] = safe_convert(element, 'accountType', str)
        data['arNumber'] = safe_convert(element, 'arNumber', str)
        data['authCode'] = safe_convert(element, 'authCode', str)
        data['billingCountry'] = safe_convert(element, 'billingCountry', int)
        data['billingCurrency'] = safe_convert(element, 'billingCurrency', str)
        data['cardNumber'] = safe_convert(element, 'cardNumber', str)
        data['carrierFee'] = safe_convert(element, 'carrierFee', float)
        data['carrierId'] = safe_convert(element, 'carrierId', int)
        data['companyXRef'] = safe_convert(element, 'companyXRef', str)
        data['contractId'] = safe_convert(element, 'contractId', int)
        data['conversionRate'] = safe_convert(element, 'conversionRate', float)
        data['country'] = safe_convert(element, 'country', int)
        data['discAmount'] = safe_convert(element, 'discAmount', float)
        data['discType'] = safe_convert(element, 'discType', int)
        data['fundedTotal'] = safe_convert(element, 'fundedTotal', float)
        data['handEntered'] = safe_convert(element, 'handEntered', bool)
        data['inAddress'] = safe_convert(element, 'inAddress', str)
        data['invoice'] = safe_convert(element, 'invoice', str)
        data['issuerDeal'] = safe_convert(element, 'issuerDeal', float)
        data['issuerFee'] = safe_convert(element, 'issuerFee', float)
        data['issuerId'] = safe_convert(element, 'issuerId', int)
        data['language'] = safe_convert(element, 'language', int)
        data['locationChainId'] = safe_convert(element, 'locationChainId', int)
        data['locationCountry'] = safe_convert(element, 'locationCountry', int)
        data['locationCurrency'] = safe_convert(element, 'locationCurrency', str)
        data['locationId'] = safe_convert(element, 'locationId', int)
        data['locationName'] = safe_convert(element, 'locationName', str)
        data['locationCity'] = safe_convert(element, 'locationCity', str)
        data['locationState'] = safe_convert(element, 'locationState', str)
        data['locationZip'] = safe_convert(element, 'locationZip', str)
        data['locationAddress'] = safe_convert(element, 'locationAddress', str)
        data['locationLongitude'] = safe_convert(element, 'locationLongitude', str)
        data['locationLatitude'] = safe_convert(element, 'locationLatitude', str)
        data['messageDLVD'] = safe_convert(element, 'messageDLVD', str)
        data['netTotal'] = safe_convert(element, 'netTotal', float)
        data['nonAreaFee'] = safe_convert(element, 'nonAreaFee', float)
        data['override'] = safe_convert(element, 'override', bool)
        data['originalTransId'] = safe_convert(element, 'originalTransId', int)
        data['postDiscTax'] = safe_convert(element, 'postDiscTax', float)
        data['preDiscTax'] = safe_convert(element, 'preDiscTax', float)
        data['prefTotal'] = safe_convert(element, 'prefTotal', float)
        data['reportedCarrier'] = safe_convert(element, 'reportedCarrier', datetime)
        data['settleAmount'] = safe_convert(element, 'settleAmount', float)
        data['settleId'] = safe_convert(element, 'settleId', int)
        data['splitBilling'] = safe_convert(element, 'splitBilling', bool)
        data['statementId'] = safe_convert(element, 'statementId', int)
        data['supplierId'] = safe_convert(element, 'supplierId', int)
        data['suprFee'] = safe_convert(element, 'suprFee', float)
        data['taxExemptAmount'] = safe_convert(element, 'taxExemptAmount', float)
        data['terminalId'] = safe_convert(element, 'terminalId', str)
        data['terminalType'] = safe_convert(element, 'terminalType', str)
        data['transReported'] = safe_convert(element, 'transReported', datetime)
        data['transactionDate'] = safe_convert(element, 'transactionDate', datetime)
        data['transactionId'] = safe_convert(element, 'transactionId', int)
        data['transactionType'] = safe_convert(element, 'transactionType', int)

        # --- Nested Single Models ---
        fleet_memo_elem = element.find('fleetMemo')
        if fleet_memo_elem is not None and not is_nil(fleet_memo_elem):
            data['fleetMemo'] = WSFleetMemo.from_xml_element(fleet_memo_elem)

        # --- Nested List Models ---
        data['lineItems'] = [
            WSTransactionLineItemExt.from_xml_element(item)
            for item in element.findall('lineItems')
        ]
        data['carmsStatements'] = [
            WSTransactionCarmsStmt.from_xml_element(item)
            for item in element.findall('carmsStatements')
        ]
        data['infos'] = [
            WSTransactionInfo.from_xml_element(item)
            for item in element.findall('infos')
        ]
        data['metaData'] = [
            WSMetaData.from_xml_element(item) for item in element.findall('metaData')
        ]
        data['transTaxes'] = [
            WSTransTaxes.from_xml_element(item)
            for item in element.findall('transTaxes')
        ]

        try:
            # Use model_validate to create the instance
            return cls.model_validate(data)
        except ValidationError as e:
            logger.error(f'Failed to validate parsed XML data: {e}\nData: {data}')
            # Raise a more specific error
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
        root: etree._Element = parse_soap_response(xml_string)

        # 2. Check for SOAP:Fault
        check_for_soap_fault(root)

        # 3. Get the <soap:Body>
        body: etree._Element = extract_soap_body(root)

        # 4. Find all transaction elements and parse them
        transactions: list[WSMCTransExtLocV2] = []

        # Based on the XML structure, the repeating transaction element is <value>
        # First find the result element, then get its direct value children
        result_element = body.find('.//result')
        if result_element is None:
            logger.warning('No <result> element found in the response body.')
            return cls(transactions=[])

        # Get only the direct children named 'value'
        result_elements = result_element.findall('./value')

        if not result_elements:
            logger.warning('No <value> elements found in the response body.')
            # This isn't an error, just an empty list
            return cls(transactions=[])

        logger.info(f'Found {len(result_elements)} <value> elements to parse.')

        for trans_elem in result_elements:
            try:
                transactions.append(WSMCTransExtLocV2.from_xml_element(trans_elem))
            except Exception as e:
                # Log the error and the specific XML that failed
                failed_xml = etree.tostring(
                    trans_elem, pretty_print=True, encoding='unicode'
                )
                logger.error(
                    f'Failed to parse one transaction <value> element: {e}\n'
                    f'--- Failing XML Snippet ---\n{failed_xml}\n'
                    f'--- End Snippet ---'
                )
                # Continue parsing other transactions

        logger.info(f'Successfully parsed {len(transactions)} transactions.')
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

            # Extract info values using list comprehension
            info_dict: dict[str | None, str | None] = {
                info.info_type: info.info_value for info in t.infos
            }
            trans_data.update(
                {
                    col_name: info_dict.get(info_type)
                    for info_type, col_name in INFO_FIELD_MAP.items()
                }
            )

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

        # Apply datetime conversion to columns
        for col in datetime_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])

        # Apply fuel type name mapping to column
        if 'fuel_type' in df.columns:
            df['fuel_type_name'] = df['fuel_type'].map(FUEL_TYPE_MAP)

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
