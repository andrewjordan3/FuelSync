# fuelsync/response_models/__init__.py
"""
Response models for EFS SOAP API operations.

This package contains Pydantic models for parsing SOAP responses from
various EFS API operations. Each operation has its own module with
dedicated response models.
"""

from .getTranRejects_response import GetTranRejectsResponse, WSTranReject
from .TransExtLocV2_response import (
    GetMCTransExtLocV2Response,
    WSFleetMemo,
    WSMCTransExtLocV2,
    WSMetaData,
    WSTransactionCarmsStmt,
    WSTransactionInfo,
    WSTransactionLineItemExt,
    WSTransTaxes,
)
from .transSummary_response import TransSummaryResponse, WSTransSummary

__all__: list[str] = [
    # TransExtLocV2 models
    'GetMCTransExtLocV2Response',
    'WSMCTransExtLocV2',
    'WSTransactionLineItemExt',
    'WSTransactionInfo',
    'WSTransTaxes',
    'WSMetaData',
    'WSFleetMemo',
    'WSTransactionCarmsStmt',
    # TransSummary models
    'TransSummaryResponse',
    'WSTransSummary',
    # TranRejects models
    'GetTranRejectsResponse',
    'WSTranReject',
]
