"""Pytest configuration and shared fixtures for FuelSync tests."""

from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest
import yaml
from requests import Response

from fuelsync.utils import FuelSyncConfig


@pytest.fixture
def sample_config() -> FuelSyncConfig:
    """Create a sample FuelSyncConfig for testing."""
    config_dict: dict[str, Any] = {
        'efs': {
            'endpoint_url': 'https://test.example.com/api',
            'username': 'test_user',
            'password': 'test_password',
        },
        'client': {
            'request_timeout': [10, 30],
            'verify_ssl': True,
            'max_retries': 3,
            'retry_backoff_factor': 2,
        },
        'pipeline': {
            'default_start_date': '2024-01-01',
            'batch_size_days': 1,
            'lookback_days': 7,
            'request_delay_seconds': 0.5,
        },
        'storage': {
            'parquet_file': 'data/test_transactions.parquet',
            'compression': 'snappy',
        },
        'logging': {
            'console_level': 'INFO',
            'file_level': 'DEBUG',
            'file_path': 'test_fuelsync.log',
        },
    }
    return FuelSyncConfig.model_validate(config_dict)


@pytest.fixture
def sample_datetime() -> datetime:
    """Create a sample datetime for testing."""
    return datetime(2024, 11, 14, 15, 30, 45, 123000, tzinfo=UTC)


@pytest.fixture
def mock_soap_response() -> str:
    """Create a mock SOAP response for testing."""
    return """<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
    <soapenv:Body>
        <ns:getMCTransExtLocV2Response xmlns:ns="http://ws.efs.com">
            <return>
                <transactions>
                    <transaction>
                        <transId>12345</transId>
                        <tranDate>2024-11-14T15:30:45.000Z</tranDate>
                        <cardNum>1234567890</cardNum>
                    </transaction>
                </transactions>
            </return>
        </ns:getMCTransExtLocV2Response>
    </soapenv:Body>
</soapenv:Envelope>"""


@pytest.fixture
def mock_soap_fault() -> str:
    """Create a mock SOAP fault response for testing."""
    return """<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
    <soapenv:Body>
        <soapenv:Fault>
            <faultcode>soapenv:Server</faultcode>
            <faultstring>Invalid credentials</faultstring>
        </soapenv:Fault>
    </soapenv:Body>
</soapenv:Envelope>"""


@pytest.fixture
def mock_login_response() -> str:
    """Create a mock login response for testing."""
    return """<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
    <soapenv:Body>
        <ns:loginResponse xmlns:ns="http://ws.efs.com">
            <return>test-session-token-12345</return>
        </ns:loginResponse>
    </soapenv:Body>
</soapenv:Envelope>"""


@pytest.fixture
def mock_requests_response() -> Mock:
    """Create a mock requests.Response object."""
    response = Mock(spec=Response)
    response.status_code = 200
    response.text = '<?xml version="1.0"?><response>Success</response>'
    response.headers = {'Content-Type': 'text/xml'}
    return response


@pytest.fixture
def temp_config_file(tmp_path: Path, sample_config: FuelSyncConfig) -> Path:
    """Create a temporary config file for testing."""
    config_path: Path = tmp_path / 'config.yaml'

    # Dump the config using pydantic + yaml so types are preserved
    config_dict: dict[str, Any] = sample_config.model_dump(mode='json')

    config_path.write_text(
        yaml.safe_dump(
            config_dict,
            sort_keys=False,  # Keep a nice human order
        )
    )

    return config_path
