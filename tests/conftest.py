"""Pytest configuration and shared fixtures for FuelSync tests."""

from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import pytest
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
    config_path = tmp_path / 'config.yaml'
    config_content = f"""efs:
  endpoint_url: "{sample_config.efs.endpoint_url}"
  username: "{sample_config.efs.username}"
  password: "{sample_config.efs.password}"

client:
  request_timeout: {sample_config.client.request_timeout}
  verify_ssl: {str(sample_config.client.verify_ssl).lower()}
  max_retries: {sample_config.client.max_retries}
  retry_backoff_factor: {sample_config.client.retry_backoff_factor}

pipeline:
  default_start_date: "{sample_config.pipeline.default_start_date}"
  batch_size_days: {sample_config.pipeline.batch_size_days}
  lookback_days: {sample_config.pipeline.lookback_days}
  request_delay_seconds: {sample_config.pipeline.request_delay_seconds}

storage:
  parquet_file: "{sample_config.storage.parquet_file}"
  compression: "{sample_config.storage.compression}"

logging:
  console_level: "{sample_config.logging.console_level}"
  file_level: "{sample_config.logging.file_level}"
  file_path: "{sample_config.logging.file_path}"
"""
    config_path.write_text(config_content)
    return config_path
