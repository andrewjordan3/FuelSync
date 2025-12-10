"""Tests for configuration loading."""

from pathlib import Path

import pytest
from pydantic import ValidationError

from fuelsync.utils.config_loader import FuelSyncConfig, load_config


class TestFuelSyncConfig:
    """Tests for FuelSyncConfig model."""

    def test_valid_config_creation(self) -> None:
        """Test creating a valid config from dictionary."""
        config_dict = {
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
                'parquet_file': 'data/transactions.parquet',
                'compression': 'snappy',
            },
            'logging': {
                'console_level': 'INFO',
                'file_level': 'DEBUG',
                'file_path': 'fuelsync.log',
            },
        }

        config = FuelSyncConfig.model_validate(config_dict)

        assert config.efs.endpoint_url == 'https://test.example.com/api'
        assert config.efs.username == 'test_user'
        assert config.client.max_retries == 3
        assert config.pipeline.batch_size_days == 1
        assert config.storage.compression == 'snappy'
        assert config.logging.console_level == 'INFO'

    def test_missing_required_field_raises_error(self) -> None:
        """Test that missing required fields raise ValidationError."""
        config_dict = {
            'efs': {
                'endpoint_url': 'https://test.example.com/api',
                # Missing username and password
            },
            'client': {
                'request_timeout': [10, 30],
            },
        }

        with pytest.raises(ValidationError):
            FuelSyncConfig.model_validate(config_dict)

    def test_invalid_date_format_raises_error(self) -> None:
        """Test that invalid date format raises ValidationError."""
        config_dict = {
            'efs': {
                'endpoint_url': 'https://test.example.com/api',
                'username': 'test_user',
                'password': 'test_password',
            },
            'client': {
                'request_timeout': [10, 30],
                'verify_ssl': True,
            },
            'pipeline': {
                'default_start_date': 'invalid-date',  # Invalid format
                'batch_size_days': 1,
            },
            'storage': {
                'parquet_file': 'data/transactions.parquet',
            },
            'logging': {
                'console_level': 'INFO',
            },
        }

        with pytest.raises(ValidationError):
            FuelSyncConfig.model_validate(config_dict)


class TestLoadConfig:
    """Tests for load_config function."""

    def test_load_config_from_file(self, temp_config_file: Path) -> None:
        """Test loading config from a file."""
        config = load_config(temp_config_file)

        assert config.efs.endpoint_url == 'https://test.example.com/api'
        assert config.efs.username == 'test_user'
        assert config.client.max_retries == 3

    def test_load_config_file_not_found_raises_error(self) -> None:
        """Test that loading non-existent file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            load_config(Path('/nonexistent/config.yaml'))

    def test_load_config_with_string_path(self, temp_config_file: Path) -> None:
        """Test loading config with string path."""
        config = load_config(str(temp_config_file))

        assert config.efs.username == 'test_user'

    def test_load_invalid_yaml_raises_error(self, tmp_path: Path) -> None:
        """Test that invalid YAML raises an error."""
        invalid_yaml = tmp_path / 'invalid.yaml'
        invalid_yaml.write_text('invalid: yaml: content: [')

        with pytest.raises(Exception):  # Should raise YAML parsing error
            load_config(invalid_yaml)
