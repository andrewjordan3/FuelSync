"""Tests for EFS SOAP API client."""

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from requests import Response

from fuelsync.efs_client import EfsClient
from fuelsync.models import GetMCTransExtLocV2Request
from fuelsync.utils import FuelSyncConfig


class TestEfsClientInit:
    """Tests for EfsClient initialization."""

    @patch('fuelsync.efs_client.login_to_efs')
    def test_client_initialization_with_config(
        self, mock_login: Mock, sample_config: FuelSyncConfig
    ) -> None:
        """Test client initialization with a config object."""
        mock_login.return_value = 'test-session-token'

        client = EfsClient(config=sample_config)

        assert client.config == sample_config
        assert client.session_token == 'test-session-token'
        mock_login.assert_called_once_with(sample_config)

    @patch('fuelsync.efs_client.login_to_efs')
    @patch('fuelsync.efs_client.load_config')
    def test_client_initialization_with_config_path(
        self, mock_load: Mock, mock_login: Mock, sample_config: FuelSyncConfig
    ) -> None:
        """Test client initialization with a config path."""
        mock_load.return_value = sample_config
        mock_login.return_value = 'test-session-token'

        test_path = Path('/test/config.yaml')
        client = EfsClient(config_path=test_path)

        assert client.session_token == 'test-session-token'
        mock_load.assert_called_once_with(test_path)
        mock_login.assert_called_once_with(sample_config)

    @patch('fuelsync.efs_client.login_to_efs')
    @patch('fuelsync.efs_client.load_config')
    def test_client_initialization_without_config(
        self, mock_load: Mock, mock_login: Mock, sample_config: FuelSyncConfig
    ) -> None:
        """Test client initialization without config (uses default)."""
        mock_load.return_value = sample_config
        mock_login.return_value = 'test-session-token'

        client = EfsClient()

        assert client.session_token == 'test-session-token'
        mock_load.assert_called_once()
        mock_login.assert_called_once()

    @patch('fuelsync.efs_client.login_to_efs')
    def test_client_initialization_login_failure(
        self, mock_login: Mock, sample_config: FuelSyncConfig
    ) -> None:
        """Test that login failure raises an exception."""
        mock_login.side_effect = RuntimeError('Login failed')

        with pytest.raises(RuntimeError, match='Login failed'):
            EfsClient(config=sample_config)


class TestEfsClientBuildHeaders:
    """Tests for _build_headers method."""

    @patch('fuelsync.efs_client.login_to_efs')
    def test_build_headers_includes_soap_action(
        self, mock_login: Mock, sample_config: FuelSyncConfig
    ) -> None:
        """Test that headers include SOAPAction."""
        mock_login.return_value = 'test-token'
        client = EfsClient(config=sample_config)

        headers: dict[str, str] = client._build_headers('testOperation')  # pyright: ignore[reportPrivateUsage]

        assert headers['SOAPAction'] == 'testOperation'
        assert headers['Content-Type'] == 'text/xml; charset=utf-8'
        assert headers['Accept'] == 'text/xml'


class TestEfsClientRenderTemplate:
    """Tests for _render_template method."""

    @patch('fuelsync.efs_client.login_to_efs')
    def test_render_template_with_request(
        self, mock_login: Mock, sample_config: FuelSyncConfig
    ) -> None:
        """Test rendering a template with a request model."""
        mock_login.return_value = 'test-token'
        client = EfsClient(config=sample_config)

        beg_date = datetime(2024, 11, 1, tzinfo=UTC)
        end_date = datetime(2024, 11, 14, tzinfo=UTC)
        request = GetMCTransExtLocV2Request(beg_date=beg_date, end_date=end_date)

        rendered: str = client._render_template('getMCTransExtLocV2.xml', request)  # pyright: ignore[reportPrivateUsage]

        assert isinstance(rendered, str)
        # Check that session token is in the rendered template
        assert 'test-token' in rendered


class TestEfsClientExecuteOperation:
    """Tests for execute_operation method."""

    @patch('fuelsync.efs_client.login_to_efs')
    @patch('fuelsync.efs_client.requests.post')
    def test_execute_operation_success(
        self,
        mock_post: Mock,
        mock_login: Mock,
        sample_config: FuelSyncConfig,
        mock_requests_response: Mock,
    ) -> None:
        """Test successful operation execution."""
        mock_login.return_value = 'test-token'
        mock_post.return_value = mock_requests_response

        client = EfsClient(config=sample_config)

        beg_date = datetime(2024, 11, 1, tzinfo=UTC)
        end_date = datetime(2024, 11, 14, tzinfo=UTC)
        request = GetMCTransExtLocV2Request(beg_date=beg_date, end_date=end_date)

        response: Response = client.execute_operation(request)

        assert response.status_code == 200  # noqa: PLR2004
        mock_post.assert_called_once()


class TestEfsClientLogout:
    """Tests for logout method."""

    @patch('fuelsync.efs_client.login_to_efs')
    @patch('fuelsync.efs_client.requests.post')
    def test_logout_success(
        self,
        mock_post: Mock,
        mock_login: Mock,
        sample_config: FuelSyncConfig,
        mock_requests_response: Mock,
    ) -> None:
        """Test successful logout."""
        mock_login.return_value = 'test-token'
        mock_post.return_value = mock_requests_response

        client = EfsClient(config=sample_config)
        client.logout()

        assert client.session_token == ''
        mock_post.assert_called()

    @patch('fuelsync.efs_client.login_to_efs')
    @patch('fuelsync.efs_client.requests.post')
    def test_logout_clears_token_on_failure(
        self, mock_post: Mock, mock_login: Mock, sample_config: FuelSyncConfig
    ) -> None:
        """Test that logout clears token even if request fails."""
        mock_login.return_value = 'test-token'
        mock_post.side_effect = Exception('Network error')

        client = EfsClient(config=sample_config)

        with pytest.raises(Exception, match='Network error'):
            client.logout()

        # Token should be cleared even though logout failed
        assert client.session_token == ''


class TestEfsClientContextManager:
    """Tests for context manager functionality."""

    @patch('fuelsync.efs_client.login_to_efs')
    @patch('fuelsync.efs_client.requests.post')
    def test_context_manager_calls_logout(
        self,
        mock_post: Mock,
        mock_login: Mock,
        sample_config: FuelSyncConfig,
        mock_requests_response: Mock,
    ) -> None:
        """Test that context manager calls logout on exit."""
        mock_login.return_value = 'test-token'
        mock_post.return_value = mock_requests_response

        with EfsClient(config=sample_config) as client:
            assert client.session_token == 'test-token'

        # Logout should have been called
        assert mock_post.call_count >= 1

    @patch('fuelsync.efs_client.login_to_efs')
    @patch('fuelsync.efs_client.requests.post')
    def test_context_manager_logout_on_exception(
        self, mock_post: Mock, mock_login: Mock, sample_config: FuelSyncConfig
    ) -> None:
        """Test that context manager calls logout even if exception occurs."""
        mock_login.return_value = 'test-token'
        mock_post.return_value = Mock()

        with (
            pytest.raises(ValueError, match='Test error'),
            EfsClient(config=sample_config),
        ):
            raise ValueError('Test error')

        # Logout should still have been called
        assert mock_post.call_count >= 1


class TestEfsClientRepr:
    """Tests for __repr__ method."""

    @patch('fuelsync.efs_client.login_to_efs')
    def test_repr_with_token(
        self, mock_login: Mock, sample_config: FuelSyncConfig
    ) -> None:
        """Test string representation with active token."""
        mock_login.return_value = 'test-token'
        client = EfsClient(config=sample_config)

        repr_str: str = repr(client)

        assert 'EfsClient' in repr_str
        assert str(sample_config.efs.endpoint_url) in repr_str
        assert 'authenticated=True' in repr_str
