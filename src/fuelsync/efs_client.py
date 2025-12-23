# fuelsync/efs_client.py
"""
EFS SOAP API Client

This module provides a high-level client for interacting with the EFS
(Electronic Funds Source) SOAP API. It handles authentication, request
construction using Jinja2 templates, and communication with the API endpoint.
"""

import logging
from pathlib import Path
from types import TracebackType

import requests
from jinja2 import Environment, FileSystemLoader, Template

from fuelsync.models import EfsOperationRequest
from fuelsync.utils import FuelSyncConfig, load_config, login_to_efs

# Set up module-level logger
logger: logging.Logger = logging.getLogger(__name__)


class EfsClient:
    """
    Client for interacting with the EFS SOAP API.

    This class manages authentication and provides a type-safe interface
    for executing SOAP operations against the EFS API. It uses:
    - Pydantic models for request validation
    - Jinja2 templates for SOAP envelope generation
    - Type-safe operation methods

    The client does NOT parse responses - that responsibility belongs
    to the calling code, which can implement operation-specific parsing.

    Attributes:
        config: The EFS configuration object containing endpoint and credentials.
        session_token: The authenticated session token (clientId) used for
                      all API operations.
        base_soap_headers: The base HTTP headers used for all SOAP requests.
                          The SOAPAction header is added per-operation.
        jinja_env: The Jinja2 environment for loading and rendering templates.

    Usage:
        Context Manager (Recommended):
            >>> with EfsClient() as client:
            >>>     response = client.execute_operation('getCardList')
            >>>     # ... work with response ...
            >>> # Automatically logs out when exiting the with block

        Manual Management:
            >>> client = EfsClient()
            >>> try:
            >>>     response = client.execute_operation('getCardList')
            >>>     # ... work with response ...
            >>> finally:
            >>>     client.logout()
    """

    def __init__(
        self, config_path: Path | None = None, config: FuelSyncConfig | None = None
    ) -> None:
        """
        Initialize the EFS client with configuration and authentication.

        This constructor performs three critical setup steps:
        1. Loads configuration from the specified path or default location
        2. Authenticates with the EFS API to obtain a session token
        3. Pre-builds the base SOAP headers for efficiency

        Args:
            config_path: Optional path to the configuration file.
                        If None, uses the default configuration location
                        as defined in load_config().
            config: Optional pre-loaded FuelSyncConfig instance. If provided,
                   config_path is ignored.

        Raises:
            FileNotFoundError: If the config file doesn't exist.
            ValueError: If the config file is invalid.
            RuntimeError: If authentication fails.
            requests.exceptions.RequestException: If network errors occur.

        Example:
            >>> # Use default config
            >>> client = EfsClient()
            >>>
            >>> # Use custom config location
            >>> client = EfsClient(Path('/etc/fuelsync/config.yaml'))
        """
        # ====================================================================
        # STEP 1: Load Configuration
        # ====================================================================
        # If no config object is passed, load the configuration, either from the
        # specified path or from the default location. This config contains the
        # API endpoint, credentials, and request settings.
        if config is not None:
            # Use the injected configuration (Dependency Injection)
            self.config: FuelSyncConfig = config
            logger.debug('Initializing EfsClient with injected configuration')
        elif config_path is not None:
            logger.info('Loading EFS configuration from: %r', config_path)
            self.config = load_config(config_path)
        else:
            logger.info('Loading EFS configuration from default location')
            self.config = load_config()

        logger.debug('Configuration loaded successfully')

        # ====================================================================
        # STEP 2: Authenticate and Obtain Session Token
        # ====================================================================
        # Immediately authenticate with the EFS API to obtain a session token.
        # This token will be included in all subsequent operation requests.
        # If authentication fails, the exception will propagate to the caller.
        logger.info('Authenticating with EFS API at %r', self.config.efs.endpoint_url)

        try:
            self.session_token: str = login_to_efs(self.config)
            logger.info('EFS client initialized successfully with active session')
        except Exception as auth_error:
            logger.error('Failed to authenticate with EFS API: %r', auth_error)
            raise

        # ====================================================================
        # STEP 3: Pre-build Base SOAP Headers
        # ====================================================================
        # Build the base headers once during initialization rather than
        # recreating them for every request. This improves efficiency.
        # The SOAPAction header will be added per-operation since it varies.
        self.base_soap_headers: dict[str, str] = {
            'Content-Type': 'text/xml; charset=utf-8',
            'Accept': 'text/xml',
        }
        logger.debug('Base SOAP headers initialized')

        # ====================================================================
        # STEP 4: Set Up Jinja2 Template Environment
        # ====================================================================
        # Find the templates directory relative to this file
        templates_dir: Path = Path(__file__).parent / 'templates'

        if not templates_dir.exists():
            error_message: str = f'Templates directory not found at: {templates_dir}'
            logger.error(error_message)
            raise FileNotFoundError(error_message)

        self.jinja_env: Environment = Environment(
            loader=FileSystemLoader(str(templates_dir)),
            autoescape=True,  # Automatically escape variables for XML safety
            trim_blocks=True,
            lstrip_blocks=True,
        )
        logger.debug(
            'Jinja2 environment initialized with templates from: %r', templates_dir
        )

    def _build_headers(self, operation_name: str) -> dict[str, str]:
        """
        Build complete HTTP headers for a SOAP operation.

        This method takes the pre-built base headers and adds the
        operation-specific SOAPAction header.

        Args:
            operation_name: The name of the SOAP operation being executed.

        Returns:
            A dictionary of HTTP headers ready for the request.
        """
        # Create a copy of base headers and add the operation-specific SOAPAction.
        # Using dict unpacking creates a new dict without modifying the original.
        operation_headers: dict[str, str] = {
            **self.base_soap_headers,
            'SOAPAction': operation_name,
        }
        logger.debug('Built headers for operation: %r', operation_name)
        return operation_headers

    def _render_template(
        self,
        template_name: str,
        request_model: EfsOperationRequest,
    ) -> str:
        """
        Render a Jinja2 template with validated request data.

        Args:
            template_name: The name of the template file (e.g., 'getMCTransExtLocV2.xml').
            request_model: A validated Pydantic model containing request parameters.

        Returns:
            The rendered SOAP envelope as an XML string.

        Raises:
            jinja2.TemplateNotFound: If the template file doesn't exist.
        """
        logger.debug('Loading template: %r', template_name)

        template: Template = self.jinja_env.get_template(template_name)

        # Get SOAP-formatted data from the model
        soap_data: dict[str, str] | dict[str, str | None] = (
            request_model.to_soap_format()
        )

        # Add client_id to the template context
        template_context: dict[str, str] | dict[str, str | None] = {
            'client_id': self.session_token,
            **soap_data,
        }

        logger.debug('Rendering template with %d parameters', len(template_context))
        rendered_xml: str = template.render(template_context)

        return rendered_xml

    def _send_request(
        self,
        operation_name: str,
        headers: dict[str, str],
        body: str,
    ) -> requests.Response:
        """
        Send the SOAP request to the EFS API endpoint.

        This method handles the actual HTTP POST request, including
        proper encoding, timeout handling, and SSL verification.

        Args:
            operation_name: The name of the operation (for logging purposes).
            headers: The complete HTTP headers for the request.
            body: The SOAP envelope as an XML string.

        Returns:
            The HTTP response from the EFS API.

        Raises:
            requests.exceptions.Timeout: If the request times out.
            requests.exceptions.HTTPError: If the server returns an HTTP error.
            requests.exceptions.RequestException: For other network-level errors.
        """
        try:
            logger.debug(
                'Sending SOAP request to %r (connect/read timeout=%r)',
                self.config.efs.endpoint_url,
                self.config.client.request_timeout,
            )

            # Send the POST request with the SOAP envelope encoded as UTF-8 bytes
            response: requests.Response = requests.post(
                str(self.config.efs.endpoint_url),
                data=body.encode('utf-8'),
                headers=headers,
                timeout=self.config.client.request_timeout,
                verify=self.config.client.verify_ssl,
            )

            # Log the response status for debugging/monitoring
            logger.debug(
                'Received response for operation %r: HTTP %r',
                operation_name,
                response.status_code,
            )

            # Check for HTTP-level errors (4xx, 5xx status codes).
            # Note: SOAP Faults typically return 200 OK, so the caller must
            # still check the response XML for application-level errors.
            response.raise_for_status()

            logger.info(
                'Operation %r completed successfully (HTTP %r)',
                operation_name,
                response.status_code,
            )

            return response

        except requests.exceptions.Timeout as timeout_error:
            logger.error(
                'Request timeout for operation %r after %r: %r',
                operation_name,
                self.config.client.request_timeout,
                timeout_error,
            )
            raise

        except requests.exceptions.HTTPError as http_error:
            logger.error('HTTP error for operation %r: %r', operation_name, http_error)

            # Log request details
            logger.debug('***REQUEST HEADERS***')
            logger.debug('\n'.join(f'  {k}: {v}' for k, v in headers.items()))

            logger.debug('***REQUEST BODY (XML)***')
            logger.debug(body)

            # Log response details if available
            if http_error.response is not None:
                logger.debug('***RESPONSE HEADERS***')
                logger.debug(
                    '\n'.join(
                        f'  {k}: {v}' for k, v in http_error.response.headers.items()
                    )
                )

                logger.debug('***RESPONSE BODY (XML)***')
                logger.debug(http_error.response.text)

            raise

        except requests.exceptions.RequestException as request_error:
            logger.error(
                'Network error for operation %r: %r', operation_name, request_error
            )
            raise

    def execute_operation(
        self,
        request_model: EfsOperationRequest,
    ) -> requests.Response:
        """
        Execute a SOAP operation against the EFS API.

        This method coordinates the execution of a SOAP operation by:
        1. Building the appropriate HTTP headers
        2. Constructing the SOAP envelope body
        3. Sending the request and returning the response

        The method returns the raw HTTP response object. The caller is
        responsible for parsing the SOAP response XML and handling any
        SOAP Faults or extracting result data.

        Args:
        request_model: A validated Pydantic model that inherits from
                      EfsOperationRequest. The model contains all required
                      parameters and knows its own operation and template names.

        Returns:
            requests.Response: The raw HTTP response from the EFS API.
                                Callers should check response.status_code,
                                parse response.text as XML, and handle any
                                SOAP Faults appropriately.

        Raises:
            requests.exceptions.HTTPError: If the server returns an HTTP error.
            requests.exceptions.Timeout: If the request times out.
            requests.exceptions.RequestException: For other network-level errors.
            jinja2.TemplateNotFound: If the template file doesn't exist.

        Example:
            >>> from fuelsync import EfsClient
            >>> from fuelsync.models import GetMCTransExtLocV2Request
            >>> from datetime import datetime, timezone
            >>>
            >>> with EfsClient() as client:
            >>>     request = GetMCTransExtLocV2Request(
            >>>         beg_date=datetime(2025, 11, 1, tzinfo=timezone.utc),
            >>>         end_date=datetime(2025, 11, 14, tzinfo=timezone.utc)
            >>>     )
            >>>     response = client.execute_operation(
            >>>         operation_name='getMCTransExtLocV2',
            >>>         template_name='getMCTransExtLocV2.xml',
            >>>         request_model=request
            >>>     )
            >>>     # Parse response...
        """
        # Extract operation name and template name from the model
        operation_name: str = request_model.operation_name
        template_name: str = request_model.template_name
        logger.info('Executing EFS operation: %r', operation_name)

        # Build the request components using our focused private methods
        headers: dict[str, str] = self._build_headers(operation_name)
        body: str = self._render_template(template_name, request_model)

        # Send the request and return the response
        return self._send_request(operation_name, headers, body)

    def logout(self) -> None:
        """
        Explicitly log out and terminate the current EFS session.

        This method calls the EFS 'logout' operation to properly terminate
        the authenticated session on the server side. After logout, the
        session token is cleared to prevent accidental reuse.

        While sessions may expire automatically after a period of inactivity,
        explicitly logging out is a best practice for:
        - Releasing server-side resources promptly
        - Security (invalidates the token immediately)
        - Clean shutdown of long-running processes

        Raises:
            requests.exceptions.HTTPError: If the logout request fails.
            requests.exceptions.RequestException: For network-level errors.

        Example:
            >>> client = EfsClient()
            >>> # ... perform operations ...
            >>> client.logout()  # Clean session termination
        """
        logger.info('Logging out of EFS session')

        try:
            # Build headers for logout operation
            headers: dict[str, str] = self._build_headers('logout')

            # Load and render the logout template
            logger.debug('Loading logout template')
            template: Template = self.jinja_env.get_template('logout.xml')
            body: str = template.render(client_id=self.session_token)
            logger.debug('Logout template rendered')

            # Send the logout request
            response: requests.Response = self._send_request('logout', headers, body)

            # Clear the session token to prevent reuse
            self.session_token = ''

            logger.info(
                'Successfully logged out of EFS (HTTP %r)', response.status_code
            )

        except Exception as logout_error:
            logger.warning(
                'Logout request encountered an error: %r. '
                'Session token cleared locally regardless.',
                logout_error,
            )
            # Clear the token even if logout failed, to prevent reuse
            self.session_token = ''
            raise

    def __enter__(self) -> 'EfsClient':
        """
        Enter the context manager (with statement).

        The client is already initialized and authenticated by __init__,
        so we just return self to make it available in the with block.

        Returns:
            The EfsClient instance.

        Example:
            >>> with EfsClient() as client:
            >>>     response = client.execute_operation('getCardList')
            >>>     # ... work with response ...
            >>> # Automatically logged out when exiting the with block
        """
        logger.debug('Entering EfsClient context manager')
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        _exc_value: BaseException | None,
        _traceback: TracebackType | None,
    ) -> None:
        """
        Exit the context manager and clean up resources.

        This method is called automatically when exiting a 'with' block,
        even if an exception occurred. It ensures the EFS session is
        properly terminated by calling logout().

        Args:
            exc_type: The type of exception that occurred (if any).
            _exc_value: The exception instance (if any).
            _traceback: The traceback object (if any).

        Note:
            This method does not suppress exceptions. If an exception
            occurred in the with block, it will be re-raised after logout.
        """
        exception_present: bool = exc_type is not None
        logger.debug(
            'Exiting EfsClient context manager (exception occurred: %s)',
            exception_present,
        )

        try:
            # Always attempt to logout, even if an exception occurred
            # in the with block
            self.logout()
        except Exception as logout_error:
            # If logout fails, log the error but don't suppress the
            # original exception from the with block
            if exception_present:
                # There was already an exception, log logout failure as warning
                logger.warning(
                    'Logout failed during exception handling: %r. '
                    'Original exception will be raised.',
                    logout_error,
                )
            else:
                # No original exception, so the logout error is the main issue
                logger.error('Logout failed: %r', logout_error)
                raise

        # Return None (or False) to allow exceptions to propagate
        # Return True would suppress the exception, which we don't want

    def __repr__(self) -> str:
        """
        Return a string representation of the client for debugging.

        Returns:
            A string showing the endpoint URL and authentication status.
        """
        return (
            f'EfsClient('
            f'endpoint={self.config.efs.endpoint_url}, '
            f'authenticated={bool(self.session_token)}'
            f')'
        )
