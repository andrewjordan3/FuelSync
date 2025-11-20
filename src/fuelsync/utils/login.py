# fuelsync/utils/login.py

import html
import logging
import xml.etree.ElementTree as ET

import requests

from .config_loader import FuelSyncConfig

# Set up a logger for this module
logger: logging.Logger = logging.getLogger(__name__)


def login_to_efs(config: FuelSyncConfig) -> str:
    """
    Authenticates with the EFS SOAP API and retrieves a session token.

    This function performs a SOAP 1.1 login operation by:
    1. Constructing a SOAP envelope with credentials
    2. Sending it via HTTP POST to the EFS endpoint
    3. Parsing the XML response to extract the session token (clientId)

    The returned token must be included in all subsequent API requests
    to maintain the authenticated session.

    SOAP Protocol Context:
        SOAP (Simple Object Access Protocol) is an XML-based messaging
        protocol. Unlike REST APIs that use HTTP methods semantically,
        SOAP always uses POST and embeds operation details in the XML body
        and SOAPAction header.

    Args:
        config: A validated FuelSyncConfig object containing:
            - EFS API endpoint URL
            - Username and password credentials
            - SSL verification settings
            - Request timeout configuration

    Returns:
        str: The session token (clientId) needed for authenticated API calls.
             This token typically expires after a period of inactivity.

    Raises:
        requests.exceptions.HTTPError:
            When the HTTP request fails (network errors, 404, 500, etc.).
        RuntimeError:
            When the SOAP operation fails at the application level
            (invalid credentials, missing token in response, etc.).

    Example:
        >>> config = FuelSyncConfig(...)
        >>> session_token = login_to_efs(config)
        >>> # Use session_token in subsequent API calls
    """
    # ========================================================================
    # STEP 1: Prepare HTTP Headers for SOAP 1.1 Request
    # ========================================================================
    # SOAP 1.1 requires specific headers to route and process the request:
    # - Content-Type: Declares we're sending XML with UTF-8 encoding
    # - Accept: Indicates we expect an XML response
    # - SOAPAction: Required by SOAP 1.1 spec; identifies the operation intent
    soap_request_headers: dict[str, str] = {
        'Content-Type': 'text/xml; charset=utf-8',
        'Accept': 'text/xml',
        # The SOAPAction header must match the operation being invoked.
        # Some SOAP servers use this to route requests even before parsing XML.
        'SOAPAction': 'login',
    }

    # ========================================================================
    # STEP 2: Construct the SOAP Envelope (XML Request Body)
    # ========================================================================
    # SOAP messages are structured as "envelopes" containing a Body.
    # The Body contains the operation name and parameters.
    # We use html.escape() to prevent XML injection attacks by escaping
    # special characters like <, >, and & in user-provided credentials.
    soap_login_request_body: str = f"""<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
  <soap:Body>
    <CardManagementEP_login>
      <user>{html.escape(config.efs.username)}</user>
      <password>{html.escape(config.efs.password.get_secret_value())}</password>
    </CardManagementEP_login>
  </soap:Body>
</soap:Envelope>"""

    # ========================================================================
    # STEP 3: Send the SOAP Request to the EFS Endpoint
    # ========================================================================
    # SOAP always uses HTTP POST, regardless of the operation type.
    # We must encode the string payload to bytes using UTF-8, matching
    # the encoding declared in both the Content-Type header and XML prolog.
    soap_response: requests.Response = requests.post(
        str(config.efs.endpoint_url),
        data=soap_login_request_body.encode('utf-8'),
        headers=soap_request_headers,
        timeout=config.client.request_timeout,
        # verify controls SSL certificate validation (important for security)
        verify=config.client.verify_ssl,
    )

    # ========================================================================
    # STEP 4: Check for HTTP-Level Errors
    # ========================================================================
    # This catches transport-level failures like:
    # - 404 Not Found (wrong endpoint URL)
    # - 500 Internal Server Error (server crash)
    # - 401 Unauthorized (if HTTP auth is also required)
    # Note: Authentication failures in SOAP often return 200 OK with a
    # SOAP Fault in the body, which we handle in the next step.
    soap_response.raise_for_status()

    # ========================================================================
    # STEP 5: Parse the XML Response
    # ========================================================================
    # If we reach here, HTTP succeeded (200 OK). Parse the response text
    # into an XML tree structure for inspection.
    response_xml_root: ET.Element = ET.fromstring(soap_response.text)

    # ========================================================================
    # STEP 6: Check for SOAP Faults (Application-Level Errors)
    # ========================================================================
    # SOAP Faults are the protocol's way of reporting application errors
    # (like "Invalid Username" or "Account Locked") while still returning
    # HTTP 200 OK. We must explicitly check for the <Fault> element.
    # The namespace prefix is required because SOAP uses XML namespaces.
    soap_fault_element: ET.Element | None = response_xml_root.find(
        './/{http://schemas.xmlsoap.org/soap/envelope/}Fault'
    )

    if soap_fault_element is not None:
        # Extract the human-readable error message from <faultstring>.
        # This element is required by the SOAP spec and should always exist
        # in a Fault, but we provide a fallback just in case.
        fault_message: str = (
            soap_fault_element.findtext('faultstring') or 'Unknown SOAP Fault'
        )
        # Provide detailed context in the error message for easier debugging.
        raise RuntimeError(
            f'EFS SOAP login failed with Fault: {fault_message}\n'
            f'This typically indicates invalid credentials or account issues.'
        )

    # ========================================================================
    # STEP 7: Extract the Session Token from the Response
    # ========================================================================
    # If we reach here, the login succeeded. The session token should be
    # in a <result> element. The './/' prefix means "search anywhere in
    # the document" rather than requiring an exact path.
    session_token_element: ET.Element | None = response_xml_root.find('.//result')

    # Validate that we found the element and it contains non-empty text.
    # Using the walrus operator := to both assign and check in one step.
    if session_token_element is None or not (
        session_token := (session_token_element.text or '').strip()
    ):
        # This shouldn't happen if the API is working correctly, but we
        # handle it defensively. Include the raw response for debugging.
        raise RuntimeError(
            'EFS login appeared to succeed (no SOAP Fault), but the session '
            'token was not found in the response.\n'
            f'This may indicate an API contract change.\n'
            f'Raw XML Response:\n{soap_response.text}'
        )

    # ========================================================================
    # Success: Return the Session Token
    # ========================================================================
    # This token must be included in subsequent API calls to maintain
    # the authenticated session. Store it securely and be prepared to
    # handle expiration (you may need to re-login).
    logger.info('Successfully authenticated with EFS API')
    return session_token
