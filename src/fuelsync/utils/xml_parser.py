# fuelsync/utils/xml_parser.py
"""
XML parsing utilities for EFS SOAP responses.

Provides helper functions for parsing SOAP XML responses with proper
namespace handling.
"""

from lxml import etree


def parse_soap_response(xml_string: str) -> etree.Element:
    """
    Parse a SOAP XML response string into an lxml Element.

    Args:
        xml_string: The raw XML response from the SOAP API.

    Returns:
        The root element of the parsed XML tree.

    Raises:
        etree.XMLSyntaxError: If the XML is malformed.
    """
    return etree.fromstring(xml_string.encode('utf-8'))


def extract_soap_body(root: etree.Element) -> etree.Element:
    """
    Extract the Body element from a SOAP envelope.

    Args:
        root: The root element of the SOAP envelope.

    Returns:
        The Body element containing the actual response data.

    Raises:
        ValueError: If no Body element is found.
    """
    # Define SOAP namespace
    namespaces: dict[str, str] = {
        'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
    }

    body: etree.Element | None = root.find('.//soap:Body', namespaces=namespaces)

    if body is None:
        raise ValueError('No SOAP Body element found in response')

    return body


def check_for_soap_fault(root: etree.Element) -> None:
    """
    Check if the SOAP response contains a Fault element and raise if found.

    Args:
        root: The root element of the SOAP envelope.

    Raises:
        RuntimeError: If a SOAP Fault is found in the response.
    """
    namespaces: dict[str, str] = {
        'soap': 'http://schemas.xmlsoap.org/soap/envelope/',
    }

    fault: etree.Element | None = root.find('.//soap:Fault', namespaces=namespaces)

    if fault is not None:
        fault_code: str = fault.findtext('faultcode', default='Unknown')
        fault_string: str = fault.findtext('faultstring', default='Unknown error')
        raise RuntimeError(f'SOAP Fault [{fault_code}]: {fault_string}')
