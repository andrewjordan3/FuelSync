"""Tests for XML parsing utilities."""

import pytest
from lxml import etree
from lxml.etree import Element

from fuelsync.utils.xml_parser import (
    check_for_soap_fault,
    extract_soap_body,
    parse_soap_response,
)


class TestParseSoapResponse:
    """Tests for parse_soap_response function."""

    def test_parse_valid_soap_response(self) -> None:
        """Test parsing a valid SOAP response."""
        xml = """<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
    <soapenv:Body>
        <response>Success</response>
    </soapenv:Body>
</soapenv:Envelope>"""
        root: Element = parse_soap_response(xml)
        assert root is not None
        assert isinstance(root, Element)

    def test_parse_malformed_xml_raises_error(self) -> None:
        """Test that malformed XML raises XMLSyntaxError."""
        xml = '<invalid><xml>'
        with pytest.raises(etree.XMLSyntaxError):
            parse_soap_response(xml)

    def test_parse_empty_string_raises_error(self) -> None:
        """Test that empty string raises XMLSyntaxError."""
        with pytest.raises(etree.XMLSyntaxError):
            parse_soap_response('')


class TestExtractSoapBody:
    """Tests for extract_soap_body function."""

    def test_extract_body_from_valid_envelope(self) -> None:
        """Test extracting body from a valid SOAP envelope."""
        xml = """<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
    <soapenv:Body>
        <response>Success</response>
    </soapenv:Body>
</soapenv:Envelope>"""
        root: Element = parse_soap_response(xml)
        body: Element = extract_soap_body(root)
        assert body is not None
        assert body.tag.endswith('Body')  # pyright: ignore[reportArgumentType, reportAttributeAccessIssue]

    def test_extract_body_raises_error_when_missing(self) -> None:
        """Test that missing Body element raises ValueError."""
        xml = """<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
    <soapenv:Header/>
</soapenv:Envelope>"""
        root: Element = parse_soap_response(xml)
        with pytest.raises(ValueError, match='No SOAP Body element found'):
            extract_soap_body(root)


class TestCheckForSoapFault:
    """Tests for check_for_soap_fault function."""

    def test_no_fault_does_not_raise(self) -> None:
        """Test that a response without fault does not raise an error."""
        xml = """<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
    <soapenv:Body>
        <response>Success</response>
    </soapenv:Body>
</soapenv:Envelope>"""
        root: Element = parse_soap_response(xml)
        # Should not raise
        check_for_soap_fault(root)

    def test_fault_raises_runtime_error(self) -> None:
        """Test that a SOAP fault raises RuntimeError."""
        xml = """<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
    <soapenv:Body>
        <soapenv:Fault>
            <faultcode>soapenv:Server</faultcode>
            <faultstring>Invalid credentials</faultstring>
        </soapenv:Fault>
    </soapenv:Body>
</soapenv:Envelope>"""
        root: Element = parse_soap_response(xml)
        with pytest.raises(
            RuntimeError, match=r'SOAP Fault.*Server.*Invalid credentials'
        ):
            check_for_soap_fault(root)

    def test_fault_with_missing_faultcode(self) -> None:
        """Test handling SOAP fault with missing faultcode."""
        xml = """<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
    <soapenv:Body>
        <soapenv:Fault>
            <faultstring>Something went wrong</faultstring>
        </soapenv:Fault>
    </soapenv:Body>
</soapenv:Envelope>"""
        root: Element = parse_soap_response(xml)
        with pytest.raises(
            RuntimeError, match=r'SOAP Fault.*Unknown.*Something went wrong'
        ):
            check_for_soap_fault(root)

    def test_fault_with_missing_faultstring(self) -> None:
        """Test handling SOAP fault with missing faultstring."""
        xml = """<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/">
    <soapenv:Body>
        <soapenv:Fault>
            <faultcode>soapenv:Client</faultcode>
        </soapenv:Fault>
    </soapenv:Body>
</soapenv:Envelope>"""
        root: Element = parse_soap_response(xml)
        with pytest.raises(RuntimeError, match=r'SOAP Fault.*Client.*Unknown error'):
            check_for_soap_fault(root)
