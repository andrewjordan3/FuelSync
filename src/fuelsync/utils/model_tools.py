# fuelsync/utils/model_tools.py

# pyright: reportUnknownVariableType=false, reportUnknownMemberType=false, reportAttributeAccessIssue=false, reportUnknownParameterType=false, reportUnknownArgumentType=false
import logging
from datetime import datetime

from lxml import etree

logger: logging.Logger = logging.getLogger(__name__)


def is_nil(element: etree._Element | None) -> bool:
    """
    Check if an XML element has xsi:nil='1' or xsi:nil='true' attribute.

    Many SOAP responses use xsi:nil to indicate null values rather than
    omitting the element entirely. This helper checks for that attribute.

    Args:
        element: The XML element to check.

    Returns:
        True if the element is None or has xsi:nil set to '1' or 'true'.

    Example XML:
        <discType xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:nil="1" />
    """
    if element is None:
        return True

    # Check for xsi:nil attribute (with proper namespace)
    nil_attr: etree._Element = element.get(
        '{http://www.w3.org/2001/XMLSchema-instance}nil'
    )
    return nil_attr == '1' or nil_attr == 'true'


def safe_convert(
    element: etree._Element | None,
    tag: str,
    target_type: type,
) -> int | float | bool | datetime | str | None:
    """
    Finds a child tag and safely converts its text to the target type.

    Returns None if:
    - The tag is missing
    - The tag has xsi:nil='1' attribute
    - The text is empty or whitespace-only
    - The conversion fails

    Args:
        element: The parent lxml element.
        tag: The string name of the child tag to find.
        target_type: The Python type to convert the text to
                    (int, float, bool, datetime, str).

    Returns:
        The converted value, or None if conversion fails or value is null.
    """
    if element is None:
        return None

    child: etree._Element | None = element.find(tag)
    if child is None or is_nil(child):
        return None

    text: str = child.text
    if text is None or not text.strip():
        return None

    text = text.strip()

    try:
        if target_type is int:
            return int(text)
        if target_type is float:
            return float(text)
        if target_type is bool:
            return text.lower() == 'true'
        if target_type is datetime:
            # Handle timezone-aware ISO strings (e.g., 2025-11-11T13:29:00.000-06:00)
            return datetime.fromisoformat(text)
        return text  # Default to string
    except (ValueError, TypeError) as e:
        logger.warning(
            f'Failed to convert text "{text}" from tag <{tag}> to {target_type}: {e}'
        )
        return None
