# fuelsync/utils/model_tools.py
"""
XML parsing utilities for converting lxml elements to Pydantic-ready dictionaries.

This module provides low-level tools for extracting data from XML elements and
structuring it into dictionaries that match Pydantic model definitions.
It bridges the gap between raw XML (via lxml) and typed Python objects (via Pydantic).
"""
# pyright: reportUnknownVariableType=false, reportUnknownMemberType=false, reportAttributeAccessIssue=false, reportUnknownParameterType=false, reportUnknownArgumentType=false

import logging
import types
from typing import Any, Union, get_args, get_origin

from lxml import etree
from pydantic import BaseModel

logger: logging.Logger = logging.getLogger(__name__)


def is_nil(element: etree._Element | None) -> bool:
    """
    Check if an XML element has xsi:nil='1' or xsi:nil='true' attribute.

    Many SOAP responses use the xsi:nil attribute to explicitly indicate a null value,
    rather than simply omitting the tag. This helper reliably detects that state.

    Args:
        element: The XML element to check. Can be None.

    Returns:
        True if the element is None or has xsi:nil set to '1' or 'true'.
        False otherwise.
    """
    if element is None:
        return True

    # Check for xsi:nil attribute (with proper namespace)
    # The namespace map is usually handled by lxml, but looking up the
    # fully qualified name is the most robust method.
    nil_attr: str | None = element.get(
        '{http://www.w3.org/2001/XMLSchema-instance}nil'
    )
    return nil_attr in {'1', 'true'}


def extract_text(element: etree._Element, tag: str) -> str | None:
    """
    Extract stripped text content from a direct child element.

    This function locates a child tag within the parent element and returns its
    text content. It handles cases where the child is missing, nil, or empty.

    Note: This function performs NO type conversion. It returns raw strings (or None).
    Type conversion is the responsibility of the Pydantic model validation step.

    Args:
        element: The parent XML element to search within.
        tag: The name of the child tag to find.

    Returns:
        The stripped text string if found and valid.
        None if the tag is missing, explicitly nil, or contains only whitespace.
    """
    child: etree._Element | None = element.find(tag)

    # Check if child exists and isn't explicitly marked as nil
    if child is None or is_nil(child):
        return None

    # Get the text content
    text: str | None = child.text

    # Return None if text is None or empty string after stripping
    if text is None or not text.strip():
        return None

    return text.strip()


# --- Type Introspection Helpers ---


def _unwrap_optional(field_type: Any) -> Any:
    """
    Unwrap Optional[X] or X | None to get the actual type X.

    Args:
        field_type: A type annotation that might be wrapped in Optional/Union.

    Returns:
        The unwrapped type, or the original type if not wrapped.

    Example:
        >>> _unwrap_optional(str | None)
        str
        >>> _unwrap_optional(list[int])
        list[int]
    """
    origin: Any = get_origin(field_type)
    args: tuple[Any, ...] = get_args(field_type)

    # Check for Union types (both typing.Union and types.UnionType for | syntax)
    if origin is Union or origin is types.UnionType:
        # Find the non-None type in the union
        non_none_types: list[type] = [arg for arg in args if arg is not type(None)]
        if non_none_types:
            return non_none_types[0]

    return field_type


def _is_list_type(field_type: Any) -> bool:
    """
    Check if a type annotation is a list type.

    Args:
        field_type: A type annotation.

    Returns:
        True if the type is list[X], False otherwise.
    """
    return get_origin(field_type) is list


def _get_list_item_type(field_type: Any) -> Any | None:
    """
    Extract the item type from a list[X] annotation.

    This function also unwraps any Optional wrappers around the item type.
    e.g., list[Model | None] -> Model.

    Args:
        field_type: A type annotation that should be list[X].

    Returns:
        The type X, or None if not a list or has no args.
    """
    if not _is_list_type(field_type):
        return None
    args: tuple[Any, ...] = get_args(field_type)
    if not args:
        return None
    # Unwrap the inner type (e.g., list[Optional[int]] -> int)
    return _unwrap_optional(args[0])


def _is_pydantic_model(field_type: Any) -> bool:
    """
    Check if a type is a Pydantic BaseModel subclass.

    Args:
        field_type: A type annotation.

    Returns:
        True if the type is a Pydantic model, False otherwise.
    """
    return isinstance(field_type, type) and issubclass(field_type, BaseModel)


# --- Field Parsing Helpers ---


def _parse_primitive_list(
    element: etree._Element,
    xml_tag: str,
) -> list[str | None]:
    """
    Parse a list of primitive values from repeated XML tags.

    Example XML:
        <items>value1</items>
        <items>value2</items>
        <items xsi:nil="true"/>

    Args:
        element: Parent XML element.
        xml_tag: Name of the repeating child tag.

    Returns:
        List of string values (or None for nil/empty elements).
    """
    nested_elements: list[etree._Element] = element.findall(xml_tag)
    result: list[str | None] = []

    for elem in nested_elements:
        if is_nil(elem):
            result.append(None)
        else:
            text: str | None = elem.text
            if text and text.strip():
                result.append(text.strip())
            else:
                result.append(None)

    return result


def _parse_model_list(
    element: etree._Element,
    xml_tag: str,
    model_class: type[BaseModel],
) -> list[dict[str, Any]]:
    """
    Parse a list of nested Pydantic models from repeated XML tags.

    Example XML:
        <lineItems><amount>10.5</amount></lineItems>
        <lineItems><amount>20.0</amount></lineItems>

    Args:
        element: Parent XML element.
        xml_tag: Name of the repeating child tag.
        model_class: The Pydantic model class for each list item.

    Returns:
        List of dictionaries, each representing a parsed model.
    """
    nested_elements: list[etree._Element] = element.findall(xml_tag)

    if not nested_elements:
        return []

    logger.debug(
        f"Parsing list field (tag '{xml_tag}'): found {len(nested_elements)} items"
    )

    # Recursively parse each nested element
    return [
        parse_xml_to_dict(nested_elem, model_class)
        for nested_elem in nested_elements
        if not is_nil(nested_elem)
    ]


def _parse_nested_model(
    element: etree._Element,
    xml_tag: str,
    model_class: type[BaseModel],
) -> dict[str, Any] | None:
    """
    Parse a single nested Pydantic model from an XML tag.

    Example XML:
        <fleetMemo><cardNumber>123</cardNumber></fleetMemo>

    Args:
        element: Parent XML element.
        xml_tag: Name of the child tag containing the nested model.
        model_class: The Pydantic model class for the nested object.

    Returns:
        Dictionary representing the parsed model, or None if tag missing/nil.
    """
    nested_elem: etree._Element | None = element.find(xml_tag)

    if nested_elem is None or is_nil(nested_elem):
        return None

    # Recursively parse the nested model
    return parse_xml_to_dict(nested_elem, model_class)


def _parse_primitive_field(
    element: etree._Element,
    xml_tag: str,
) -> str | None:
    """
    Parse a simple primitive field (str, int, float, bool, datetime, etc.).

    Pydantic will handle the actual type conversion - this just extracts raw text.

    Args:
        element: Parent XML element.
        xml_tag: Name of the child tag to extract.

    Returns:
        Raw string value, or None if missing/nil/empty.
    """
    return extract_text(element, xml_tag)


# --- Main Parser ---


def parse_xml_to_dict(
    element: etree._Element,
    model_class: type[BaseModel],
) -> dict[str, Any]:
    """
    Parse an XML element into a dictionary structure matching a Pydantic model.

    This function introspects the Pydantic model to determine which XML tags to
    look for and how to structure the output. It dispatches to specialized helpers
    for each field type (primitive, nested model, or list).

    Args:
        element: The root XML element containing data for this model.
        model_class: The Pydantic model class definition to inspect.

    Returns:
        A dictionary containing the extracted data, ready for model_validate().

    Example:
        >>> data = parse_xml_to_dict(xml_element, TransactionModel)
        >>> model = TransactionModel.model_validate(data)
    """
    data: dict[str, Any] = {}

    for field_name, field_info in model_class.model_fields.items():
        # Get the XML tag name (alias) or fallback to Python field name
        xml_tag: str = field_info.alias or field_name

        # Get the field type and unwrap Optional[X] if needed
        field_type: Any = field_info.annotation
        actual_type: Any = _unwrap_optional(field_type)

        # Dispatch to appropriate parser based on type
        if _is_list_type(actual_type):
            item_type: Any | None = _get_list_item_type(actual_type)

            if item_type and _is_pydantic_model(item_type):
                # List of nested models
                data[field_name] = _parse_model_list(element, xml_tag, item_type)
            else:
                # List of primitives
                data[field_name] = _parse_primitive_list(element, xml_tag)

        elif _is_pydantic_model(actual_type):
            # Single nested model
            nested_parsed: dict[str, Any] | None = _parse_nested_model(element, xml_tag, actual_type)
            if nested_parsed is not None:
                data[field_name] = nested_parsed

        else:
            # Simple primitive field
            parsed: str | None = _parse_primitive_field(element, xml_tag)
            if parsed is not None:
                data[field_name] = parsed

    return data
