"""Formatting help functions."""

import locale
import typing as t

__all__ = ["format_number", "format_percent"]


_LOCALE_MAP: dict[int, str] = {
    0: "%.0f",
    1: "%.1f",
    2: "%.2f",
    3: "%.3f",
    4: "%.4f",
    5: "%.5f",
}

_PERCENT_LIMIT_MAP: dict[int, float] = {
    0: 1.0,
    1: 0.1,
    2: 0.01,
    3: 0.001,
    4: 0.0001,
    5: 0.00001,
}


def format_number(number: t.Union[float, int], significant_digits: int = 3) -> str:
    """Format a number with given number of significant digits with locale."""
    if number >= 100:
        significant_digits -= 3
    elif number >= 10:
        significant_digits -= 2
    elif number >= 1:
        significant_digits -= 1
    n_decimals = max(significant_digits, 0)
    try:
        formatted_number = locale.format_string(_LOCALE_MAP[n_decimals], number, grouping=True)
    except KeyError:
        raise ValueError(f"Don't know how to format with {n_decimals}") from None

    return formatted_number


def format_percent(number: t.Union[float, int], n_decimals: int) -> str:
    try:
        limit = _PERCENT_LIMIT_MAP[n_decimals]
    except KeyError:
        raise ValueError(f"Don't know how to format with {n_decimals}") from None
    if number < limit:
        below_limit_value = locale.format_string(_LOCALE_MAP[n_decimals], limit)
        return f"< {below_limit_value}"
    try:
        formatted_number = locale.format_string(_LOCALE_MAP[n_decimals], number, grouping=True)
    except KeyError:
        raise ValueError(f"Don't know how to format with {n_decimals}") from None

    return formatted_number
