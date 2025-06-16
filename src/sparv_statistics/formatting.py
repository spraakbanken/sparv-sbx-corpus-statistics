"""Formatting help functions."""

import locale

__all__ = ["fmt_number_decimals", "fmt_number_signific"]


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

_SIGN_3: int = 100
_SIGN_2: int = 10


def fmt_number_signific(number: float | int, significant_digits: int = 3) -> str:
    """Format a number with given number of significant digits with locale.

    >>> fmt_number_signific(0.123456789)
    '0.123'
    >>> fmt_number_signific(1.23456789)
    '1.23'
    >>> fmt_number_signific(12.3456789)
    '12.3'
    >>> fmt_number_signific(123.456789)
    '123'
    """
    if number >= _SIGN_3:
        significant_digits -= 3
    elif number >= _SIGN_2:
        significant_digits -= 2
    elif number >= 1:
        significant_digits -= 1
    n_decimals = max(significant_digits, 0)
    try:
        formatted_number = locale.format_string(_LOCALE_MAP[n_decimals], number, grouping=True)
    except KeyError:
        raise ValueError(f"Don't know how to format with {n_decimals}") from None

    return formatted_number


def fmt_number_decimals(number: float | int, n_decimals: int) -> str:
    """Format a number with given number decimals with locale.

    >>> fmt_number_decimals(0.1, 0)
    '< 1'
    >>> fmt_number_decimals(0.1, 1)
    '0.1'
    >>> fmt_number_decimals(0.01, 1)
    '< 0.1'
    """
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
