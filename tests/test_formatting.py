import pytest
from syrupy.assertion import SnapshotAssertion

from sparv_statistics import exporters
from sparv_statistics import formatting as f


@pytest.mark.parametrize("lang", ["en", "sv"])
@pytest.mark.parametrize("number", [0.11, 1.1, 11, 111, 1111])
def test_format_float(number: float, lang: str, snapshot: SnapshotAssertion) -> None:
    exporters.set_locale_from_lang(lang)
    value = f.format_number(number)

    assert value == snapshot


@pytest.mark.parametrize("lang", ["en", "sv"])
@pytest.mark.parametrize(("number", "significant_digits"), [(1.1, 5), (1.1, 0)])
def test_format_float_with_significant_digits(
    number: float, significant_digits: int, lang: str, snapshot: SnapshotAssertion
) -> None:
    exporters.set_locale_from_lang(lang)
    value = f.format_number(number, significant_digits)

    assert value == snapshot


@pytest.mark.parametrize("lang", ["en", "sv"])
@pytest.mark.parametrize("number", [1.11111, 0.111111, 0.011111, 0.001111, 0.0001111, 0.00001111, 0.000001111])
@pytest.mark.parametrize("n_decimals", [0, 1, 2, 3, 4, 5])
def test_format_percent_with_n_decimals(number: float, n_decimals: int, lang: str, snapshot: SnapshotAssertion) -> None:
    exporters.set_locale_from_lang(lang)
    value = f.format_percent(number, n_decimals)

    assert value == snapshot
