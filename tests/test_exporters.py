import io

import pytest
from sparv.api.classes import Annotation
from syrupy.assertion import SnapshotAssertion

from sparv_statistics import exporters


@pytest.mark.parametrize("lang", ("en", "sv"))
def test_write_statistical_overview(
    lang: str,
    snapshot: SnapshotAssertion,
    all_stats: dict[str, dict[str, exporters.Stats]],
) -> None:
    data = io.StringIO()

    exporters.set_locale_from_lang(lang)
    exporters._write_statistical_overview(data, all_stats, lang=lang)

    assert data.getvalue() == snapshot


@pytest.mark.parametrize("lang", ("en", "sv"))
def test_write_tokenization_and_word_segmentation(
    lang: str,
    snapshot: SnapshotAssertion,
    # freqs: dict[str, dict[str, dict[str, int]]],
    all_stats: dict[str, dict[str, exporters.Stats]],
) -> None:
    data = io.StringIO()

    exporters.set_locale_from_lang(lang)
    exporters._write_tokenization_and_word_segmentation(data, all_stats=all_stats, lang=lang)

    assert data.getvalue() == snapshot


@pytest.mark.parametrize("lang", ("en", "sv"))
def test_write_top_10_lemmas(
    lang: str,
    snapshot: SnapshotAssertion,
    all_stats: dict[str, dict[str, exporters.Stats]],
    lemma_attribute: Annotation,
) -> None:
    data = io.StringIO()

    exporters.set_locale_from_lang(lang)
    exporters._write_top_10_lemmas(data, all_stats, lemma_attribute=lemma_attribute, lang=lang)

    assert data.getvalue() == snapshot


@pytest.mark.parametrize("lang", ("en", "sv"))
def test_write_pos_tags(
    lang: str,
    snapshot: SnapshotAssertion,
    freqs: dict[str, dict[str, dict[str, int]]],
    pos_token_freqs: dict[str, dict[str, int]],
    pos_lemma_freqs_flat: dict[str, dict[str, int]],
    pos_suc_feats_freqs_flat: dict[str, dict[str, dict[str, int]]],
) -> None:
    data = io.StringIO()

    exporters.set_locale_from_lang(lang)
    exporters._write_pos_tags(
        data,
        token_freqs=freqs["segment.token"],
        pos_token_freqs=pos_token_freqs,
        pos_lemma_freqs_flat=pos_lemma_freqs_flat,
        pos_feats_freqs_flat=pos_suc_feats_freqs_flat,
        lang=lang,
    )

    assert data.getvalue() == snapshot


@pytest.mark.parametrize("lang", ("en", "sv"))
def test_write_features(
    lang: str,
    snapshot: SnapshotAssertion,
    freqs: dict[str, dict[str, dict[str, int]]],
    ufeat_pos_freqs_flat: dict[str, dict[str, dict[str, int]]],
) -> None:
    data = io.StringIO()

    exporters.set_locale_from_lang(lang)
    exporters._write_features(
        data,
        token_freqs=freqs["segment.token"],
        ufeat_pos_freqs_flat=ufeat_pos_freqs_flat,
        lang=lang,
    )

    assert data.getvalue() == snapshot


@pytest.mark.parametrize("lang", ("en", "sv"))
def test_write_suc_features(
    lang: str,
    snapshot: SnapshotAssertion,
    pos_suc_feats_freqs_flat: dict[str, dict[str, dict[str, int]]],
) -> None:
    data = io.StringIO()

    exporters.set_locale_from_lang(lang)
    exporters._write_suc_features(
        data,
        pos_suc_feats_freqs_flat=pos_suc_feats_freqs_flat,
        lang=lang,
    )

    assert data.getvalue() == snapshot


# @pytest.mark.parametrize("lang", ("en", "sv"))
# def test_write_readability(
#     lang: str, snapshot: SnapshotAssertion, all_stats: dict[str, dict[str, exporters.Stats]]
# ) -> None:
#     data = io.StringIO()

#     exporters._write_readability(data, stats=all_stats, lang=lang)

#     assert data.getvalue() == snapshot


@pytest.mark.parametrize("lang", ("en", "sv"))
def test_write_morphology(lang: str, snapshot: SnapshotAssertion, freqs: dict[str, dict[str, dict[str, int]]]) -> None:
    data = io.StringIO()

    exporters.set_locale_from_lang(lang)
    exporters._write_morphology(data, token_freqs=freqs["segment.token"], lang=lang)

    assert data.getvalue() == snapshot
