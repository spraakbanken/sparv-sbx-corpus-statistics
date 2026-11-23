import math
import pickle

from running_stats.running_stats import RunningMeanVar

from sbx_corpus_statistics import exporters


class FakeAnnotation:
    """Stand-in for a Sparv annotation of one source file."""

    def __init__(self, name: str, values: list[str]) -> None:
        self.name = name
        self.values = values

    def read(self) -> list[str]:
        return self.values


def _count(values: dict[str, list[str]]) -> exporters._Counts:
    annotations = [FakeAnnotation(name, vals) for name, vals in values.items()]
    selection = exporters._select_attributes(annotations, "segment.token", {})
    return exporters._count_source_file("file", selection, FakeAnnotation("segment.token", values["segment.token"]))


FILE_1 = {
    "text": ["en text", "en annan text"],
    "segment.token": ["en", "text", "en", "annan", "text"],
    "text:sets": ["|a|bb|", "|", "|ccc|a|"],
}
FILE_2 = {
    "text": ["tredje text"],
    "segment.token": ["tredje", "text", "ny"],
    "text:sets": ["|bb|dddd|", "|", "|a|"],
}


def _assert_same(expected: object, actual: object, path: str = "") -> None:
    """Assert that nested dicts are equal, with keys in the same order and floats almost equal."""
    if isinstance(expected, dict):
        assert list(expected) == list(actual), path
        for key in expected:
            _assert_same(expected[key], actual[key], f"{path}/{key}")
    elif isinstance(expected, RunningMeanVar):
        assert expected.num_values == actual.num_values, path
        assert math.isclose(expected.M1, actual.M1, rel_tol=1e-12, abs_tol=1e-12), path
        assert math.isclose(expected.M2, actual.M2, rel_tol=1e-12, abs_tol=1e-12), path
    else:
        assert expected == actual, path


def test_merged_counts_equal_counting_all_values_at_once() -> None:
    # Counting two source files separately and merging them must give the same result as counting all values in one
    # go (which is what stat_highlights did before counting each source file separately)
    together = _count({name: FILE_1[name] + FILE_2[name] for name in FILE_1})

    merged = exporters._Counts()
    for file_values in (FILE_1, FILE_2):
        # Partial counts are saved to file between the steps
        merged.merge(pickle.loads(pickle.dumps(_count(file_values).to_plain())))

    _assert_same(together.to_plain(), merged.to_plain())


def test_words_are_counted_once_per_corpus() -> None:
    merged = exporters._Counts()
    merged.merge(_count(FILE_1).to_plain())
    merged.merge(_count(FILE_2).to_plain())

    words = merged.attribute_stats["text:sets"][exporters.WORDS]
    # "a" and "bb" occur in both files but are only counted once; empty values are counted every time
    assert words == {"a": 1, "bb": 1, exporters.EMPTY: 2, "ccc": 1, "dddd": 1}
    # Length of each distinct word once, plus 0 for each empty value
    length_of_words = merged.stats["text"]["text:sets length (of words)"]
    assert length_of_words.num_values == 6  # noqa: PLR2004
    assert math.isclose(length_of_words.mean(), (1 + 2 + 3 + 4) / 6)
