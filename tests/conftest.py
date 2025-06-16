import json
import typing as t
from pathlib import Path

import pytest
from running_stats.running_stats import RunningMeanVar
from sparv.api.classes import Annotation
from syrupy.assertion import SnapshotAssertion
from syrupy.extensions.json import JSONSnapshotExtension

from sparv_statistics.exporters import Stats


@pytest.fixture
def snapshot_json(snapshot: SnapshotAssertion) -> SnapshotAssertion:
    return snapshot.use_extension(JSONSnapshotExtension)


@pytest.fixture(scope="session")
def all_stats() -> dict[str, dict[str, Stats]]:
    with Path("assets/all_stats.json").open(encoding="utf-8") as fp:
        all_stats = json.load(fp)

    for outer_stats in all_stats.values():
        for inner_stats in outer_stats.values():
            inner_stats["stats"] = dict_to_running_mean_var(inner_stats["stats"])
    return all_stats


@pytest.fixture
def lemma_attribute() -> Annotation:
    return Annotation(name="segment.token:saldo.baseform2")


def dict_to_running_mean_var(a: dict[str, t.Union[int, float]]) -> RunningMeanVar:
    return RunningMeanVar(num_values=int(a["number"]), M1=a["mean"], M2=a["M2"])


@pytest.fixture(scope="session")
def freqs() -> dict[str, dict[str, dict[str, int]]]:
    with Path("assets/freqs.json").open(encoding="utf-8") as fp:
        return json.load(fp)


@pytest.fixture(scope="session")
def pos_lemma_freqs_flat() -> dict[str, dict[str, int]]:
    with Path("assets/pos_lemma_freqs_flat.json").open(encoding="utf-8") as fp:
        return json.load(fp)


@pytest.fixture(scope="session")
def ufeat_pos_freqs_flat() -> dict[str, dict[str, int]]:
    with Path("assets/ufeat_pos_freqs_flat.json").open(encoding="utf-8") as fp:
        return json.load(fp)


@pytest.fixture(scope="session")
def pos_ufeats_freqs_flat() -> dict[str, dict[str, int]]:
    with Path("assets/pos_ufeats_freqs_flat.json").open(encoding="utf-8") as fp:
        return json.load(fp)


@pytest.fixture(scope="session")
def pos_suc_feats_freqs_flat() -> dict[str, dict[str, int]]:
    with Path("assets/pos_suc_feats_freqs_flat.json").open(encoding="utf-8") as fp:
        return json.load(fp)


@pytest.fixture(scope="session")
def pos_token_freqs() -> dict[str, dict[str, int]]:
    with Path("assets/pos_token_freqs.json").open(encoding="utf-8") as fp:
        return json.load(fp)
