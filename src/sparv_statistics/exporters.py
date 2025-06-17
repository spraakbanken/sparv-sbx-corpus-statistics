"""Sparv exporter computing corpus statistics."""

import json
import locale
import math
import operator
import os
import typing as t
from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path
from tempfile import NamedTemporaryFile, _TemporaryFileWrapper
from typing import Any, TextIO, TypedDict, TypeVar

import sparv.api as sparv_api
from running_stats.running_stats import RunningMeanVar
from sparv.api import (
    AllSourceFilenames,
    AnnotationAllSourceFiles,
    Corpus,
    Export,
    ExportAnnotationsAllSourceFiles,
    SourceAnnotationsAllSourceFiles,
    exporter,
    util,
)
from sparv.api.classes import Annotation

from sparv_statistics import formatting as f
from sparv_statistics import suc_msd

__all__ = ["set_locale_from_lang"]

logger = sparv_api.get_logger(__name__)

TOKEN_COUNT: str = "TOKEN_COUNT"
RAW: str = "raw"
EMPTY: str = "<EMPTY>"
WORDS: str = "words"
WORDS_NON_EMPTY: str = "words (non-empty)"
NUM_TOPS: int = 20
TOP_NUM_LABEL: str = f"top_{NUM_TOPS}"


class Stats(TypedDict):
    """Stats for attribute_name."""

    stats: RunningMeanVar
    toplist: list[str]


T = TypeVar("T")


_SUC_FEAT_MAP: dict[str, str] = {
    "UTR": "Genus",
    "NEU": "Genus",
    "MAS": "Genus",
    "UTR/NEU": "Genus",
    "SIN": "Numerus",
    "PLU": "Numerus",
    "SIN/PLU": "Numerus",
    "IND": "Definite",
    "DEF": "Definite",
    "IND/DEF": "Definite",
    "NOM": "Noun form",
    "GEN": "Noun form",
    "SMS": "Noun form",
    "POS": "Comparation",
    "KOM": "Comparation",
    "SUV": "Comparation",
    "SUB": "Part of Sentence",
    "OBJ": "Part of Sentence",
    "SUB/OBJ": "Part of Sentence",
    "PRS": "Verb form",
    "PRT": "Verb form",
    "INF": "Verb form",
    "SUP": "Verb form",
    "IMP": "Verb form",
    "AKT": "Verb form",
    "SFO": "Verb form",
    "KON": "Verb form",
    "PRF": "Verb form",
}

_MSD_MAP: dict[str, dict[str, str]] = {
    "en": {
        "AB": "Adverb",
        "DT": "Determiner",
        "HA": "Interrogative/Relative Adverb",
        "HD": "Interrogative/Relative Determiner",
        "HP": "Interrogative/Relative Pronoun",
        "HS": "Interrogative/Relative Possessive",
        "IE": "Infinitive Marker",
        "IN": "Interjection",
        "JJ": "Adjective",
        "KN": "Conjunction",
        "NN": "Noun",
        "PC": "Participle",
        "PL": "Particle",
        "PM": "Proper Noun",
        "PN": "Pronoun",
        "PP": "Preposition",
        "PS": "Possessive",
        "RG": "Cardinal Number",
        "RO": "Ordinal Number",
        "SN": "Subjunction",
        "UO": "Foreign Word",
        "VB": "Verb",
        "UTR": "Non-neuter (Uter)",
        "NEU": "Neuter",
        "MAS": "Masculine",
        "UTR/NEU": "Underspecified",
        "SIN": "Singular",
        "PLU": "Plural",
        "SIN/PLU": "Underspecified",
        "IND": "Indefinite",
        "DEF": "Definite",
        "IND/DEF": "Underspecified",
        "-": "Unspecified",
        "NOM": "Nominative",
        "GEN": "Genitive",
        "SMS": "Compound",
        "POS": "Positive",
        "KOM": "Comparative",
        "SUV": "Superlative",
        "SUB": "Subject",
        "OBJ": "Object",
        "SUB/OBJ": "Underspecified",
        "PRS": "Present",
        "PRT": "Preterite",
        "INF": "Infinitive",
        "SUP": "Supine",
        "IMP": "Imperative",
        "AKT": "Active",
        "SFO": "S-form",
        "KON": "Subjunctive",
        "PRF": "Perfect participle",
        "AN": "Abbreviation",
        "MAD": "Major Delimiter",
        "MID": "Minor Delimiter",
        "PAD": "Pairwise Delimiter",
    },
    "sv": {
        "AB": "Adverb",
        "DT": "Determinerare, bestämningsord",
        "HA": "Frågande/relativt adverb",
        "HD": "Frågande/relativ bestämning",
        "HP": "Frågande/relativt pronomen",
        "HS": "Frågande/relativt possessivuttryck",
        "IE": "Infinitivmärke",
        "IN": "Interjektion",
        "JJ": "Adjektiv",
        "KN": "Konjunktion",
        "NN": "Substantiv",
        "PC": "Particip",
        "PL": "Partikel",
        "PM": "Egennamn",
        "PN": "Pronomen",
        "PP": "Preposition",
        "PS": "Possessivuttryck",
        "RG": "Räkneord: grundtal",
        "RO": "Räkneord: ordningstal",
        "SN": "Subjunktion",
        "UO": "Utländskt ord",
        "VB": "Verb",
        "UTR": "Utrum",
        "NEU": "Neutrum",
        "MAS": "Maskulinum",
        "UTR/NEU": "Underspecificerat",
        "-": "Ospecificerat",
        "SIN": "Singularis",
        "PLU": "Pluralis",
        "SIN/PLU": "Underspecificerat",
        "IND": "Obestämd form",
        "DEF": "Bestämd form",
        "IND/DEF": "Underspecificerat",
        "NOM": "Grundform",
        "GEN": "Genitiv",
        "SMS": "Sammansättning",
        "POS": "Positiv",
        "KOM": "Komparativ",
        "SUV": "Superlativ",
        "SUB": "Subjekt",
        "OBJ": "Objekt",
        "SUB/OBJ": "Underspecificerat",
        "PRS": "Presens",
        "PRT": "Preteritum (imperfekt)",
        "INF": "Infinitiv",
        "SUP": "Supinum",
        "IMP": "Imperativ",
        "AKT": "Aktiv diates",
        "SFO": "S-form (passivum, deponens)",
        "KON": "Konjunktiv",
        "PRF": "Perfekt particip",
        "AN": "Förkortning",
        "MAD": "Meningsskiljande interpunktion",
        "MID": "Interpunktion",
        "PAD": "Interpunktion",
    },
}


@exporter("Statistics highlights")
def stat_highlights(
    corpus_id: Corpus = Corpus(),
    token: AnnotationAllSourceFiles = AnnotationAllSourceFiles("<token>"),
    # paragraph: AnnotationAllSourceFiles = AnnotationAllSourceFiles("<paragraph>"),
    # sentence: AnnotationAllSourceFiles = AnnotationAllSourceFiles("<sentence>"),
    # word: AnnotationAllSourceFiles = AnnotationAllSourceFiles("[export.word]"),
    # token_word: AnnotationAllSourceFiles = AnnotationAllSourceFiles("<token:word>"),
    source_files: AllSourceFilenames = AllSourceFilenames(),
    export_annotations: ExportAnnotationsAllSourceFiles = ExportAnnotationsAllSourceFiles("export.annotations"),
    source_annotations: SourceAnnotationsAllSourceFiles = SourceAnnotationsAllSourceFiles("export.source_annotations"),
    # word: AnnotationAllSourceFiles = AnnotationAllSourceFiles("[export]"),
    out_highlights_en: Export = Export("sparv_statistics.stat_highlights/stat_highlights_en_[metadata.id].md"),
    out_highlights_sv: Export = Export("sparv_statistics.stat_highlights/stat_highlights_sv_[metadata.id].md"),
    out_all: Export = Export("sparv_statistics.stat_highlights/all_stats_[metadata.id].json"),
) -> None:
    logger.progress(total=len(source_files) + 1)  # type: ignore
    logger.debug("export_annotations = %s", export_annotations)
    logger.debug("export_annotations.items = %s", export_annotations.items)
    logger.debug("source_annotations = %s", source_annotations)
    logger.debug("source_annotations.annotations = %s", source_annotations.annotations)

    # annotations = [(word, None)]
    # Get annotations list and export names
    annotation_list, token_attribute_names, export_names = util.export.get_annotation_names(
        export_annotations,
        source_annotations or [],  # type: ignore
        token_name=token.name,
    )
    logger.debug("annotation_list = %s", annotation_list)
    logger.debug("export_names = %s", export_names)
    logger.debug("token_attribute_names = %s", token_attribute_names)

    lemma_name = None
    for name, export_name in export_names.items():
        if export_name == "lemma":
            lemma_name = name

    attributes = defaultdict(list)
    pos_attribute: AnnotationAllSourceFiles | None = None
    lemma_attribute: AnnotationAllSourceFiles | None = None
    ufeats_attribute: AnnotationAllSourceFiles | None = None
    suc_feats_attribute: AnnotationAllSourceFiles | None = None
    for a in annotation_list:
        if ":" not in a.name:
            attributes["spans"].append(a)
            continue
        if a.name.startswith(f"{token.name}:stanza.pos"):
            if pos_attribute is not None:
                logger.warning(
                    "pos_attribute '%s' already found, overwriting with '%s'",
                    pos_attribute.name,
                    a.name,
                )
            pos_attribute = t.cast(AnnotationAllSourceFiles, a)
        elif a.name.startswith(f"{token.name}:stanza.ufeats"):
            if ufeats_attribute is not None:
                logger.warning(
                    "ufeats_attribute '%s' already found, overwriting with '%s'",
                    ufeats_attribute.name,
                    a.name,
                )
            ufeats_attribute = t.cast(AnnotationAllSourceFiles, a)
        elif a.name.startswith(f"{token.name}:stanza.msd"):
            if suc_feats_attribute is not None:
                if a.name.endswith("info"):
                    continue
                logger.warning(
                    "suc_feats_attribute '%s' already found, overwriting with '%s'",
                    suc_feats_attribute.name,
                    a.name,
                )
            suc_feats_attribute = t.cast(AnnotationAllSourceFiles, a)
        elif a.name == lemma_name:
            if lemma_attribute is not None:
                logger.warning(
                    "lemma_attribute '%s' already found, overwriting with '%s'",
                    lemma_attribute.name,
                    a.name,
                )
            lemma_attribute = t.cast(AnnotationAllSourceFiles, a)
        annotation, _attribute = a.name.split(":")
        attributes[annotation].append(a)

    logger.debug(
        "pos_attribute = %s",
        pos_attribute.name if pos_attribute is not None else "None",
    )
    logger.debug(
        "lemma_attribute = %s",
        lemma_attribute.name if lemma_attribute is not None else "None",
    )
    logger.debug("attributes=%s", attributes)
    # Get all token and struct annotations (except the span annotations)
    token_attributes = [
        a for a in annotation_list if a.name.startswith(token.name) and a.attribute_name in token_attribute_names
    ]
    struct_attributes = [a for a in annotation_list if ":" in a.name and a.attribute_name not in token_attributes]
    struct_spans = [a for a in annotation_list if ":" not in a.name and a.attribute_name not in token_attributes]
    logger.debug("token_attributes = %s", token_attributes)
    logger.debug("struct_attributes = %s", struct_attributes)
    logger.debug("struct_spans = %s", struct_spans)

    # Calculate token frequencies
    # freq_dict = defaultdict(int)
    # struct_freqs = defaultdict(lambda: defaultdict(int))
    # token_freqs = defaultdict(lambda: defaultdict(int))
    freqs: dict[str, dict[str, dict[str, int]]] = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    stats: dict[str, dict[str, RunningMeanVar]] = defaultdict(lambda: defaultdict(RunningMeanVar))
    pos_lemma_freqs: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    pos_lemma_freqs_flat: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    ufeat_pos_freqs_flat: dict[str, dict[str, dict[str, int]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(int))
    )
    pos_ufeats_freqs_flat: dict[str, dict[str, dict[str, int]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(int))
    )
    pos_suc_feats_freqs_flat: dict[str, dict[str, dict[str, int]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(int))
    )
    pos_token_freqs: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    attribute_temp_stat_file: dict[str, _TemporaryFileWrapper[str]] = {}
    # defaultdict(
    #     lambda: NamedTemporaryFile(mode="w+t")
    # )

    for source_file in source_files:  # noqa: PLR1702
        for attribute_name, attribute_list in attributes.items():
            for attribute in attribute_list:
                logger.debug(
                    "attribute.name=%s, attribute_temp_stat_file=%s",
                    attribute.name,
                    attribute_temp_stat_file.keys(),
                )
                attribute_stats = {}
                if attribute.name in attribute_temp_stat_file:
                    logger.debug(
                        "Loading stats for '%s' from '%s'",
                        attribute.name,
                        attribute_temp_stat_file[attribute.name].name,
                    )

                    attribute_temp_stat_file[attribute.name].seek(0, os.SEEK_SET)
                    attribute_stats = json.load(attribute_temp_stat_file[attribute.name])
                    logger.debug(
                        "Read attribute_stats=%s for '%s'",
                        attribute_stats,
                        attribute.name,
                    )
                else:
                    attribute_temp_stat_file[attribute.name] = NamedTemporaryFile(  # noqa: SIM115
                        mode="w+t",
                        delete=False,
                        encoding="utf-8",
                    )
                try:
                    logger.debug(
                        "About to read attrbute='%s' from source_file='%s'",
                        attribute.name,
                        source_file,
                    )
                    for annot in attribute(source_file).read():
                        # logger.debug("attribute_name=%s", attribute_name)
                        # logger.debug(
                        #     "attribute.name=%s, annot=%s, type(annot)=%s",
                        #     attribute.name,
                        #     annot,
                        #     type(annot),
                        # )
                        if attribute.name in {
                            "segment.sentence",
                            "segment.paragraph",
                            "p",
                            "file",
                            "dokument",
                            "text",
                        }:
                            freqs[attribute_name][attribute.name][TOKEN_COUNT] += 1
                        else:
                            freqs[attribute_name][attribute.name][annot] += 1
                        if RAW not in attribute_stats:
                            attribute_stats[RAW] = {}
                        if attribute.name in {
                            "segment.sentence",
                            "segment.paragraph",
                            "segment.token",
                            "p",
                            "file",
                            "dokument",
                            "text",
                        }:
                            annot_name = TOKEN_COUNT
                        else:
                            annot_name = annot
                        if annot_name not in attribute_stats[RAW]:
                            attribute_stats[RAW][annot_name] = 0
                        attribute_stats[RAW][annot_name] += 1
                        if attribute.name.startswith("text:readability"):
                            pass
                        else:
                            stats[attribute_name][f"{attribute.name} length (characters)"].push(float(len(annot)))

                        if "|" in annot:
                            if WORDS not in attribute_stats:
                                attribute_stats[WORDS] = {}
                            if WORDS_NON_EMPTY not in attribute_stats:
                                attribute_stats[WORDS_NON_EMPTY] = {}
                            words = annot.split("|")

                            if len(words) < 3:  # noqa: PLR2004
                                if EMPTY not in attribute_stats[WORDS]:
                                    attribute_stats[WORDS][EMPTY] = 0
                                attribute_stats[WORDS][EMPTY] += 1
                                stats[attribute_name][f"{attribute.name} length (of words)"].push(0.0)
                                stats[attribute_name][f"{attribute.name} length (in words)"].push(1.0)
                            else:
                                stats[attribute_name][f"{attribute.name} length (in words)"].push(float(len(words) - 2))
                                stats[attribute_name][f"{attribute.name} length (in non-empty words)"].push(
                                    float(len(words) - 2)
                                )
                                if attribute.name == "segment.token:wsd.sense":
                                    words = [word.split(":")[0] for word in words]
                                for word_ in words:
                                    if not word_:
                                        continue
                                    if word_ not in attribute_stats[WORDS]:
                                        attribute_stats[WORDS][word_] = 0
                                        if word_ not in attribute_stats[WORDS_NON_EMPTY]:
                                            attribute_stats[WORDS_NON_EMPTY][word_] = 0
                                            attribute_stats[WORDS][word_] += 1
                                            attribute_stats[WORDS_NON_EMPTY][word_] += 1
                                            stats[attribute_name][f"{attribute.name} length (of words)"].push(
                                                float(len(word_))
                                            )
                                    stats[attribute_name][f"{attribute.name} length (of non-empty words)"].push(
                                        float(len(word_))
                                    )

                        if attribute.name == "segment.sentence":
                            stats[attribute_name][f"{attribute.name} length (in words)"].push(
                                float(len(annot.split(" ")))
                            )
                        elif attribute.name.startswith("text:readability"):
                            annot_value = float(annot)
                            # check if NaN or Inf
                            if not math.isnan(annot_value) and not math.isinf(annot_value):
                                logger.debug("%s = %s", attribute.name, annot_value)
                                stats[attribute_name][f"{attribute.name} value"].push(annot_value)
                                if (
                                    stats[attribute_name][f"{attribute.name} value"].M1
                                    != stats[attribute_name][f"{attribute.name} value"].M1
                                ):
                                    logger.warning(
                                        "Found NaN! stats[%s][%s].M1 = %s",
                                        attribute_name,
                                        f"{attribute.name} value",
                                        stats[attribute_name][f"{attribute.name} value"].M1,
                                    )

                        # elif attribute.name.startswith(f"{token.name}:stanza.msd"):
                        #     stats[attribute_name][f"{attribute.name} "]
                except FileNotFoundError as exc:
                    logger.warning("Suppressed error: %s", repr(exc))
                logger.debug(
                    "Writing temporary stats=%s for attribute=%s",
                    attribute_stats,
                    attribute.name,
                )
                attribute_temp_stat_file[attribute.name].seek(0, os.SEEK_SET)
                json.dump(attribute_stats, attribute_temp_stat_file[attribute.name])
        if pos_attribute is not None:
            for pos, token_ in zip(pos_attribute(source_file).read(), token(source_file).read(), strict=True):
                pos_token_freqs[pos][token_] += 1
            if lemma_attribute is not None:
                for pos, lemma in zip(
                    pos_attribute(source_file).read(),
                    lemma_attribute(source_file).read(),
                    strict=True,
                ):
                    pos_lemma_freqs[pos][lemma] += 1
                    for lemma_ in lemma.split("|"):
                        if lemma_:
                            pos_lemma_freqs_flat[pos][lemma_] += 1

            if ufeats_attribute is not None:
                for pos, ufeats in zip(
                    pos_attribute(source_file).read(),
                    ufeats_attribute(source_file).read(),
                    strict=True,
                ):
                    for ufeat_value in ufeats.split("|"):
                        if ufeat_value:
                            ufeat, value = ufeat_value.split("=")
                            ufeat_pos_freqs_flat[ufeat][value][pos] += 1
                            pos_ufeats_freqs_flat[pos][ufeat][value] += 1
                        # else:
                        #     pos_ufeats_freqs_flat[pos][ufeat_value]["MISSING"] += 1

        if suc_feats_attribute is not None:
            for msd_raw in suc_feats_attribute(source_file).read():
                msd = suc_msd.parse(msd_raw)
                if isinstance(msd, suc_msd.MsdWPos):
                    if msd.is_abbreviation:
                        pos_suc_feats_freqs_flat[str(msd.pos)]["Abbreviation"]["AN"] += 1
                        continue
                    if msd.degree:
                        pos_suc_feats_freqs_flat[str(msd.pos)]["Degree"][str(msd.degree)] += 1
                    if msd.gender:
                        pos_suc_feats_freqs_flat[str(msd.pos)]["Gender"][str(msd.gender)] += 1
                    if msd.number:
                        pos_suc_feats_freqs_flat[str(msd.pos)]["Number"][str(msd.number)] += 1
                    if msd.definiteness:
                        pos_suc_feats_freqs_flat[str(msd.pos)]["Definiteness"][str(msd.definiteness)] += 1
                    if msd.case:
                        pos_suc_feats_freqs_flat[str(msd.pos)]["Case"][str(msd.case)] += 1
                    if msd.tense:
                        pos_suc_feats_freqs_flat[str(msd.pos)]["Tense"][str(msd.tense)] += 1
                    if msd.voice:
                        pos_suc_feats_freqs_flat[str(msd.pos)]["Voice"][str(msd.voice)] += 1
                    if msd.mood:
                        pos_suc_feats_freqs_flat[str(msd.pos)]["Mood"][str(msd.mood)] += 1
                    if msd.particle_form:
                        pos_suc_feats_freqs_flat[str(msd.pos)]["Particle Form"][str(msd.particle_form)] += 1
                    if msd.pronoun_form:
                        pos_suc_feats_freqs_flat[str(msd.pos)]["Pronoun Form"][str(msd.pronoun_form)] += 1
                elif isinstance(msd, suc_msd.MsdWDelimiter):
                    pos_suc_feats_freqs_flat["Delimiter"]["Delimiter"][str(msd.delimiter)] += 1
                else:
                    raise RuntimeError(f"Unknown MSD={msd}")

        logger.progress()  # type: ignore

    # Collect statistics
    stats_2: dict[str, dict[str, Stats]] = defaultdict(
        lambda: defaultdict(lambda: {"stats": RunningMeanVar(), "toplist": []})
    )
    for attribute_name, stats_file in attribute_temp_stat_file.items():
        stats_file.seek(0, os.SEEK_SET)
        attr_stats: dict[str, dict[str, int]] = json.load(stats_file)
        for attr, afreqs in attr_stats.items():
            for aword, afreq in afreqs.items():
                for _ in range(afreq):
                    stats_2[attribute_name][attr]["stats"].push(float(len(aword)))
            toplist = [x[0] for x in sorted(afreqs.items(), key=operator.itemgetter(1), reverse=True)[:NUM_TOPS]]
            logger.debug(
                "attribute_name=%s, attr=%s, toplist=%s, freqs=%s",
                attribute_name,
                attr,
                toplist,
                afreqs,
            )
            stats_2[attribute_name][attr]["toplist"] = toplist

    # logger.debug("struct_freqs = %s", struct_freqs)
    # logger.debug("token_freqs = %s", token_freqs)
    # logger.debug("freq_dict = %s", freq_dict)
    logger.debug("freqs = %s", freqs)
    logger.debug("stats = %s", stats)
    logger.debug("pos_token_freqs = %s", pos_token_freqs)
    logger.debug("pos_lemma_freqs = %s", pos_lemma_freqs)

    all_stats = _combine_all_stats(stats, stats_2)
    with Path("all_stats.json").open(mode="w", encoding="utf-8") as fp:
        json.dump(all_stats, fp, cls=StatsJsonEncoder)
    with Path("stats2.json").open(mode="w", encoding="utf-8") as fp:
        json.dump(stats_2, fp, cls=StatsJsonEncoder)
    with Path("freqs.json").open(mode="w", encoding="utf-8") as fp:
        json.dump(freqs, fp, cls=StatsJsonEncoder)
    with Path("pos_token_freqs.json").open(mode="w", encoding="utf-8") as fp:
        json.dump(pos_token_freqs, fp, cls=StatsJsonEncoder)
    with Path("pos_lemma_freqs.json").open(mode="w", encoding="utf-8") as fp:
        json.dump(pos_lemma_freqs, fp, cls=StatsJsonEncoder)
    with Path("pos_lemma_freqs_flat.json").open(mode="w", encoding="utf-8") as fp:
        json.dump(pos_lemma_freqs_flat, fp, cls=StatsJsonEncoder)
    with Path("ufeat_pos_freqs_flat.json").open(mode="w", encoding="utf-8") as fp:
        json.dump(ufeat_pos_freqs_flat, fp, cls=StatsJsonEncoder)
    with Path("pos_ufeats_freqs_flat.json").open(mode="w", encoding="utf-8") as fp:
        json.dump(pos_ufeats_freqs_flat, fp, cls=StatsJsonEncoder)
    with Path("pos_suc_feats_freqs_flat.json").open(mode="w", encoding="utf-8") as fp:
        json.dump(pos_suc_feats_freqs_flat, fp, cls=StatsJsonEncoder)
    write_all_stats(
        out_all,
        all_stats=all_stats,
    )
    write_stat_highlights(
        out_highlights_en,
        lang="en",
        corpus_id=corpus_id,
        all_stats=all_stats,
        freqs=freqs,
        pos_token_freqs=pos_token_freqs,
        pos_lemma_freqs_flat=pos_lemma_freqs_flat,
        pos_ufeats_freqs_flat=pos_ufeats_freqs_flat,
        pos_suc_feats_freqs_flat=pos_suc_feats_freqs_flat,
        stats2=stats_2,
        lemma_attribute=lemma_attribute,
    )
    write_stat_highlights(
        out_highlights_sv,
        lang="sv",
        corpus_id=corpus_id,
        all_stats=all_stats,
        freqs=freqs,
        pos_token_freqs=pos_token_freqs,
        pos_lemma_freqs_flat=pos_lemma_freqs_flat,
        pos_ufeats_freqs_flat=pos_ufeats_freqs_flat,
        pos_suc_feats_freqs_flat=pos_suc_feats_freqs_flat,
        stats2=stats_2,
        lemma_attribute=lemma_attribute,
    )


def running_mean_var_to_dict(m: RunningMeanVar) -> dict[str, int | float]:
    return {
        "number": m.num_values,
        "mean": m.mean(),
        "M2": m.M2,
        "variance": m.variance(),
        "standard derivation": m.standard_deviation(),
    }


def _combine_all_stats(
    stats: dict[str, dict[str, Stats]],
    stats2: dict[str, dict[str, Stats]],
) -> dict[str, dict[str, Stats]]:
    all_stats: dict[str, dict[str, Stats]] = defaultdict(lambda: defaultdict(dict))
    for astats in stats.values():
        for attr, stats_ in astats.items():
            attr_, attr_feat = attr.split(" ", maxsplit=1)
            all_stats[attr_][attr_feat]["stats"] = stats_
        # all_stats.update(astats)
    for attr2, stats2_ in stats2.items():
        for attr, stats_ in stats2_.items():
            all_stats[attr2][attr] = {
                "stats": stats_["stats"],
                "toplist": stats_["toplist"],
            }
    return all_stats


class StatsJsonEncoder(json.JSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, RunningMeanVar):
            return running_mean_var_to_dict(o)
        return super().default(o)


def write_all_stats(
    out: Export,
    all_stats: dict[str, dict[str, Stats]],
) -> None:
    """Write all stats."""
    out_path = Path(out)
    with out_path.open("w") as fp:
        json.dump(all_stats, fp, cls=StatsJsonEncoder)


_T: dict[str, dict[str, str]] = {
    "en": {
        "and": "and",
        "count": "number of",
        "dokument": "documents",
        "dokument length (characters)": "document length, in characters",
        "features_diff_values, singular": "<code>{feat}</code> occurs with {num_diff_values} value: ",
        "features_diff_values, plural": "<code>{feat}</code> occurs with {num_diff_values} different values: ",
        "features_header": "## Features\n",
        "features_multi_tokens": " {multi_feat_token} tokens ({multi_feat_token_percent}%) have multiple values of `{feat}`.\n",  # noqa: E501
        "features_nonempty_tokens": "{feat_token} tokens ({feat_token_percent}%) have a non-empty value of <code>{feat}</code>.\n The feature is used with {num_pos_tags} part-of-speech tags:\n",  # noqa: E501
        "features_overview": "This corpus contains the following features:\n",
        "features_pos_feat": "{freqs} `{pos}` tokens ({freq_percent}% of all `{pos}` tokens) have a non-empty value of `{feat}`.\n",  # noqa: E501
        "features_pos_values": "`{pos}` tokens may have the following values of `{feat}`:\n",
        "features_subheader": "### Features: **{feat}**\n",
        "file": "files",
        "file length (characters)": "file length, in characters",
        "header": "# Corpus Statistics of {corpus_id}\n",
        "instances": "instances",
        "length (characters)": "length (characters)",
        "length (in words)": "length (in words)",
        "missing": "<missing>",
        "morphology_header": "## Morphology\n",
        "morphology_msd_header": "### Morphosyntactic descriptors (MSD)\n",
        "morphology_msd_table_header": "MSD | Frequency | Percent\n",
        "morphology_no_msd": "This corpus does not contain any morphosyntactic descriptors.\n",
        "no features for pos": "{pos_tag} not present among features\n",
        "no POS": "Found no POS tags.\n",
        "of non-empty": "of non-empty",
        "POS_descr": "There are {pos_tag_freqs} ({pos_tag_precent}%) `{pos_tag}` tokens. Out of {pos_stats_num_tags} observed tags, the rank of `{pos_tag}` is: {pos_stats_rank_of_tokens} in number of tokens.\n",  # noqa: E501
        "POS_header": "## POS Tags\n",
        "POS_no_lemmas": "Contains no base forms with `{pos_tag}`.\n",
        "POS_subheader": "### POS Tag: **{pos_tag}**\n",
        "POS_tags_description": "The following POS tags is present in this corpus:\n",
        "POS_top_lemmas": "The {number} most frequent `{pos_tag}` base forms: {top_lemmas}\n",
        "POS_top_tokens": "The {number} most frequent `{pos_tag}` tokens: {top_tokens}\n",
        "pos_distribution_header": "### Distribution of POS-tags\n",
        "pos_distribution_table_header": "Part-Of-Speech | Frequency | Percent (%)\n",
        "pos_feature_overview": "#### POS Tag: **{pos_tag}**, features\n",
        "pos_feature_text": "\n",
        "readability_header": "## Readability\n",
        "readability_scores": "This corpus has the following readability scores:\n",
        "readability_value_row": "- mean value for **{score}** is {mean:.2f} and standard deviation is {std:.2f}\n",
        "score is": "score is",
        "segment.paragraph": "paragraphs",
        "segment.paragraph length (characters)": "paragraph length, in characters",
        "segment.sentence": "sentences",
        "segment.sentence length (characters)": "sentence length, in characters",
        "segment.sentence length (in words)": "sentence length, in words",
        "segment.token": "tokens",
        "segment.token length (characters)": "token length, in characters",
        "stats_header": "## Overview\n",
        "stats_table_header": "Feature | Number | Mean | Standard deviation\n",
        "stats_table_readability": "READABILITY | ---: | ---: | ---:\n",
        "text": "texts",
        "text length (characters)": "text length, in characters",
        "tokenization_header": "## Tokenization and Word Segmentation\n",
        "tokenization_paragraphs": "- This corpus contains {num_paragraphs} paragraphs in {num_documents} documents.\n",
        "tokenization_sentences": "- This corpus contains {num_sentences} sentences and {num_tokens} tokens.\n",
        # "tokenization_sentences": "- This corpus contains {num_sentences} sentences and {num_tokens} tokens, where {unique_tokens} tokens are unique.\n",  # noqa: E501 TODO: track unique tokens, https://github.com/spraakbanken/sparv-statistics/issues/12
        "tokenization_texts": "- This corpus is built from {num_texts} texts, over {num_documents} documents, in {num_files} files.\n",  # noqa: E501
        "top_lemmas": "## Top 10 base forms\n",
        "value": "value",
        "Degree": "Degree",
        "Gender": "Gender",
        "Number": "Number",
        "Definiteness": "Definiteness",
        "Case": "Case",
        "Tense": "Tense",
        "Voice": "Voice",
        "Mood": "Mood",
        "Particle Form": "Particle Form",
        "Pronoun Form": "Pronoun Form",
        "Delimiter": "Delimiter",
        "Abbreviation": "Abbreviation",
    },
    "sv": {
        "and": "och",
        "count": "antal",
        "Count": "Antal",
        "dokument length (characters)": "dokumentlängd, i tecken",
        "feature": "särdrag",
        "features": "särdrag",
        "features_diff_values, singular": "<code>{feat}</code> förekommer med {num_diff_values} värde: ",
        "features_diff_values, plural": "<code>{feat}</code> förekommer med {num_diff_values} olika värden: ",
        "features_header": "## Särdrag\n",
        "features_multi_tokens": " {multi_feat_token} tokens ({multi_feat_token_percent}%) har multipla värden av <code>{feat}</code>.\n",  # noqa: E501
        "features_nonempty_tokens": "{feat_token} tokens ({feat_token_percent}%) har ett icke-tomt värde av <code>{feat}</code>.\n Detta särdrag är använt tillsammans med {num_pos_tags} ordklasser:\n",  # noqa: E501
        "features_overview": "Denna korpus innehåller följande särdrag:\n",
        "features_pos_feat": "{freqs} `{pos}` tokens ({freq_percent}% av alla `{pos}` tokens) har `{feat}`.\n",
        "features_pos_values": "`{pos}` tokens har följande värden av `{feat}`:\n",
        "features_subheader": "### Särdrag: **{feat}**\n",
        "file": "filer",
        "file length (characters)": "fillängd, i tecken",
        "header": "# Korpusstatistik för {corpus_id}\n",
        "instances": "instanser",
        "length (characters)": "längd (tecken)",
        "length (in words)": "längd (i ord)",
        "missing": "<saknas>",
        "morphology_header": "## Morfologi\n",
        "morphology_msd_header": "### Fördelning av ordklassernas särdrag\n",
        "morphology_msd_table_header": "Ordklassernas särdrag | Frequency | Percent\n",
        "morphology_no_msd": "Denna korpus innehåller inga morfosyntaktiska deskriptorer.\n",
        "no features for pos": "Hittade inga särdrag för {pos_tag}\n",
        "no POS": "Hittade inga ordklasser.\n",
        "of non-empty": "av icke-tomma",
        "Part of Speech": "Ordklass",
        "Percentage": "Procentandel",
        "POS_descr": "Det finns {pos_tag_freqs} ({pos_tag_precent}%) `{pos_tag}` tokens. Av {pos_stats_num_tags} observerade ordklasser, så är `{pos_tag}`:s rank: {pos_stats_rank_of_tokens} i antalet tokens.\n",  # noqa: E501
        "POS_header": "## Ordklasser\n",
        "POS_no_lemmas": "Innhåller inga grundformer med `{pos_tag}`.\n",
        "POS_subheader": "### Ordklass: **{pos_tag}**\n",
        "POS_tags_description": "Följande ordklasser förekommer i korpusen:\n",
        "POS_top_lemmas": "De {number} mest frekventa `{pos_tag}` grundformer: {top_lemmas}\n",
        "POS_top_tokens": "De {number} mest frekventa `{pos_tag}` tokens: {top_tokens}\n",
        "pos_distribution_header": "### Fördelning av ordklasser\n",
        "pos_distribution_table_header": "Ordklass | Antal | Andel (%)\n",
        "pos_feature_overview": "#### Ordklass: **{pos_tag}**, särdrag\n",
        "pos_feature_text": "\n",
        "readability_header": "## Läsbarhet\n",
        "readability_scores": "Denna korpus har följande läsbarhetsindex:\n",
        "readability_value_row": "- medelvärdet för **{score}** är {mean:.2f} och standardavikelsen är {std:.2f}\n",
        "score is": "värde är",
        "segment.paragraph": "stycken",
        "segment.paragraph length (characters)": "styckeslängd, i tecken",
        "segment.sentence": "meningar",
        "segment.sentence length (characters)": "meningslängd, i tecken",
        "segment.sentence length (in words)": "meningslängd, i ord",
        "segment.token": "tokens",
        "segment.token length (characters)": "tokenlängd, i tecken",
        "stats_header": "## Översikt\n",
        "stats_table_header": "Egenskap | Värde | Medelvärde | Standardavvikelse\n",
        "stats_table_readability": "LÄSBARHET | ---: | ---: | ---:\n",
        "text": "texter",
        "text length (characters)": "textlängd, i tecken",
        "tokenization_header": "## Tokenisering och segmentering\n",
        "tokenization_paragraphs": "- Denna korpus innehåller {num_paragraphs} stycken i {num_documents} dokument.\n",
        "tokenization_sentences": "- Denna korpus innehåller {num_sentences} meningar och {num_tokens} tokens.\n",
        # "tokenization_sentences": "- Denna korpus innehåller {num_sentences} meningar och {num_tokens} tokens, varav {unique_tokens} tokens är unika.\n",  # noqa: E501 TODO: track unique tokens, https://github.com/spraakbanken/sparv-statistics/issues/12
        "tokenization_texts": "- Denna korpus är skapad från {num_texts} texter, i {num_documents} dokument, som finns i {num_files} filer.\n",  # noqa: E501
        "top_lemmas": "## Topp-10 grundformer\n",
        "value": "värde",
        "Yes": "Ja (`Yes`)",
        "Degree": "Komparation",
        "Gender": "Genus",
        "Number": "Numerus",
        "Definiteness": "Bestämdhet",
        "Case": "Kasus",
        "Tense": "Tempus",
        "Voice": "Diates",
        "Mood": "Modus",
        "Particle Form": "Partikelform",
        "Pronoun Form": "Pronomform",
        "Delimiter": "Interpunktion",
        "Abbreviation": "Förkortning",
    },
}


def _t(lang: str, key: str) -> str:
    return _T[lang].get(key) or key


def _format_description(name: str, measure: str, lang: str) -> str:
    name_translated = _T[lang].get(name, name)
    measure_translated = _T[lang][measure]
    return f"{measure_translated} {name_translated} (`{name}`)"


def _format_description_long(name: str, measure: str, lang: str) -> str:
    # name_translated = _T[lang].get(name) or name
    # measure_translated = _T[lang][measure]
    format_key = f"{name} {measure}"
    key_formatted = _T[lang].get(format_key) or format_key
    return f"{key_formatted} (`{name}`)"


_LANG_TO_LOCALE_MAP: dict[str, str] = {
    "sv": "sv_SE.UTF-8",
    "en": "en_US.UTF-8",
}


def set_locale_from_lang(lang: str) -> str:
    """Set locale based on lang code.

    Args:
        lang: language code
    """
    return locale.setlocale(locale.LC_ALL, _LANG_TO_LOCALE_MAP[lang])


def write_stat_highlights(
    out: Export,
    corpus_id: Corpus,
    freqs: dict[str, dict[str, dict[str, int]]],
    all_stats: dict[str, dict[str, Stats]],
    pos_token_freqs: dict[str, dict[str, int]],
    pos_lemma_freqs_flat: dict[str, dict[str, int]],
    pos_ufeats_freqs_flat: dict[str, dict[str, dict[str, int]]],
    pos_suc_feats_freqs_flat: dict[str, dict[str, dict[str, int]]],
    lang: str,
    stats2: dict[str, dict[str, Stats]],
    lemma_attribute: t.Any,
) -> None:
    """Write statistcs highlights."""
    out_path = Path(out)
    # logger.debug("freqs=%s", freqs)
    old_locale = locale.getlocale(locale.LC_CTYPE)
    loc = set_locale_from_lang(lang)
    logger.debug("locale.setlocale=%s", loc)
    with Path("freqs.json").open(mode="w", encoding="utf-8") as fp:
        json.dump(freqs, fp)
    with out_path.open("w") as fp:
        # fp.write(f"# Statistics of {corpus_id}\n")
        fp.write(_T[lang]["header"].format(corpus_id=corpus_id))
        fp.write("\n")
        _write_statistical_overview(fp, all_stats, lang=lang)
        fp.write("\n")
        _write_tokenization_and_word_segmentation(fp, all_stats=all_stats, lang=lang)
        fp.write("\n")
        _write_top_10_lemmas(fp, stats2, lemma_attribute, lang=lang)
        fp.write("\n")
        _write_pos_tags(
            fp,
            token_freqs=freqs["segment.token"],
            pos_token_freqs=pos_token_freqs,
            pos_lemma_freqs_flat=pos_lemma_freqs_flat,
            pos_feats_freqs_flat=pos_ufeats_freqs_flat,
            lang=lang,
        )
        # fp.write("\n")
        # _write_features(fp, freqs["segment.token"], ufeat_pos_freqs_flat, lang=lang)
        fp.write("\n")
        _write_suc_features(fp, pos_suc_feats_freqs_flat, lang=lang)
        # fp.write("\n")
        # _write_readability(fp, all_stats, lang=lang)
        fp.write("\n")
        _write_morphology(fp, token_freqs=freqs["segment.token"], lang=lang)
        fp.write("\n")
    locale.setlocale(locale.LC_ALL, old_locale)


def _write_statistical_overview(
    fp: TextIO,
    stats: dict[str, dict[str, Stats]],
    lang: str,
) -> None:
    fp.write(_T[lang]["stats_header"])
    fp.write("\n")
    fp.write(_T[lang]["stats_table_header"])
    fp.write("--- | ---: | ---: | ---:\n")
    _write_attr_stats(fp, "file", stats, lang=lang)
    _write_attr_stats(fp, "text", stats, lang=lang)
    _write_attr_stats(fp, "dokument", stats, lang=lang)
    _write_attr_stats(fp, "segment.paragraph", stats, lang=lang)
    _write_attr_stats(fp, "segment.sentence", stats, lang=lang)
    _write_attr_stats(fp, "segment.token", stats, lang=lang)

    fp.write(_T[lang]["stats_table_readability"])
    _write_readability_attr_stats(fp, "text:readability.lix", stats, lang=lang)
    _write_readability_attr_stats(fp, "text:readability.ovix", stats, lang=lang)
    _write_readability_attr_stats(fp, "text:readability.nk", stats, lang=lang)


def _write_attr_stats(
    fp: TextIO,
    attr: str,
    stats: dict[str, dict[str, Stats]],
    lang: str,
) -> None:
    attr_feat_stats = stats.get(attr)
    if attr_feat_stats is None:
        return

    logger.debug("attr=%s, attr_feat_stats=%s", attr, attr_feat_stats)
    written_stats = set()
    for level, level_stats in attr_feat_stats.items():
        logger.debug("level=%s, level_stats=%s", level, level_stats)
        if level.startswith("length"):
            stats_ = level_stats["stats"]
            # attr_count = f"{attr} {_T[lang]['count']}"
            attr_count = _format_description(attr, "count", lang=lang)
            if attr_count not in written_stats:
                formatted_number = f.fmt_number_signific(stats_.num_values, 0)
                fp.write(f"{attr_count} | {formatted_number} | | \n")
                written_stats.add(attr_count)
            attr_level = _format_description_long(attr, level, lang=lang)

            formatted_number = f.fmt_number_signific(stats_.num_values * stats_.mean(), 0)
            formatted_mean = f.fmt_number_signific(stats_.mean(), 3)
            formatted_std = f.fmt_number_signific(stats_.standard_deviation(), 3)
            fp.write(f"{attr_level} | {formatted_number} | {formatted_mean} | {formatted_std}\n")


def _write_readability_attr_stats(
    fp: TextIO,
    attr: str,
    stats: dict[str, dict[str, Stats]],
    lang: str,
) -> None:
    attr_feat_stats = stats.get(attr)
    if attr_feat_stats is None:
        return

    logger.debug("attr=%s, attr_feat_stats=%s", attr, attr_feat_stats)
    # written_stats = set()
    for level, level_stats in attr_feat_stats.items():
        logger.debug("level=%s, level_stats=%s", level, level_stats)
        if level == "value":
            stats_ = level_stats["stats"]

            index = attr.rsplit(".", maxsplit=1)[-1].upper()
            formatted_mean = f.fmt_number_signific(stats_.mean(), 3)
            formatted_std = f.fmt_number_signific(stats_.standard_deviation(), 3)
            fp.write(f"{index} {_T[lang][level]} (`{attr}`) | - | {formatted_mean} | {formatted_std}\n")


def _write_top_10_lemmas(
    fp: TextIO, all_stats: dict[str, dict[str, Stats]], lemma_attribute: Annotation, lang: str
) -> None:
    fp.write(_T[lang]["top_lemmas"])

    fp.write("\n")
    if lemma_attribute.name in all_stats:
        lemma_stats = all_stats[lemma_attribute.name]
        lemma_stats_toplist = None
        if WORDS_NON_EMPTY in lemma_stats:
            lemma_stats_toplist = lemma_stats[WORDS_NON_EMPTY]["toplist"]
        elif WORDS in lemma_stats:
            lemma_stats_toplist = lemma_stats[WORDS]["toplist"]
        elif "raw" in lemma_stats:
            lemma_stats_toplist = lemma_stats["raw"]["toplist"]
        if lemma_stats_toplist:
            fp.writelines(f"{i}. {lemma}\n" for i, lemma in enumerate(lemma_stats_toplist[:10], start=1))


def _write_pos_tags(
    fp: TextIO,
    token_freqs: dict[str, dict[str, int]],
    pos_token_freqs: dict[str, dict[str, int]],
    pos_lemma_freqs_flat: dict[str, dict[str, int]],
    pos_feats_freqs_flat: dict[str, dict[str, dict[str, int]]],
    lang: str,
) -> None:
    logger.debug("token_freqs=%s", token_freqs)
    # fp.write("## POS Tags\n")
    fp.write(_T[lang]["POS_header"])
    fp.write("\n")
    fp.write(_T[lang]["POS_tags_description"])
    pos_freqs = None
    for key, freqs in token_freqs.items():
        if key.startswith("segment.token:stanza.pos"):
            pos_freqs = freqs
            break

    if pos_freqs is None:
        logger.warning("Found no POS tags")
        # fp.write("Found no POS tags\n")
        fp.write(_T[lang]["no POS"])
        return

    pos_stats = PosStats(pos_freqs)
    # pos_feature_stats = PosFeatureStats(feat_pos_freqs_flat)
    fp.write(" - ".join(pos_stats.tags))
    fp.write("\n")
    fp.write("\n")
    _write_pos_distribution(fp, pos_stats=pos_stats, lang=lang)
    fp.write("\n")
    for pos_tag in pos_stats.tags:
        fp.write("\n")
        _write_pos_tag(
            fp,
            pos_tag=pos_tag,
            pos_stats=pos_stats,
            pos_token_freqs=pos_token_freqs,
            pos_lemma_freqs_flat=pos_lemma_freqs_flat,
            lang=lang,
        )
        fp.write("\n")
        _write_pos_feature_overview(
            fp, pos_tag=pos_tag, pos_stats=pos_stats, pos_feats_freqs_flat=pos_feats_freqs_flat, lang=lang
        )


def _write_features(
    fp: TextIO,
    token_freqs: dict[str, dict[str, int]],
    ufeat_pos_freqs_flat: dict[str, dict[str, dict[str, int]]],
    lang: str,
) -> None:
    # fp.write("## Features\n")
    fp.write(_T[lang]["features_header"])
    features = defaultdict(lambda: defaultdict(int))
    total = 0
    for feats_raw, freq in token_freqs["segment.token:stanza.ufeats"].items():
        total += freq
        feats_values = feats_raw.split("|")
        logger.debug("feats_values=%s", feats_values)
        for feats_value in feats_values:
            if not feats_value:
                continue
            feat, value = feats_value.split("=")
            features[feat][value] += freq

    logger.debug("features=%s", features)
    features_sorted = sorted(features.keys())
    pos_freqs = defaultdict(int)
    for values_pos_freq in ufeat_pos_freqs_flat.values():
        for pos_freq in values_pos_freq.values():
            for pos, freq in pos_freq.items():
                pos_freqs[pos] += freq
    fp.write(" - ".join(features_sorted))
    fp.write("\n")
    fp.write("\n")

    for feat in features_sorted:
        logger.debug("writing ### Features: %s", feat)
        # fp.write(f"### Features: **{feat}**\n")
        fp.write(_T[lang]["features_subheader"].format(feat=feat))
        feat_values = set()
        for values in features[feat]:
            feat_values.update(f"`{value}`" for value in values.split(","))

        # fp.write(f" It occurs with {len(feat_values)} different values: ")
        num_feat_values = len(feat_values)
        if num_feat_values == 1:
            fp.write(_T[lang]["features_diff_values, singular"].format(feat=feat, num_diff_values=num_feat_values))
        else:
            num_feat_values_str = f.fmt_number_signific(num_feat_values, 0)
            fp.write(_T[lang]["features_diff_values, plural"].format(feat=feat, num_diff_values=num_feat_values_str))
        fp.write(", ".join(sorted(feat_values)))
        fp.write(".\n")
        fp.write("\n")
        feat_token = sum(features[feat].values())
        feat_token_percent = round(feat_token / total * 100)
        multi_feat_token = sum(v for k, v in features[feat].items() if "," in k)
        multi_feat_token_percent = round(multi_feat_token / feat_token * 100)
        feat_pos_freqs = defaultdict(int)

        pos_value_freqs = defaultdict(lambda: defaultdict(int))
        for values, value_pos_freq in ufeat_pos_freqs_flat[feat].items():
            for pos, pos_freq in value_pos_freq.items():
                feat_pos_freqs[pos] += pos_freq
                for value in values.split(","):
                    pos_value_freqs[pos][value] += pos_freq
        logger.debug("feat_pos_freqs=%s", feat_pos_freqs)
        logger.debug("pos_value_freqs=%s", pos_value_freqs)
        tmp1 = []
        for pos, freqs in feat_pos_freqs.items():
            tmp1.append((pos, freqs, freqs / total * 100))
        pos_tags = sorted(tmp1, key=operator.itemgetter(1), reverse=True)
        tmp3 = (
            f"`{pos}` ({f.fmt_number_signific(count)}; {f.fmt_number_decimals(perc, 0)}% {_T[lang]['instances']})"
            for pos, count, perc in pos_tags
        )
        pos_tags_str = ", ".join(tmp3)

        fp.write(
            _T[lang]["features_nonempty_tokens"].format(
                feat_token=f.fmt_number_signific(feat_token),
                feat_token_percent=feat_token_percent,
                feat=feat,
                num_pos_tags=len(pos_tags),
                pos_tags=pos_tags_str,
            )
        )
        if multi_feat_token > 0:
            fp.write(
                _T[lang]["features_multi_tokens"].format(
                    multi_feat_token=f.fmt_number_signific(multi_feat_token),
                    multi_feat_token_percent=f.fmt_number_decimals(multi_feat_token_percent, 0),
                    feat=feat,
                )
            )
        fp.write("\n")

        fp.write("\n\n")


def _mklink(level: str, name: str) -> str:
    link = f"#{level}-{name.lower()}"
    link = link.replace(" ", "-")
    return link


def _write_suc_features(
    fp: TextIO,
    pos_suc_feats_freqs_flat: dict[str, dict[str, dict[str, int]]],
    lang: str,
) -> None:
    # fp.write("## Features\n")
    fp.write(_T[lang]["features_header"])
    fp.write("\n")
    fp.write(_T[lang]["features_overview"])

    fp.write("\n")
    features = defaultdict(lambda: defaultdict(int))

    total = 0
    for suc_feats in pos_suc_feats_freqs_flat.values():
        for suc_feat, value_freq in suc_feats.items():
            for value, freq in value_freq.items():
                total += freq
                features[suc_feat][value] += freq
        # features.update(_T[lang][suc_feat] for suc_feat in suc_feats)

    logger.debug("features=%s", features)
    features_sorted = sorted(features.keys(), key=lambda x: _T[lang][x])
    fp.write(
        " - ".join(
            f"[{_t(lang, suc_feat)}]({_mklink(_t(lang, 'features'), _t(lang, suc_feat))})"
            for suc_feat in features_sorted
        )
    )
    fp.write("\n")
    fp.write("\n")

    for feat in features_sorted:
        logger.debug("writing ### Features: %s", feat)
        fp.write(_T[lang]["features_subheader"].format(feat=_T[lang][feat]))

        fp.write("<details>\n")
        fp.write("<summary>\n")
        feat_values = set()
        for values in features[feat]:
            feat_values.update(f"<code>{value}</code>" for value in values.split("/"))

        num_feat_values = len(feat_values)
        if num_feat_values == 1:
            fp.write(
                _T[lang]["features_diff_values, singular"].format(feat=_T[lang][feat], num_diff_values=num_feat_values)
            )
        else:
            num_feat_values_str = f.fmt_number_signific(num_feat_values, 0)
            fp.write(
                _T[lang]["features_diff_values, plural"].format(
                    feat=_T[lang][feat], num_diff_values=num_feat_values_str
                )
            )
        fp.write(", ".join(sorted(feat_values)))
        fp.write(".\n")
        fp.write("\n")

        fp.write("</summary>\n")
        fp.write("\n")
        feat_token = sum(features[feat].values())
        feat_token_percent = round(feat_token / total * 100)
        multi_feat_token = sum(v for k, v in features[feat].items() if "/" in k)
        multi_feat_token_percent = round(multi_feat_token / feat_token * 100)
        feat_pos_freqs = defaultdict(int)

        pos_value_freqs = defaultdict(lambda: defaultdict(int))
        for pos, suc_feat_values in pos_suc_feats_freqs_flat.items():
            for values, freq in suc_feat_values.get(feat, {}).items():
                feat_pos_freqs[pos] += freq
                for value in values.split("/"):
                    pos_value_freqs[pos][value] += freq

        logger.debug("feat_pos_freqs=%s", feat_pos_freqs)
        logger.debug("pos_value_freqs=%s", pos_value_freqs)
        tmp1 = []
        for pos, freqs in feat_pos_freqs.items():
            tmp1.append((pos, freqs, freqs / total * 100))
        pos_tags = sorted(tmp1, key=operator.itemgetter(1), reverse=True)

        fp.write(
            _T[lang]["features_nonempty_tokens"].format(
                feat_token=f.fmt_number_signific(feat_token),
                feat_token_percent=feat_token_percent,
                feat=_T[lang][feat],
                num_pos_tags=len(pos_tags),
            )
        )
        _write_table_html(
            fp,
            headers=[_t(lang, "Part of Speech"), _t(lang, "Count"), _t(lang, "Percentage")],
            rows=((f"<code>{pos}</code>", count, perc) for pos, count, perc in pos_tags),
            formatters=[lambda x: f.fmt_number_signific(x, 0), lambda x: f"{f.fmt_number_decimals(x, 0)}%"],
        )
        if multi_feat_token > 0:
            fp.write("\n")
            fp.write(
                _T[lang]["features_multi_tokens"].format(
                    multi_feat_token=f.fmt_number_signific(multi_feat_token),
                    multi_feat_token_percent=f.fmt_number_decimals(multi_feat_token_percent, 0),
                    feat=_T[lang][feat],
                )
            )
        fp.write("\n")

        fp.write("</details>\n")
        fp.write("\n\n")


def _write_table_html(
    fp: TextIO,
    headers: list[str],
    rows: Iterable[tuple[str, int | float, ...]],
    formatters: list[t.Callable[[int | float], str]],
) -> None:
    fp.write("<table>\n")
    fp.write("<thead>\n")
    fp.write("<tr>\n")
    fp.writelines(
        "<th{align}>{header}</th>\n".format(align=' align="right"' if i > 0 else "", header=header)
        for i, header in enumerate(headers)
    )
    fp.write("</tr>\n")
    fp.write("</thead>\n")
    fp.write("<tbody>\n")
    n_rows = 0
    totals = []
    for row in rows:
        n_rows += 1
        fp.write("<tr>\n")
        fp.write(f"<td>{row[0]}</td>\n")
        for i, (column, formatter) in enumerate(zip(row[1:], formatters, strict=True)):
            while i >= len(totals):
                totals.append(0.0)
            totals[i] += column
            column_str = formatter(column).replace("<", "&lt;")
            fp.write(f'<td align="right">{column_str}</td>\n')
        fp.write("</tr>\n")
    if n_rows > 1:
        fp.write("<td>Σ</td>\n")
        for column, formatter in zip(totals, formatters, strict=True):
            column_str = formatter(column)
            fp.write(f'<td align="right">{column_str}</td>\n')
    fp.write("</tbody>\n")
    fp.write("</table>\n")


class PosStats:
    """Statistics of POS."""

    def __init__(self, pos_freqs: dict[str, int]) -> None:
        self.freqs = pos_freqs
        self.tags = sorted(pos_freqs.keys())
        self.sum_freqs = sum(pos_freqs.values())
        self.stats = {tag: pos_freqs[tag] / self.sum_freqs * 100 for tag in self.tags}
        self.stats = dict(sorted(self.stats.items(), key=operator.itemgetter(1), reverse=True))

    @property
    def num_tags(self) -> int:
        return len(self.tags)

    def rank_of_tokens(self, tag: str) -> int:
        for rank, stat_tag in enumerate(self.stats, 1):
            if stat_tag == tag:
                return rank

        raise KeyError(f"Unknown tag '{tag}'")


class PosFeatureStats:
    """POS stats divided per feature."""

    def __init__(self, feat_pos_freqs_flat: dict[str, dict[str, dict[str, int]]]) -> None:
        pos_feat_stats: dict[str, dict[str, dict[str, int]]] = {}
        for feat, feat_values in feat_pos_freqs_flat.items():
            for feat_value, pos_values in feat_values.items():
                for pos, pos_feat_value in pos_values.items():
                    pos_feat_stats_ = pos_feat_stats.get(pos, {})
                    feat_stats_ = pos_feat_stats_.get(feat, {})
                    feat_stats_[feat_value] = pos_feat_value
                    pos_feat_stats_[feat] = feat_stats_
                    pos_feat_stats[pos] = pos_feat_stats_
        self.stats = pos_feat_stats
        logger.debug("pos_feature_stats=%s", self.stats)


def _write_pos_distribution(fp: TextIO, pos_stats: PosStats, lang: str) -> None:
    fp.write(_T[lang]["pos_distribution_header"])

    fp.write("\n")
    fp.write(_T[lang]["pos_distribution_table_header"])
    fp.write("--- | ---: | ---:\n")
    fp.writelines(
        f"{pos_tag} | {pos_stats.freqs[pos_tag]} | {pos_tag_percent:.2f}%\n"
        for pos_tag, pos_tag_percent in pos_stats.stats.items()
    )


def _write_pos_tag(
    fp: TextIO,
    pos_tag: str,
    pos_stats: PosStats,
    pos_token_freqs: dict[str, dict[str, int]],
    pos_lemma_freqs_flat: dict[str, dict[str, int]],
    lang: str,
) -> None:
    # fp.write(f"### POS Tags: **{pos_tag}**\n")
    fp.write(_T[lang]["POS_subheader"].format(pos_tag=pos_tag))
    pos_tag_precent = f.fmt_number_decimals(pos_stats.stats[pos_tag], 0)

    fp.write(
        _T[lang]["POS_descr"].format(
            pos_tag_freqs=f.fmt_number_signific(pos_stats.freqs[pos_tag], 0),
            pos_tag_precent=pos_tag_precent,
            pos_tag=pos_tag,
            pos_stats_num_tags=pos_stats.num_tags,
            pos_stats_rank_of_tokens=pos_stats.rank_of_tokens(pos_tag),
        )
    )

    fp.write("\n")
    if pos_tag in pos_lemma_freqs_flat:
        top_lemmas = sorted(
            pos_lemma_freqs_flat[pos_tag].items(),
            key=operator.itemgetter(1, 0),
            reverse=True,
        )[:5]
        top_lemmas = [f"`{w}`" for w, _f in top_lemmas]
        top_lemmas_str = ", ".join(top_lemmas)
        # fp.write(f"The 5 most frequent `{pos_tag}` lemmas: {top_5_lemmas_flat}\n")
        if len(top_lemmas) == 0:
            fp.write(_T[lang]["POS_no_lemmas"].format(pos_tag=pos_tag))
        else:
            fp.write(
                _T[lang]["POS_top_lemmas"].format(number=len(top_lemmas), pos_tag=pos_tag, top_lemmas=top_lemmas_str)
            )

        fp.write("\n")
    if pos_tag in pos_token_freqs:
        top_tokens = sorted(
            pos_token_freqs[pos_tag].items(),
            key=operator.itemgetter(1, 0),
            reverse=True,
        )[:5]
        top_tokens = [f"`{w}`" for w, _f in top_tokens]
        top_tokens_str = ", ".join(top_tokens)
        # fp.write(f"The 5 most frequent `{pos_tag}` tokens: {top_tokens}\n")
        fp.write(_T[lang]["POS_top_tokens"].format(number=len(top_tokens), pos_tag=pos_tag, top_tokens=top_tokens_str))
        fp.write("\n")


def _write_pos_feature_overview(
    fp: TextIO,
    pos_tag: str,
    pos_stats: PosStats,
    # pos_feature_stats: PosFeatureStats,
    pos_feats_freqs_flat: dict[str, dict[str, dict[str, int]]],
    lang: str,
) -> None:
    # fp.write("#### ")
    fp.write(_T[lang]["pos_feature_overview"].format(pos_tag=pos_tag))

    fp.write(_T[lang]["pos_feature_text"])

    if pos_tag not in pos_feats_freqs_flat:
        # fp.write(f"{pos_tag} not present among features\n")
        fp.write(_T[lang]["no features for pos"].format(pos_tag=pos_tag))
        return
    for feat, feat_values in sorted(pos_feats_freqs_flat[pos_tag].items()):
        logger.debug("feat=%s, feat_values=%s", feat, feat_values)
        freqs = sum(feat_values.values())
        total_pos_freqs = pos_stats.freqs[pos_tag]
        fp.write(f"##### {pos_tag} {_T[lang]['and']} {feat}\n")
        fp.write("\n")
        freq_precent_str = f.fmt_number_decimals(freqs / total_pos_freqs * 100.0, 0)
        fp.write(
            _T[lang]["features_pos_feat"].format(
                freqs=f.fmt_number_signific(freqs, 0),
                pos=pos_tag,
                freq_percent=freq_precent_str,
                feat=feat,
            )
        )
        fp.write("\n")
        fp.write(_T[lang]["features_pos_values"].format(pos=pos_tag, feat=feat))
        fp.write("\n")
        fp.writelines(
            f"- `{_T[lang].get(feat_category) or feat_category}` ({f.fmt_number_signific(feat_value, 0)}; {f.fmt_number_decimals(feat_value / total_pos_freqs * 100, 0)}%)\n"  # noqa: E501
            for feat_category, feat_value in sorted(feat_values.items(), key=operator.itemgetter(1))
        )
        num_missing_tags = total_pos_freqs - freqs
        num_missing_tags_str = f.fmt_number_signific(num_missing_tags, 0)
        if num_missing_tags > 0:
            num_missing_percent = num_missing_tags / total_pos_freqs * 100.0
            num_missing_percent_str = f.fmt_number_decimals(num_missing_percent, 0)
        else:
            num_missing_percent_str = "0"

        fp.write(f"- `{_T[lang]['missing']}` ({num_missing_tags_str}; {num_missing_percent_str}%)\n")
        fp.write("\n")


def _write_tokenization_and_word_segmentation(
    fp: TextIO,
    # freqs: dict[str, dict[str, dict[str, int]]],
    all_stats: dict[str, dict[str, Stats]],
    lang: str,
) -> None:
    # fp.write("## Tokenization and Word Segmentation\n")
    fp.write(_T[lang]["tokenization_header"])
    num_sentences = f.fmt_number_signific(all_stats["segment.sentence"]["raw"]["stats"].num_values, 0)
    # num_tokens, unique_tokens = filter_and_count_total_and_unique(
    #     "segment.token", freqs["segment.token"]
    # )
    num_tokens = f.fmt_number_signific(all_stats["segment.token"]["raw"]["stats"].num_values, 0)
    num_documents = f.fmt_number_signific(all_stats["dokument"]["raw"]["stats"].num_values, 0)
    num_files = f.fmt_number_signific(all_stats["file"]["raw"]["stats"].num_values, 0)
    num_paragraphs = (
        all_stats["segment.paragraph"]["raw"]["stats"].num_values if "segment.paragraph" in all_stats else 0
    )
    num_texts = f.fmt_number_signific(all_stats["text"]["raw"]["stats"].num_values, 0)

    fp.write(
        _T[lang]["tokenization_texts"].format(num_texts=num_texts, num_documents=num_documents, num_files=num_files)
    )
    if num_paragraphs > 0:
        num_paragraphs_str = f.fmt_number_signific(num_paragraphs, 0)
        fp.write(
            _T[lang]["tokenization_paragraphs"].format(num_paragraphs=num_paragraphs_str, num_documents=num_documents)
        )

    fp.write(
        _T[lang]["tokenization_sentences"].format(
            num_sentences=num_sentences,
            num_tokens=num_tokens,
            # unique_tokens=unique_tokens, TODO: track unique tokens, https://github.com/spraakbanken/sparv-statistics/issues/12
        )
    )


# def _write_readability(fp: TextIO, stats: dict[str, dict[str, Stats]], lang: str) -> None:
#     fp.write(_T[lang]["readability_header"])
#     fp.write("\n")
#     fp.write(_T[lang]["readability_scores"])
#     for annot, freqs in stats.items():
#         if annot.startswith("text:readability"):
#             score = annot.rsplit(".", maxsplit=1)[-1]
#             annot_stats: RunningMeanVar = freqs["value"]["stats"]
#             fp.writelines(
#                 _T[lang]["readability_value_row"].format(
#                     score=score, mean=annot_stats.mean(), std=annot_stats.standard_deviation()
#                 )
#             )


def _write_morphology(
    fp: TextIO,
    token_freqs: dict[str, dict[str, int]],
    lang: str,
) -> None:
    # fp.write("## Morphology\n")
    fp.write(_T[lang]["morphology_header"])
    fp.write("\n")
    _write_msd(fp, token_freqs, lang=lang)


def _write_msd(fp: TextIO, token_freqs: dict[str, dict[str, int]], lang: str) -> None:
    fp.write(_T[lang]["morphology_msd_header"])
    fp.write("\n")
    msd_freqs = None
    for annot, freqs in token_freqs.items():
        if annot.startswith("segment.token:stanza.msd"):
            msd_freqs = freqs
            break

    if msd_freqs is None:
        # fp.write("This corpus does not contain any morphosyntatic descriptors.")
        fp.write(_T[lang]["morphology_no_msd"])
        return

    toplevel_freqs = defaultdict(int)
    num_toplevels = 0
    for msd, value in msd_freqs.items():
        toplevel_msd = msd.split(".", maxsplit=1)[0]
        toplevel_freqs[toplevel_msd] += value
        num_toplevels += value

    msds_sorted = sorted(msd_freqs.keys())

    # fp.write("Top-level MSD | Frequency | Percent\n")
    # fp.write(_T[lang]["morphology_msd_top_table_header"])
    # fp.write("--- | --- | ---\n")
    # for msd in sorted(toplevel_freqs.keys()):
    #     msd_percent = toplevel_freqs[msd] / num_toplevels * 100
    #     fp.write(f"{msd} | {toplevel_freqs[msd]} | {msd_percent:.2f}%\n")

    fp.write("\n")
    fp.write("\n")

    num_msds = sum(msd_freqs.values())
    # fp.write("MSD | Frequency | Percent\n")
    fp.write(_T[lang]["morphology_msd_table_header"])
    fp.write("--- | ---: | ---:\n")
    for msd in msds_sorted:
        msd_percent = f.fmt_number_decimals(msd_freqs[msd] / num_msds * 100, 2)
        msd_freqs_str = f.fmt_number_signific(msd_freqs[msd], 0)
        fp.write(f"{msd} | {msd_freqs_str} | {msd_percent}%\n")


def filter_and_count_total_and_unique(label: str, items: dict[str, dict[str, int]]) -> tuple[int, int]:
    unique = 0
    total = 0
    for count in items[label].values():
        unique += 1
        total += count
    return total, unique
