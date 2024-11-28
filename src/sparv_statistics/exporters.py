from sparv.api import AllSourceFilenames, Corpus, Export, exporter


@exporter("Statistics highlights")
def stat_highlights(
    corpus: Corpus = Corpus(),
    source_files: AllSourceFilenames = AllSourceFilenames(),
    out: Export = Export(
        "sparv_statistics.stat_highlight/stat_highlight_[metadata.id].csv"
    ),
) -> None: ...
