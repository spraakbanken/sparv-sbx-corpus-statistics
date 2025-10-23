# sparv-sbx-corpus-statistics

[![PyPI version](https://img.shields.io/pypi/v/sparv-sbx-corpus-statistics.svg)](https://pypi.org/project/sparv-sbx-corpus-statistics/)
[![PyPI license](https://img.shields.io/pypi/l/sparv-sbx-corpus-statistics.svg)](https://pypi.org/project/sparv-sbx-corpus-statistics/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/sparv-sbx-corpus-statistics.svg)](https://pypi.org/project/sparv-sbx-corpus-statistics)

[![Maturity badge - level 2](https://img.shields.io/badge/Maturity-Level%202%20--%20First%20Release-yellowgreen.svg)](https://github.com/spraakbanken/getting-started/blob/main/scorecard.md)
[![Stage](https://img.shields.io/pypi/status/sparv-sbx-corpus-statistics.svg)](https://pypi.org/project/sparv-sbx-corpus-statistics/)

[![codecov](https://codecov.io/gh/spraakbanken/sparv-sbx-corpus-statistics/graph/badge.svg?token=DUV4CL6AK2)](https://codecov.io/gh/spraakbanken/sparv-sbx-corpus-statistics)

[![CI(check)](https://github.com/spraakbanken/sparv-sbx-corpus-statistics/actions/workflows/check.yml/badge.svg)](https://github.com/spraakbanken/sparv-sbx-corpus-statistics/actions/workflows/check.yml)
[![CI(release)](https://github.com/spraakbanken/sparv-sbx-corpus-statistics/actions/workflows/release.yml/badge.svg)](https://github.com/spraakbanken/sparv-sbx-corpus-statistics/actions/workflows/release.yml)
[![CI(scheduled)](https://github.com/spraakbanken/sparv-sbx-corpus-statistics/actions/workflows/rolling.yml/badge.svg)](https://github.com/spraakbanken/sparv-sbx-corpus-statistics/actions/workflows/rolling.yml)
[![CI(test)](https://github.com/spraakbanken/sparv-sbx-corpus-statistics/actions/workflows/test.yml/badge.svg)](https://github.com/spraakbanken/sparv-sbx-corpus-statistics/actions/workflows/test.yml)

A [Sparv](https://github.com/spraakbanken/sparv) plugin to collect statistics about a corpus.

## Install

First, install [Sparv](https://github.com/spraakbanken/sparv) as suggested,

with [`pipx`](https://pipx.pypa.io/):

```bash
pipx install sparv
```

or, with [`uv-pipx`](https://github.com/pytgaen/uv-pipx):

```bash
uvpipx install sparv
```

Then install `sparv-sbx-corpus-statistics` with,

**the suggested method**:

```bash
sparv plugins install sparv-sbx-corpus-statistics
```

or, if you used `pipx` above:

```bash
pipx inject sparv sparv-sbx-corpus-statistics
```

or, if you used `uv-pipx` above:

```bash
uvpipx install sparv-sbx-corpus-statistics --inject sparv
```

## Usage

To use this plugin add `sbx_corpus_statistics:stat_highlights` under `export.default` in your `config.yaml`

```yaml
export:
  default:
    - xml_export:pretty
    - sbx_corpus_statistics:stat_highlights
    # - more exports
```

## Minimum Supported Python Version Policy

The Minimum Supported Python Version is fixed for a given minor (1.x)
version. However it can be increased when bumping minor versions, i.e. going
from 1.0 to 1.1 allows us to increase the Minimum Supported Python Version. Users unable to increase their
Python version can use an older minor version instead. Below is a list of sparv-sbx-corpus-statistics versions
and their Minimum Supported Python Version:

- v0.1: Python 3.11.

Note however that sparv-sbx-corpus-statistics also has dependencies, which might have different MSRV
policies. We try to stick to the above policy when updating dependencies, but
this is not always possible.

## Changelog

This project keeps a [changelog](./CHANGELOG.md).

## License

This repository is licensed under the [MIT](./LICENSE) license.

## Development

### Development prerequisites

- [`uv`](https://docs.astral.sh/uv/)
- [`pre-commit`](https://pre-commit.org)

For starting to develop on this repository:

- Clone the repo (in one of the ways below):
  - `git clone git@github.com:spraakbanken/sparv-sbx-corpus-statistics.git`
  - `git clone https://github.com/spraakbanken/sparv-sbx-corpus-statistics.git`
- Setup environment: `make dev`
- Install `pre-commit` hooks: `pre-commit install`

Do your work.

Tasks to do:

- Test the code with `make test` or `make test-w-coverage`.
- Lint the code with `make lint`.
- Check formatting with `make check-fmt`.
- Format the code with `make fmt`.
- Type-check the code with `make type-check`.
- Test the examples with:
  - `make test-example-small-txt`

This repo uses [conventional commits](https://www.conventionalcommits.org/en/v1.0.0/).

### Release a new version

- Prepare the CHANGELOG: `make prepare-release`.
- Edit `CHANGELOG.md` to your liking. Keep the header `[unreleased]`
- Add to git: `git add --update`
- Commit with `git commit -m 'chore(release): prepare release'` or `cog commit chore 'prepare release' release`.
- Bump version (depends on [`bump-my-version](https://callowayproject.github.io/bump-my-version/))
  - Major: `make bumpversion part=major`
  - Minor: `make bumpversion part=minor`
  - Patch: `make bumpversion part=patch` or `make bumpversion`
- Push `main` and tags to GitHub: `git push main --tags` or `make publish`
  - [GitHub Actions workflow](./.github/workflows/release.yaml) will build, test and publish the package to [PyPi](https://pypi.prg).
- Add metadata for [Språkbanken's resource](https://spraakbanken.gu.se/analyser)
  - Generate metadata: `make generate-metadata`
  - Upload the files from `assets/metadata/export/sbx_metadata/utility` to <https://github.com/spraakbanken/metadata/tree/main/yaml/utility>.
