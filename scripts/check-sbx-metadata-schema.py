"""Script to download and validate metadata schema."""

# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "httpx>=0.28.1",
#     "jsonschema-rs>=0.42.0",
#     "pyyaml>=6.0.3",
# ]
# ///
import sys
from pathlib import Path

import httpx
import jsonschema_rs
import yaml

METADATA_SCHEMA_URL: str = "https://github.com/spraakbanken/metadata/raw/refs/heads/main/schema/metadata.json"
METADATA_SCHEMA_PATH: Path = Path("assets/schemas/metadata.json")


def main(start_dir: Path) -> None:
    """Validate all yaml in the subtree rooted at start_dir."""
    schema = _get_schema()

    validator = jsonschema_rs.validator_for(schema, validate_formats=True, ignore_unknown_formats=False)

    for metadata_file in start_dir.glob("**/*.yaml"):
        print(f"evaluating '{metadata_file}' ...", file=sys.stderr, end="")  # noqa: T201
        with metadata_file.open() as fp:
            instance = yaml.load(fp, Loader=yaml.BaseLoader)

        evaluation = validator.evaluate(instance)
        if evaluation.flag()["valid"] is True:
            print(" OK", file=sys.stderr)  # noqa: T201
        else:
            print(" ERROR", file=sys.stderr)  # noqa: T201
            for error in evaluation.errors():
                print(f"  error at '{error['instanceLocation']}': {error['error']}", file=sys.stderr)  # noqa: T201
            sys.exit(1)


def _get_schema() -> str:
    try:
        return METADATA_SCHEMA_PATH.read_text(encoding="utf-8")
    except Exception:
        schema = httpx.get(METADATA_SCHEMA_URL, follow_redirects=True)
        METADATA_SCHEMA_PATH.parent.mkdir(parents=True, exist_ok=True)
        METADATA_SCHEMA_PATH.write_text(schema.text, encoding="utf-8")
        return schema.text


if __name__ == "__main__":
    start_dir = Path(sys.argv[1])
    main(start_dir)
