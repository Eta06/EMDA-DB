#!/usr/bin/env python3
"""Validate a release tag and extract its changelog section without publishing."""

import argparse
from datetime import date
from pathlib import Path
import re
import sys
import tomllib

ROOT = Path(__file__).resolve().parent.parent
NUMBER = r"(?:0|[1-9][0-9]*)"
TAG_PATTERN = re.compile(
    rf"^v({NUMBER}\.{NUMBER}\.{NUMBER})(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?$"
)
SECTION_NAMES = {"Added", "Changed", "Deprecated", "Removed", "Fixed", "Security", "Limitations"}


def validate_tag(tag: str) -> str:
    match = TAG_PATTERN.fullmatch(tag)
    if match is None:
        raise ValueError("tag must be vMAJOR.MINOR.PATCH with an optional prerelease suffix")
    suffix = match.group(2)
    if suffix and any(part.isascii() and part.isdigit() and len(part) > 1 and part[0] == "0" for part in suffix.split(".")):
        raise ValueError("numeric prerelease identifiers must not have leading zeroes")
    return tag[1:]


def extract_notes(tag: str, manifest_path: Path, changelog_path: Path) -> str:
    version = validate_tag(tag)
    with manifest_path.open("rb") as manifest_file:
        package_version = tomllib.load(manifest_file)["package"]["version"]
    if package_version != version:
        raise ValueError(f"tag version {version} does not match package version {package_version}")

    content = changelog_path.read_text(encoding="utf-8")
    heading = re.compile(rf"^## \[{re.escape(version)}\][^\n]*$", re.MULTILINE)
    matches = list(heading.finditer(content))
    if len(matches) != 1:
        raise ValueError(f"changelog must contain exactly one dated heading for {version}")
    dated = re.fullmatch(rf"## \[{re.escape(version)}\] - (\d{{4}}-\d{{2}}-\d{{2}})[ \t]*", matches[0].group())
    if dated is None:
        raise ValueError("release heading must include a date in YYYY-MM-DD format")
    try:
        date.fromisoformat(dated.group(1))
    except ValueError as error:
        raise ValueError("release heading has an invalid date") from error
    next_heading = re.search(r"^## ", content[matches[0].end():], re.MULTILINE)
    end = matches[0].end() + next_heading.start() if next_heading else len(content)
    section = content[matches[0].start():end].strip()
    category = None
    has_entry = False
    for line in section.splitlines():
        if line.startswith("### "):
            category = line[4:].strip()
        elif category in SECTION_NAMES and re.match(r"^-\s+\S", line):
            has_entry = True
    if not has_entry:
        raise ValueError("release section must include a changelog entry under a supported category")
    return section + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", required=True, help="release tag, for example v0.1.0")
    parser.add_argument("--manifest", type=Path, default=ROOT / "Cargo.toml")
    parser.add_argument("--changelog", type=Path, default=ROOT / "CHANGELOG.md")
    parser.add_argument("--output", type=Path, help="write notes to this path; default is stdout")
    args = parser.parse_args()
    try:
        notes = extract_notes(args.tag, args.manifest, args.changelog)
        if args.output:
            args.output.write_text(notes, encoding="utf-8")
        else:
            sys.stdout.write(notes)
    except (OSError, ValueError, KeyError, tomllib.TOMLDecodeError) as error:
        print(f"release validation failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
