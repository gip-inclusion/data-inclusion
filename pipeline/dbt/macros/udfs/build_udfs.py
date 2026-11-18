#!/usr/bin/env -S uv run --script

# /// script
# requires-python = ">=3.12"
# dependencies = [
# ]
# ///


import re
from pathlib import Path

UDFS_DIR = Path(__file__).parent

# e.g. `#INLINE deduplicate.py#`, path relative to the template
MARKER = re.compile(r"#INLINE (?P<path>\S+)#")
WARNING = "{# !!! THIS FILE IS AUTO-GENERATED. DO NOT EDIT MANUALLY. !!! #}"


def process_template(template_path: Path):
    template_content = template_path.read_text()

    if MARKER.search(template_content) is None:
        raise ValueError(f"no `#INLINE <module>#` marker in {template_path}")

    sql = MARKER.sub(
        lambda match: (template_path.parent / match["path"]).read_text(),
        template_content,
    )
    output_path = template_path.with_name(template_path.name.removesuffix(".template"))
    output_path.write_text(WARNING + "\n" * 2 + sql)


if __name__ == "__main__":
    for template_path in UDFS_DIR.glob("**/*.sql.template"):
        process_template(template_path=template_path)
