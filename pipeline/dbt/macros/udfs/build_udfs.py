#!/usr/bin/env -S uv run --script

# /// script
# requires-python = ">=3.12"
# dependencies = [
# ]
# ///


from pathlib import Path

UDFS_DIR = Path(__file__).parent

MARKER = "#INLINE#"
WARNING = "{# !!! THIS FILE IS AUTO-GENERATED. DO NOT EDIT MANUALLY. !!! #}"


def process_udf(udf_dir: Path):
    template_path = next(udf_dir.glob("*.sql.template"))
    template_content = template_path.read_text()
    python = next(udf_dir.glob("[!test_]*.py")).read_text()
    sql = template_content.replace(MARKER, python)
    output_path = udf_dir / template_path.name.replace(".template", "")
    output_path.write_text(WARNING + "\n" * 2 + sql)


if __name__ == "__main__":
    for udf_dir in UDFS_DIR.glob("**/*.sql.template"):
        process_udf(udf_dir=udf_dir.parent)
