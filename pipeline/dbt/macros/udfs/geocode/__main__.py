"""Geocode a single address against the BAN.

Kept out of `geocode.py`: that file is inlined in a plpython function body,
where `__name__ == "__main__"` holds too.

Usage, from `pipeline/`:

    PYTHONPATH=dbt/macros/udfs uv run python -m geocode "17 rue Malus" \
        --code-postal 59000 --code-insee 59350 --commune Lille
"""

import argparse
import json
import logging

from geocode.geocode import geocode

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="python -m geocode",
        description="Geocode a single address against the BAN.",
    )
    parser.add_argument("adresse", nargs="?", default=None)
    parser.add_argument("--code-postal", default=None)
    parser.add_argument("--code-insee", default=None)
    parser.add_argument("--commune", default=None)
    parser.add_argument("--id", default="1")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    result = geocode(
        data={
            "id": args.id,
            "adresse": args.adresse,
            "code_postal": args.code_postal,
            "code_insee": args.code_insee,
            "commune": args.commune,
        }
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))
