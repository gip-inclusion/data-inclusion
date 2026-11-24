{# !!! THIS FILE IS AUTO-GENERATED. DO NOT EDIT MANUALLY. !!! #}

{% macro udf__geocode() %}

DROP FUNCTION IF EXISTS processings.geocode;

CREATE OR REPLACE FUNCTION processings.geocode(data JSONB)
RETURNS
    TABLE(
        id TEXT,
        result_score FLOAT,
        result_label TEXT,
        result_city TEXT,
        result_type TEXT,
        result_citycode TEXT,
        result_postcode TEXT,
        result_name TEXT,
        longitude FLOAT,
        latitude FLOAT
    )
AS $$

import json

"""Geocode addresses against the BAN (https://data.geopf.fr/geocodage/).

Entry point: `geocode(data, client=post_to_ban, ...)`, where `data` is one dict
or a list of dicts with the keys `id`, `adresse`, `code_postal`, `code_insee`,
`commune`. It returns one dict per geocoded row, in input order, with the
columns in `OUTPUT_COLUMNS`.

Rows without 2 of `code_postal`, `code_insee`, `commune` are dropped, as are
results that only resolved the city (see `keep_result`). Addresses the BAN does
not find are returned with empty result fields.

`client` takes a CSV payload and returns the BAN's CSV response; it is the only
I/O and can be replaced in tests. A failed request drops its batch with a
warning; the `int__geocodages_v1` model retries those rows on its next run.

This file is inlined into the `processings.geocode` plpython function body by
`../build_udfs.py`: it must not rely on module-level behaviour.
"""

import csv
import dataclasses
import io
import logging
import re
from collections.abc import Callable

import requests

logger = logging.getLogger(__name__)

BAN_URL = "https://data.geopf.fr/geocodage/search/csv/"

# columns of the payload sent to the BAN
CSV_COLUMNS = ("id", "adresse", "code_postal", "commune", "code_insee")

# columns returned by the UDF
OUTPUT_COLUMNS = (
    "id",
    "result_score",
    "result_label",
    "result_city",
    "result_type",
    "result_citycode",
    "result_postcode",
    "result_name",
    "longitude",
    "latitude",
)

FLOAT_COLUMNS = ("result_score", "latitude", "longitude")

# at least 2 of these must be present for an address to be geocoded
LOCATION_FIELDS = ("code_postal", "code_insee", "commune")


# abbreviations found in the FINESS "typvoie" column
STREET_ABBREVIATIONS = {
    "all": "allée",
    "av": "avenue",
    "bd": "boulevard",
    "bld": "boulevard",
    "bvd": "boulevard",
    "che": "chemin",
    "chem": "chemin",
    "crs": "cours",
    "esp": "esplanade",
    "fg": "faubourg",
    "ham": "hameau",
    "imp": "impasse",
    "ld": "lieu-dit",
    "mte": "montée",
    "pas": "passage",
    "pl": "place",
    "pro": "promenade",
    "prom": "promenade",
    "pte": "porte",
    "qu": "quai",
    "qua": "quartier",
    "r": "rue",
    "rd pt": "rond point",
    "rle": "ruelle",
    "rpt": "rond point",
    "rte": "route",
    "sq": "square",
    "tra": "traverse",
    "voi": "voie",
}

STREET_ABBREVIATION_PATTERNS = [
    (re.compile(rf"(?i)\b{re.escape(abbreviation)}\b"), full)
    for abbreviation, full in STREET_ABBREVIATIONS.items()
]

# post box mentions (boîte postale, course spéciale), e.g. "BP 123", "cs 45"
POST_BOX_PATTERN = re.compile(r"(?i)\b(?:BP|CS) *[0-9]*\b")

# cedex is always at the end of the city
CEDEX_PATTERN = re.compile(r"(?i)cedex.*")

# street addresses that legitimately resolve to the municipality only
PLACE_WORDS_PATTERN = re.compile(r"(?i)\b(?:bourg|village|mairie)\b")


@dataclasses.dataclass(frozen=True)
class Address:
    id: str | None
    adresse: str | None
    code_postal: str | None
    code_insee: str | None
    commune: str | None


def normalize_value(value) -> str | None:
    if value is None:
        return None
    value = (value if isinstance(value, str) else str(value)).strip()
    return value or None


def normalize(row: dict) -> Address:
    return Address(
        **{
            field.name: normalize_value(row.get(field.name))
            for field in dataclasses.fields(Address)
        }
    )


def is_geocodable(address: Address) -> bool:
    values = [getattr(address, field) for field in LOCATION_FIELDS]
    return sum(value is not None for value in values) >= 2


def remove_dots(adresse: str) -> str:
    # "Avenue de l'U.R.S.S." scores much better as "Avenue de l'URSS"
    return adresse.replace(".", "")


def has_post_box(adresse: str | None) -> bool:
    return adresse is not None and bool(POST_BOX_PATTERN.search(remove_dots(adresse)))


def has_cedex(commune: str | None) -> bool:
    return commune is not None and bool(CEDEX_PATTERN.search(commune))


def clean_adresse(adresse: str | None) -> str | None:
    if adresse is None:
        return None

    adresse = adresse.strip(" -")
    adresse = remove_dots(adresse)
    adresse = POST_BOX_PATTERN.sub("", adresse)
    for pattern, full in STREET_ABBREVIATION_PATTERNS:
        adresse = pattern.sub(full, adresse)
    adresse = adresse.strip(" -")
    return adresse or None


def clean_commune(commune: str | None) -> str | None:
    if commune is None:
        return None

    commune = CEDEX_PATTERN.sub("", commune)
    commune = commune.strip(" -")
    return commune or None


def clean(address: Address) -> Address:
    # post box and cedex postal codes are special codes,
    # that do not match the city's postal code
    has_special_postal_code = has_post_box(address.adresse) or has_cedex(
        address.commune
    )
    return dataclasses.replace(
        address,
        adresse=clean_adresse(address.adresse),
        commune=clean_commune(address.commune),
        code_postal=None if has_special_postal_code else address.code_postal,
    )


def to_csv(addresses: list[Address]) -> bytes:
    with io.StringIO() as buf:
        writer = csv.writer(
            buf, delimiter="|", quoting=csv.QUOTE_ALL, lineterminator="\n"
        )
        writer.writerow(CSV_COLUMNS)
        writer.writerows(
            [getattr(address, column) for column in CSV_COLUMNS]
            for address in addresses
        )
        return buf.getvalue().encode("utf-8")


def post_to_ban(payload: bytes) -> str:
    response = requests.post(
        BAN_URL,
        files={"data": ("data.csv", payload, "text/csv")},
        data={
            "columns": ["adresse", "code_postal", "commune"],
            # Post-filter on the INSEE code and not the zipcode.
            # Explanations from the BAN API creators:
            # The postcode is problematic for cities with multiple zipcodes
            # if the supplied zipcode is wrong, or the one in the BAN is.
            # The INSEE code is more stable, unique and reliable.
            # Also this post-filter does not return "possible" results,
            # it blindly filters-out.
            "citycode": "code_insee",
        },
        timeout=180,  # we upload a few MB of data, so we need a high timeout
    )
    response.raise_for_status()
    return response.text


def parse_csv(text: str) -> list[dict]:
    reader = csv.reader(io.StringIO(text), delimiter="|")
    header = next(reader, [])

    missing_columns = {"id", "result_type"} - set(header)
    if missing_columns:
        raise ValueError(f"BAN response is missing columns: {sorted(missing_columns)}")

    results = []
    for line_number, fields in enumerate(reader, start=2):
        if not fields:
            continue
        if len(fields) != len(header):
            logger.warning(
                "Skipping BAN response line %s: expected %s fields, got %s",
                line_number,
                len(header),
                len(fields),
            )
            continue

        result = {column: value or None for column, value in zip(header, fields)}
        for column in FLOAT_COLUMNS:
            if result.get(column) is not None:
                result[column] = float(result[column])
        results.append(result)

    return results


def keep_result(address: Address, result: dict) -> bool:
    # only keep results not being "only a city"...
    if result["result_type"] != "municipality":
        return True

    # ... unless the supplied address was empty
    if address.adresse is None:
        return True

    # ... or the street address is "bourg", "mairie" or "village"
    if PLACE_WORDS_PATTERN.search(address.adresse):
        return True

    # ... or the address is more or less the same as the city name
    # (e.g: adresse=Anjou and commune=anjou 07300)
    if address.commune is not None:
        adresse, commune = address.adresse.lower(), address.commune.lower()
        return adresse in commune or commune in adresse

    return False


def geocode(
    data: dict | list[dict],
    client: Callable[[bytes], str] = post_to_ban,
    # BAN api limits the batch geocoding to 50MB of data
    # In our tests, 10_000 rows is about 1MB; but we'll be conservative
    # since we also want to avoid upload timeouts.
    batch_size: int = 20_000,
) -> list[dict]:
    rows = data if isinstance(data, list) else [data]

    addresses = [
        clean(address) for address in map(normalize, rows) if is_geocodable(address)
    ]
    logger.info("Only %s rows can be geocoded.", len(addresses))

    output = []
    for start in range(0, len(addresses), batch_size):
        batch = addresses[start : start + batch_size]
        addresses_by_id = {address.id: address for address in batch}

        logger.info("Will send address batch, size=%s", len(batch))
        try:
            response = client(to_csv(batch))
        except requests.RequestException as exc:
            # the batch is dropped: the model retries its rows on the next run
            logger.warning("BAN request failed, dropping batch: %s", exc)
            continue
        results = parse_csv(response)

        kept_by_id = {}
        for result in results:
            address = addresses_by_id.get(result["id"])
            if address is None:
                logger.warning("Skipping BAN result with unknown id=%s", result["id"])
                continue
            if keep_result(address, result):
                kept_by_id[address.id] = {
                    column: result.get(column) for column in OUTPUT_COLUMNS
                }

        logger.info("Got results for address batch, size=%s", len(kept_by_id))

        # keep the input order, whatever the order of the response
        output += [
            kept_by_id.pop(address.id) for address in batch if address.id in kept_by_id
        ]

    return output


return (
    geocode(data=json.loads(data))
    if data is not None
    else []
)

$$ LANGUAGE plpython3u;

{% endmacro %}
