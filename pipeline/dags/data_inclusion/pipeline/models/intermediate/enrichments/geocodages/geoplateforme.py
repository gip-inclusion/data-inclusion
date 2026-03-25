from itertools import batched
from typing import Annotated, Literal

import httpx
import polars as pl
import pydantic

# documentation lives here: https://data.geopf.fr/geocodage/openapi

URL = "https://data.geopf.fr/geocodage/search/csv/"
BATCH_SIZE = 10_000
TIMEOUT = 180  # seconds


class ErrorRow(pydantic.BaseModel):
    result_status: Literal["error"]

    id: str


class SkippedRow(pydantic.BaseModel):
    result_status: Literal["skipped"]

    id: str


class OkRow(pydantic.BaseModel):
    result_status: Literal["ok"]

    id: str
    latitude: float
    longitude: float
    result_label: str
    result_score: float
    result_type: Literal["housenumber", "street", "locality", "municipality"]
    result_id: str
    result_housenumber: str | None
    result_name: str
    result_street: str | None
    result_postcode: str
    result_city: str
    result_context: str
    result_citycode: str | None
    result_oldcitycode: str | None
    result_oldcity: str | None
    result_district: str | None = None


GeocodeCsvResponseRow = Annotated[
    OkRow | SkippedRow | ErrorRow,
    pydantic.Field(discriminator="result_status"),
]
row_adapter = pydantic.TypeAdapter(GeocodeCsvResponseRow)


def geocode(
    data: list[dict],
    columns: list[str],
    filters: dict[Literal["citycode"], str] | None = None,
) -> list[GeocodeCsvResponseRow]:
    """Geocode a batch of addresses using the Geoplateforme API.

    Args:
        data: addresses to geocode
        columns: ordered list of columns that will be concatenated to form the geocoding
            query.
        filters: optional filters to apply on the geocoding results. Best matches will
            be selected among the filtered results.
    """
    return [
        row
        for batch in batched(data, BATCH_SIZE)
        for row in _geocode(list(batch), columns=columns, filters=filters)
    ]


def validate_row(data: dict) -> GeocodeCsvResponseRow:
    return row_adapter.validate_python(data)


def _geocode(
    data: list[dict],
    columns: list[str],
    filters: dict[Literal["citycode"], str] | None = None,
) -> list[GeocodeCsvResponseRow]:
    indexes = ["address"]
    if filters is None:
        filters = {}
    elif "postcode" in filters:
        # Important note: do not use the postcode for this, which is unreliable
        raise ValueError("Postcode should not be used as a filter for geocoding")

    # the api expects and returns csv files
    # polars is used for convenience
    df = pl.from_records(data)

    if "id" not in df.columns:
        raise ValueError("Input data must contain an 'id' column")

    if filters is not None:
        if any(column not in df.columns for column in filters.values()):
            raise ValueError("Filter columns must be present in the input data")

    file_content = df.write_csv(separator="|", quote_style="always")

    print(f"Geocoding batch of {len(data)} rows")
    response = httpx.post(
        URL,
        files={"data": ("data.csv", file_content, "text/csv")},
        data={**filters, "columns": columns, "indexes": indexes},
        timeout=TIMEOUT,
    )
    response.raise_for_status()

    return [
        validate_row(row)  # enforce response schema
        for row in pl.read_csv(
            response.content,
            separator="|",
            infer_schema=False,
            null_values=[""],
        ).to_dicts()
    ]
