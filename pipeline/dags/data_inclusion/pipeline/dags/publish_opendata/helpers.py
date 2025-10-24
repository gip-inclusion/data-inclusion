import enum
import tempfile

import geopandas as gpd
import pandas as pd


class Format(str, enum.Enum):
    CSV = "csv"
    GEOJSON = "geojson"
    JSON = "json"
    PARQUET = "parquet"
    XLSX = "xlsx"


def to_csv(df, buf) -> None:
    df.to_csv(buf, index=False)


def to_geojson(df, buf) -> None:
    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df.longitude, df.latitude),
        crs="EPSG:4326",
    )

    with tempfile.NamedTemporaryFile() as tmp_file:
        gdf.to_file(tmp_file.name, driver="GeoJSON", engine="pyogrio")

        with open(tmp_file.name, "rb") as fp:
            buf.write(fp.read())
            buf.seek(0)


def to_json(df, buf) -> None:
    df.to_json(buf, orient="records", force_ascii=False)


def to_parquet(df, buf) -> None:
    df.to_parquet(buf, index=False)


def to_xlsx(df, buf) -> None:
    df.to_excel(
        buf,
        engine="xlsxwriter",
        engine_kwargs={"options": {"strings_to_urls": False}},
    )


FORMATS_FN = {
    Format.CSV: to_csv,
    Format.GEOJSON: to_geojson,
    Format.JSON: to_json,
    Format.PARQUET: to_parquet,
    Format.XLSX: to_xlsx,
}


def remove_closed_sources(df: pd.DataFrame) -> pd.DataFrame:
    if "_in_opendata" not in df.columns:
        raise ValueError()

    df = df.loc[df["_in_opendata"]]
    df = df.drop(columns="_in_opendata")
    return df


def remove_pii(df: pd.DataFrame) -> pd.DataFrame:
    if "_has_pii" not in df.columns:
        raise ValueError()

    if "courriel" not in df.columns or "telephone" not in df.columns:
        raise ValueError()

    df = df.assign(courriel=df["courriel"].mask(df["_has_pii"], None))
    df = df.assign(telephone=df["telephone"].mask(df["_has_pii"], None))
    df = df.drop(columns="_has_pii")
    return df


def remove_invalid_rows(df: pd.DataFrame) -> pd.DataFrame:
    if "_is_valid" not in df.columns:
        raise ValueError()

    df = df.loc[df["_is_valid"]]
    df = df.drop(columns="_is_valid")
    return df


def remove_internal_columns(df: pd.DataFrame) -> pd.DataFrame:
    internal_columns = [col for col in df.columns if col.startswith("_")]
    df = df.drop(columns=internal_columns)
    return df


def remove_closed_structures(
    structures_df: pd.DataFrame,
    services_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if "_is_closed" not in structures_df.columns:
        raise ValueError()
    structures_df = structures_df.loc[~structures_df["_is_closed"]]
    structures_df = structures_df.drop(columns="_is_closed")

    services_df = services_df.loc[services_df["structure_id"].isin(structures_df["id"])]

    return structures_df, services_df
