import logging

import numpy as np
import pandas as pd
import sentry_sdk
from furl import furl
from sqlalchemy import types
from sqlalchemy.dialects.postgresql import insert

from data_inclusion.api.config import settings
from data_inclusion.api.core import db
from data_inclusion.api.decoupage_administratif import constants, models

logger = logging.getLogger(__name__)


sentry_sdk.init(
    dsn=settings.SENTRY_DSN,
    environment=settings.ENV,
)


def import_communes():
    query_params = {
        "fields": ("nom,code,codesPostaux,codeDepartement,codeRegion,codeEpci,centre"),
        "format": "json",
    }
    dtypes = {
        "code": types.TEXT,
        "codeEpci": types.TEXT,
        "codeDepartement": types.TEXT,
        "codeRegion": types.TEXT,
        "centre": types.JSON,
        "codesPostaux": types.ARRAY(types.TEXT),
    }

    base_url = furl(constants.GEO_API_URL)

    communes_url = (base_url / "communes").set(query_params=query_params).url
    communes_df = pd.read_json(communes_url, dtype=dtypes)

    communes_associees_deleguees_url = (
        (base_url / "communes_associees_deleguees").set(query_params=query_params).url
    )
    communes_associees_deleguees_df = pd.read_json(
        communes_associees_deleguees_url, dtype=dtypes
    )

    districts_url = (
        (base_url / "communes")
        .set(query_params=query_params | {"type": "arrondissement-municipal"})
        .url
    )
    districts_df = pd.read_json(districts_url, dtype=dtypes)

    # order matters for dropping duplicates
    df = pd.concat([communes_df, communes_associees_deleguees_df, districts_df])

    # remove duplicates between communes and communes associées/déléguées:
    # sometimes when communes are merged, the resulting commune has the code
    # of one of the old communes AND the old commune remains in the database as a
    # "commune associée" or "commune déléguée".
    df = df.drop_duplicates(subset=["code"], keep="first")

    df = df.rename(
        columns={
            "codesPostaux": "codes_postaux",
            "codeDepartement": "departement",
            "codeRegion": "region",
            "codeEpci": "siren_epci",
        }
    )

    def create_point(geom):
        return f"POINT({geom['coordinates'][0]} {geom['coordinates'][1]})"

    df["centre"] = df["centre"].apply(create_point)

    df = df.replace({np.nan: None})

    df = df.sort_values(by="code")

    commune_data_list = df.to_dict(orient="records")
    stmt = insert(models.Commune).values(commune_data_list)

    column_names = [
        "code",
        "nom",
        "departement",
        "region",
        "siren_epci",
        "centre",
        "codes_postaux",
    ]
    stmt = stmt.on_conflict_do_update(
        index_elements=[models.Commune.code],
        set_={col: getattr(stmt.excluded, col) for col in column_names},
    )

    with db.SessionLocal() as session:
        session.execute(stmt)
        session.commit()
