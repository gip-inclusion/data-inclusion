import logging

import numpy as np
import pandas as pd
from furl import furl
from sqlalchemy import types
from sqlalchemy.dialects.postgresql import insert

from data_inclusion.api.core import db
from data_inclusion.api.inclusion_data import models

logger = logging.getLogger(__name__)


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

    communes_url = (
        furl("https://geo.api.gouv.fr/communes").set(query_params=query_params).url
    )
    communes_df = pd.read_json(communes_url, dtype=dtypes)

    districts_url = (
        furl("https://geo.api.gouv.fr/communes")
        .set(query_params=query_params | {"type": "arrondissement-municipal"})
        .url
    )
    districts_df = pd.read_json(districts_url, dtype=dtypes)

    df = pd.concat([communes_df, districts_df])

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
