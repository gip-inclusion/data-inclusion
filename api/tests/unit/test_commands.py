from datetime import datetime

import pandas as pd
import pytest

from data_inclusion.api.inclusion_data.v0 import commands as commands_v0
from data_inclusion.api.inclusion_data.v1 import commands as commands_v1, models
from data_inclusion.schema import v0, v1


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("date-maj", "not-a-date"),  # schema violation
        ("_is_closed", True),  # valid schema, but structure flagged as closed
        ("code_insee", "00000"),  # valid schema, but not an actual city code
    ],
)
def test_validate_dataset(db_session, schema_version, column, value):
    if schema_version == "v0":
        validate_dataset = commands_v0.validate_dataset
        valid_structure = v0.Structure(
            id="1",
            source="foo",
            nom="Structure 1",
            date_maj=datetime(2025, 1, 1),
            telephone=None,
            courriel=None,
            site_web=None,
        )
    else:
        validate_dataset = commands_v1.validate_dataset
        valid_structure = v1.Structure(
            id="foo--1",
            source="foo",
            nom="Structure 1",
            date_maj=datetime(2025, 1, 1),
        )

    validate_dataset(
        db_session=db_session,
        structures_df=pd.DataFrame(
            [
                {
                    "_di_surrogate_id": "foo-1",
                    **valid_structure.model_dump(),
                    "_is_closed": False,
                    column: value,
                }
            ]
        ),
        services_df=pd.DataFrame(),
    )


def test_prepare_dataset_doublons():
    structures_df = pd.DataFrame(
        {c.name: None for c in models.Structure.__table__.columns}
        | {
            "id": ["s1", "s2", "s3"],
            "nom": ["Structure 1", "Structure 2", "Structure 3"],
            "date_maj": ["2023-01-01", "2023-01-01", "2023-01-01"],
            "source": ["source1", "source2", "source1"],
            "_cluster_id": ["cluster1", "cluster1", None],
        }
    )

    services_df = pd.DataFrame(
        {c.name: None for c in models.Service.__table__.columns}
        | {
            "structure_id": ["s1", "s1", "s2"],
            "id": ["sv1", "sv2", "sv3"],
            "source": ["source1", "source1", "source2"],
            "nom": ["Service 1", "Service 2", "Service 3"],
            "date_maj": ["2023-01-01", "2023-01-01", "2023-01-01"],
            "score_qualite": [0.7, 0.8, 0.5],
        }
    )

    structures_df, services_df = commands_v1.prepare_dataset(
        structures_df=structures_df,
        services_df=services_df,
    )

    structures_df = structures_df.set_index("id")

    assert structures_df.loc["s1"]._is_best_duplicate is True
    assert structures_df.loc["s1"].score_qualite == 0.75  # average of 0.7 and 0.8

    assert structures_df.loc["s2"]._is_best_duplicate is False
    assert structures_df.loc["s2"].score_qualite == 0.5

    assert structures_df.loc["s3"]._is_best_duplicate is None
    assert structures_df.loc["s3"].score_qualite == 0.0  # no services, default to 0


def test_prepare_dataset_doublons_date_maj():
    structures_df = pd.DataFrame(
        {c.name: None for c in models.Structure.__table__.columns}
        | {
            "_di_surrogate_id": ["s1", "s2", "s3"],
            "id": ["s1", "s2", "s3"],
            "nom": ["Structure 1", "Structure 2", "Structure 3"],
            "date_maj": ["2023-01-01", "2025-06-21", "2023-01-01"],
            "source": ["source1", "source2", "source1"],
            "_cluster_id": ["cluster1", "cluster1", None],
        }
    )

    services_df = pd.DataFrame(
        {c.name: None for c in models.Service.__table__.columns}
        | {
            "_di_surrogate_id": ["sv1"],
            "_di_structure_surrogate_id": ["s1"],
            "structure_id": ["s1"],
            "id": ["sv1"],
            "nom": ["Service 1"],
            "source": ["source1"],
            "score_qualite": [0.0],
        }
    )

    structures_df, services_df = commands_v1.prepare_dataset(
        structures_df=structures_df,
        services_df=services_df,
    )

    structures_df = structures_df.set_index("id")

    assert structures_df.loc["s1"]._is_best_duplicate is False
    assert structures_df.loc["s1"].score_qualite == 0.0
    assert structures_df.loc["s2"]._is_best_duplicate is True
    assert structures_df.loc["s2"].score_qualite == 0.0
