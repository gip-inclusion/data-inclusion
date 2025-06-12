import numpy as np
import pandas as pd
import pytest

from data_inclusion.api.inclusion_data import commands, models


@pytest.mark.parametrize(
    ("structure_data", "is_valid_v0", "is_valid_v1"),
    [
        (
            {
                "_di_surrogate_id": "dora-1",
                "source": "dora",
                "id": "1",
                "code_insee": "59350",
                "nom": "Chez Dora",
                "date_maj": "not-a-date",
            },
            False,
            False,
        ),
        (
            {
                "_di_surrogate_id": "dora-1",
                "source": "dora",
                "id": "1",
                "code_insee": "59350",
                "nom": "Chez Dora",
                "date_maj": "2025-01-01",
                "thematiques": ["not-a-thematique"],
            },
            False,  # v0 should consider thematiques invalid
            True,  # v1 should ignore thematiques
        ),
        (
            {
                "_di_surrogate_id": "dora-1",
                "source": "dora",
                "id": "1",
                "code_insee": "not-a-code-insee",
                "nom": "Chez Dora",
                "date_maj": "2025-01-01",
            },
            False,
            False,
        ),
    ],
)
def test_validate_dataset(db_session, structure_data, is_valid_v0, is_valid_v1):
    input_structures_df = pd.DataFrame(data=[structure_data]).replace({np.nan: None})

    output_structures_df, _ = commands.validate_dataset(
        db_session=db_session,
        structures_df=input_structures_df,
        services_df=pd.DataFrame(),
    )

    assert len(output_structures_df) == len(input_structures_df)
    assert output_structures_df.iloc[0]._is_valid_v0 == is_valid_v0
    assert output_structures_df.iloc[0]._is_valid_v1 == is_valid_v1


def test_prepare_dataset_doublons():
    structures_df = pd.DataFrame(
        {c.name: None for c in models.Structure.__table__.columns}
        | {
            "_di_surrogate_id": ["s1", "s2", "s3"],
            "id": ["id1", "id2", "id3"],
            "nom": ["Structure 1", "Structure 2", "Structure 3"],
            "date_maj": ["2023-01-01", "2023-01-01", "2023-01-01"],
            "source": ["source1", "source2", "source1"],
            "_cluster_id": ["cluster1", "cluster1", None],
        }
    )

    services_df = pd.DataFrame(
        {c.name: None for c in models.Service.__table__.columns}
        | {
            "_di_surrogate_id": ["sv1", "sv2", "sv3"],
            "_di_structure_surrogate_id": ["s1", "s1", "s2"],
            "structure_id": ["id1", "id2", "id3"],
            "id": ["sid1", "sid2", "sid3"],
            "source": ["source1", "source1", "source2"],
            "nom": ["Service 1", "Service 2", "Service 3"],
            "date_maj": ["2023-01-01", "2023-01-01", "2023-01-01"],
            "score_qualite": [0.7, 0.8, 0.5],
        }
    )

    structures_df, services_df = commands.prepare_dataset(structures_df, services_df)

    structures_df = structures_df.set_index("id")

    assert structures_df.loc["id1"]._is_best_duplicate is True
    assert structures_df.loc["id1"].score_qualite == 0.75  # average of 0.7 and 0.8

    assert structures_df.loc["id2"]._is_best_duplicate is False
    assert structures_df.loc["id2"].score_qualite == 0.5

    assert structures_df.loc["id3"]._is_best_duplicate is None
    assert structures_df.loc["id3"].score_qualite == 0.0  # no services, default to 0


def test_prepare_dataset_doublons_date_maj():
    structures_df = pd.DataFrame(
        {c.name: None for c in models.Structure.__table__.columns}
        | {
            "_di_surrogate_id": ["s1", "s2", "s3"],
            "id": ["id1", "id2", "id3"],
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
            "structure_id": ["id1"],
            "nom": ["Service 1"],
            "id": ["sid1"],
            "source": ["source1"],
            "score_qualite": [0.0],
        }
    )

    structures_df, services_df = commands.prepare_dataset(structures_df, services_df)

    structures_df = structures_df.set_index("id")

    assert structures_df.loc["id1"]._is_best_duplicate is False
    assert structures_df.loc["id1"].score_qualite == 0.0
    assert structures_df.loc["id2"]._is_best_duplicate is True
    assert structures_df.loc["id2"].score_qualite == 0.0
