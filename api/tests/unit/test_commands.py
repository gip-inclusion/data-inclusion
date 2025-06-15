import numpy as np
import pandas as pd
import pytest

from data_inclusion.api.inclusion_data import commands
from data_inclusion.api.inclusion_data.v0 import models as v0_models
from data_inclusion.schema import v0, v1


@pytest.fixture
def schema(schema_version):
    if schema_version == "v0":
        return v0
    elif schema_version == "v1":
        return v1


structure_data_1 = {
    "_di_surrogate_id": "dora-1",
    "source": "dora",
    "id": "1",
    "code_insee": "59350",
    "nom": "Chez Dora",
    "date_maj": "not-a-date",
    "description": "." * 100,
}

structure_data_2 = {
    "_di_surrogate_id": "dora-1",
    "source": "dora",
    "id": "1",
    "code_insee": "59350",
    "nom": "Chez Dora",
    "date_maj": "2025-01-01",
    "thematiques": ["not-a-thematique"],
    "description": "." * 100,
}

structure_data_3 = {
    "_di_surrogate_id": "dora-1",
    "source": "dora",
    "id": "1",
    "code_insee": "not-a-code-insee",
    "nom": "Chez Dora",
    "date_maj": "2025-01-01",
    "description": "." * 100,
}


@pytest.mark.parametrize(
    ("structure_data", "schema_version", "is_valid"),
    [
        (structure_data_1, "v0", False),
        (structure_data_1, "v1", False),
        (structure_data_2, "v0", False),
        (structure_data_2, "v1", True),
        (structure_data_3, "v0", False),
        (structure_data_3, "v1", False),
    ],
)
def test_validate_dataset(db_session, structure_data, schema, is_valid):
    input_structures_df = pd.DataFrame(data=[structure_data]).replace({np.nan: None})

    output_structures_df, _ = commands.validate_dataset(
        db_session=db_session,
        structures_df=input_structures_df,
        services_df=pd.DataFrame(),
        structure_schema=schema.Structure,
        service_schema=schema.Service,
    )

    assert len(output_structures_df) == (1 if is_valid else 0)


def test_prepare_dataset_doublons():
    structures_df = pd.DataFrame(
        {c.name: None for c in v0_models.Structure.__table__.columns}
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
        {c.name: None for c in v0_models.Service.__table__.columns}
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
        {c.name: None for c in v0_models.Structure.__table__.columns}
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
        {c.name: None for c in v0_models.Service.__table__.columns}
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
