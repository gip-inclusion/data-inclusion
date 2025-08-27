import numpy as np
import pandas as pd
import pytest

from data_inclusion.api.inclusion_data import commands
from data_inclusion.api.inclusion_data.v0 import models as v0_models
from data_inclusion.api.inclusion_data.v1 import models as v1_models
from data_inclusion.schema import v0, v1


@pytest.fixture
def schema(schema_version):
    if schema_version == "v0":
        return v0
    elif schema_version == "v1":
        return v1


@pytest.fixture
def models(schema_version):
    if schema_version == "v0":
        return v0_models
    elif schema_version == "v1":
        return v1_models


def structure_data_factory(**kwargs):
    return {
        "_di_surrogate_id": "dora-1",
        "source": "dora",
        "id": "1",
        "code_insee": "59350",
        "nom": "Chez Dora",
        "date_maj": "2025-01-01",
        "description": "." * 100,
        "_is_closed": False,
    } | kwargs


@pytest.mark.parametrize(
    ("structure_data", "schema_version", "is_valid"),
    [
        (structure_data_factory(date_maj="not-a-date"), "v0", False),
        (structure_data_factory(date_maj="not-a-date"), "v1", False),
        (structure_data_factory(thematiques=["not-a-thematique"]), "v0", False),
        # structure have no thematiques in v1, therefore it should be ignored
        (structure_data_factory(thematiques=["not-a-thematique"]), "v1", True),
        (structure_data_factory(code_insee="not-a-code-insee"), "v0", False),
        (structure_data_factory(code_insee="not-a-code-insee"), "v1", False),
        (structure_data_factory(_is_closed=True), "v0", False),
        (structure_data_factory(_is_closed=True), "v1", False),
    ],
)
def test_validate_dataset(db_session, structure_data, schema_version, schema, is_valid):
    input_structures_df = pd.DataFrame(data=[structure_data]).replace({np.nan: None})

    output_structures_df, _ = commands.validate_dataset(
        schema_version=schema_version,
        db_session=db_session,
        structures_df=input_structures_df,
        services_df=pd.DataFrame(),
        structure_schema=schema.Structure,
        service_schema=schema.Service,
    )

    assert len(output_structures_df) == (1 if is_valid else 0)


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
def test_prepare_dataset_doublons(schema_version, models):
    structures_df = pd.DataFrame(
        {c.name: None for c in models.Structure.__table__.columns}
        | {
            "_di_surrogate_id": ["s1", "s2", "s3"],
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
            "_di_surrogate_id": ["sv1", "sv2", "sv3"],
            "_di_structure_surrogate_id": ["s1", "s1", "s2"],
            "structure_id": ["s1", "s1", "s2"],
            "id": ["sv1", "sv2", "sv3"],
            "source": ["source1", "source1", "source2"],
            "nom": ["Service 1", "Service 2", "Service 3"],
            "date_maj": ["2023-01-01", "2023-01-01", "2023-01-01"],
            "score_qualite": [0.7, 0.8, 0.5],
        }
    )

    structures_df, services_df = commands.prepare_dataset(
        schema_version=schema_version,
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


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
def test_prepare_dataset_doublons_date_maj(schema_version, models):
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

    structures_df, services_df = commands.prepare_dataset(
        schema_version=schema_version,
        structures_df=structures_df,
        services_df=services_df,
    )

    structures_df = structures_df.set_index("id")

    assert structures_df.loc["s1"]._is_best_duplicate is False
    assert structures_df.loc["s1"].score_qualite == 0.0
    assert structures_df.loc["s2"]._is_best_duplicate is True
    assert structures_df.loc["s2"].score_qualite == 0.0
