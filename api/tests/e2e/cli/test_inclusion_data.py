import pandas as pd
import pytest

from data_inclusion.api.cli import cli
from data_inclusion.schema import v0


@pytest.fixture
def dataset_path(structures_df, services_df, tmpdir):
    structures_df.to_parquet(tmpdir / "structures.parquet")
    services_df.to_parquet(tmpdir / "services.parquet")
    return tmpdir


@pytest.mark.parametrize(
    ("structures_df", "services_df", "expected_exit_code"),
    [
        (pd.DataFrame(), pd.DataFrame(), 1),
        (
            pd.DataFrame(
                [
                    {
                        "_di_surrogate_id": "foo-1",
                        "_cluster_id": None,
                        "description": None,
                        **v0.Structure(
                            source="foo",
                            id="1",
                            code_insee="59350",
                            nom="ma structure",
                            date_maj="2025-01-01",
                        ).model_dump(),
                    }
                ]
            ),
            pd.DataFrame(
                [
                    {
                        "_di_structure_surrogate_id": "foo-1",
                        "_di_surrogate_id": "foo-1",
                        "score_qualite_v0": 0.8,
                        "score_qualite_v1": 0.8,
                        "description": None,
                        "frais_v1": "payant",
                        "frais_precisions": "100 balles et un mars",
                        "lien_mobilisation": None,
                        "mobilisable_par": None,
                        "mobilisation_precisions": None,
                        "modes_mobilisation": None,
                        **v0.Service(
                            source="foo",
                            id="1",
                            structure_id="1",
                            code_insee="59350",
                            nom="mon service",
                            date_maj="2025-01-01",
                        ).model_dump(),
                    }
                ]
            ),
            0,
        ),
    ],
)
@pytest.mark.with_token
def test_load_inclusion_data(api_client, cli_runner, dataset_path, expected_exit_code):
    result = cli_runner.invoke(
        cli,
        [
            "load-inclusion-data",
            "--path",
            str(dataset_path),
        ],
    )

    assert result.exit_code == expected_exit_code

    if expected_exit_code == 0:
        for path in ["/api/v0/structures", "/api/v0/services"]:
            response = api_client.get(path)
            assert response.status_code == 200
            assert len(response.json()["items"]) == 1
