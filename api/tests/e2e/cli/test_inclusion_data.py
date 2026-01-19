from datetime import datetime

import pandas as pd
import pytest
from click.testing import CliRunner

from data_inclusion.api.cli import cli
from data_inclusion.schema import v0, v1


@pytest.fixture
def dataset_path(structures_df, services_df, tmpdir):
    structures_df.to_parquet(tmpdir / "structures.parquet")
    services_df.to_parquet(tmpdir / "services.parquet")
    return tmpdir


@pytest.mark.parametrize(
    ("version", "structures_df", "services_df", "expected_exit_code", "expected_count"),
    [
        ("v0", pd.DataFrame(), pd.DataFrame(), 1, 0),
        ("v1", pd.DataFrame(), pd.DataFrame(), 1, 0),
        (
            "v0",
            pd.DataFrame(
                [
                    {
                        "_di_surrogate_id": "foo-1",
                        "_cluster_id": None,
                        "_is_closed": False,
                        **v0.Structure(
                            source="foo",
                            id="1",
                            code_insee="59350",
                            nom="ma structure",
                            date_maj=datetime(2025, 1, 1),
                            telephone=None,
                            courriel=None,
                            site_web=None,
                        ).model_dump(),
                    }
                ]
            ),
            pd.DataFrame(
                [
                    {
                        "_di_surrogate_id": "foo-1",
                        "_di_structure_surrogate_id": "foo-1",
                        "score_qualite": 0.8,
                        **v0.Service(
                            source="foo",
                            id="1",
                            structure_id="1",
                            code_insee="59350",
                            nom="mon service",
                            date_maj=datetime(2025, 1, 1),
                            telephone=None,
                            courriel=None,
                        ).model_dump(),
                    }
                ]
            ),
            0,
            1,
        ),
        (
            "v1",
            pd.DataFrame(
                [
                    {
                        "_cluster_id": None,
                        "_is_closed": False,
                        "_has_valid_address": True,
                        **v1.Structure(
                            source="foo",
                            id="1",
                            code_insee="59350",
                            nom="ma structure",
                            date_maj=datetime(2025, 1, 1),
                        ).model_dump(),
                    }
                ]
            ),
            pd.DataFrame(
                [
                    {
                        "score_qualite": 0.8,
                        "_has_valid_address": None,
                        "extra": {"foo": "bar"},
                        **v1.Service(
                            source="foo",
                            id="1",
                            description="." * 100,
                            structure_id="1",
                            code_insee="59350",
                            nom="mon service",
                            date_maj=datetime(2025, 1, 1),
                        ).model_dump(),
                    }
                ]
            ),
            0,
            1,
        ),
        (
            "v1",
            pd.DataFrame(
                [
                    {
                        "_cluster_id": None,
                        "_is_closed": False,
                        "_has_valid_address": False,
                        **v1.Structure(
                            source="foo",
                            id="1",
                            code_insee="59350",
                            nom="ma structure",
                            date_maj=datetime(2025, 1, 1),
                        ).model_dump(),
                    }
                ]
            ),
            pd.DataFrame(
                [
                    {
                        "score_qualite": 0.8,
                        "_has_valid_address": False,
                        **v1.Service(
                            source="foo",
                            id="1",
                            description="." * 100,
                            structure_id="1",
                            code_insee="59350",
                            nom="mon service",
                            date_maj=datetime(2025, 1, 1),
                        ).model_dump(),
                    }
                ]
            ),
            0,
            0,
        ),
    ],
    ids=["v0-empty", "v1-empty", "v0-valid", "v1-valid", "v1-invalid-address"],
)
@pytest.mark.with_token
def test_load_inclusion_data(
    version,
    api_client,
    cli_runner: CliRunner,
    dataset_path,
    expected_exit_code,
    expected_count,
):
    result = cli_runner.invoke(
        cli,
        [
            "load-inclusion-data",
            "--version",
            version,
            "--path",
            str(dataset_path),
        ],
    )

    if expected_exit_code > 0:
        assert result.exit_code == expected_exit_code

    else:
        if result.exception is not None:
            raise result.exception

        for path in [f"/api/{version}/structures", f"/api/{version}/services"]:
            response = api_client.get(path)
            assert response.status_code == 200
            assert len(response.json()["items"]) == expected_count

        if version == "v1":
            response = api_client.get(f"/api/{version}/services")
            service = response.json()["items"][0]
            assert service["extra"] == {"foo": "bar"}
