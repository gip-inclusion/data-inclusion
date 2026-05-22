import pendulum
import polars as pl
import pytest

from data_inclusion.pipeline.dags.rename_services import model


@pytest.mark.parametrize(
    ("service", "structure", "expected"),
    [
        pytest.param(
            {"id": "1", "structure_id": "1", "nom": "Structure A"},
            {"id": "1", "nom": "Structure A"},
            True,
            id="structure name matches service name",
        ),
        pytest.param(
            {"id": "2", "structure_id": "2", "nom": "Service B"},
            {"id": "2", "nom": "Structure B"},
            False,
            id="structure name does not match service name",
        ),
    ],
)
def test_filter_services_named_after_structure(service, structure, expected):
    filtered_df = model.filter_services_named_after_structure(
        services_df=pl.DataFrame([service]),
        structures_df=pl.DataFrame([structure]),
    )

    assert (len(filtered_df) == 0) ^ expected


@pytest.mark.parametrize(
    ("service", "expected"),
    [
        pytest.param({"id": "1", "nom": "*"}, True, id="too short"),
        pytest.param({"id": "3", "nom": "*" * 200}, True, id="too long"),
        pytest.param({"id": "2", "nom": "*" * 100}, False, id="just right"),
    ],
)
def test_filter_services_with_bad_name_length(service, expected):
    filtered_df = model.filter_services_with_bad_name_length(
        services_df=pl.DataFrame([service])
    )

    assert (len(filtered_df) == 0) ^ expected


def test_int__renommages():
    model.int__renommages(
        structures_df=pl.DataFrame(
            [
                {
                    "source": "dora",
                    "id": "1",
                    "nom": "Structure A",
                }
            ]
        ),
        services_df=pl.DataFrame(
            [
                {
                    "source": "dora",
                    "id": "1",
                    "structure_id": "1",
                    "nom": "Structure A",
                    "description": "lorem ipsum dolor sit amet" * 10,
                    "thematiques": None,
                    "type": "accompagnement",
                }
            ]
        ),
        rename_fn=lambda x: x["nom"],
        existing_df=pl.DataFrame(
            [
                {
                    "reason": "test reason",
                    "nom": "Structure A",
                    "description": "lorem ipsum dolor sit amet" * 10,
                    "thematiques": None,
                    "type": "accompagnement",
                    "output": "New name for Structure A",
                    "generated_at": pendulum.now(tz="UTC"),
                }
            ]
        ),
    )
