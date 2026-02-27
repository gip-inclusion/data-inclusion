from pathlib import Path

import pydantic
import pytest

from data_inclusion.api.valideur import readers, schemas, services, translations, utils
from data_inclusion.schema import v1


def has_error(
    errors_details: list[services.ValidationError],
    fieldname: str | None = None,
    message: str | None = None,
) -> bool:
    def match(error: services.ValidationError) -> bool:
        cond = True

        if fieldname is not None:
            cond = cond and error["field"] == fieldname

        if message is not None:
            cond = cond and error["message"] == message

        return cond

    return any(match(error) for error in errors_details)


@pytest.mark.parametrize(
    ("items", "expected"),
    [
        ([], ""),
        ([1], "1"),
        ([1, 2, 3], "1-3"),
        ([1, 3, 4, 5, 7], "1, 3-5, 7"),
    ],
)
def test_display_ranges(items, expected):
    assert utils.display_ranges(items) == expected


@pytest.mark.parametrize(
    ("input_thematiques", "expected_message"),
    [
        (
            [v1.Thematique.FAMILLE__GARDE_DENFANTS.value, "foobar"],
            "La valeur 'foobar' n'existe pas",
        ),
        (
            # typo
            [v1.Thematique.FAMILLE__GARDE_DENFANTS.value, "famille--garde-denfant"],
            "La valeur 'famille--garde-denfant' n'existe pas. Vouliez-vous dire 'famille--garde-denfants' ?",  # noqa: E501
        ),
    ],
)
def test_enum_error_message_with_suggestion(
    input_thematiques: list[str], expected_message: str
):
    error_detail = None
    try:
        v1.Service.model_validate({"thematiques": input_thematiques})
    except pydantic.ValidationError as err:
        errors_details = err.errors()
        for error in errors_details:
            if error["type"] == "enum" and error["loc"][0] == "thematiques":
                error_detail = error
                break

    if error_detail is None:
        pytest.fail()

    assert (
        utils.enum_error_message_with_suggestion(
            error_details=error_detail,
            enum_cls=v1.Thematique,
        )["msg"]
        == expected_message
    )


@pytest.mark.parametrize(
    ("msg", "expected"),
    [
        ("Field required", "Champ requis"),
        (
            "String should have at least 6 characters",
            "La chaîne doit contenir au moins 6 caractères",
        ),
    ],
)
def test_tr(msg, expected):
    assert translations.tr(msg) == expected


@pytest.mark.parametrize(
    ("model", "data", "expected_message"),
    [
        (
            schemas.ValideurInputStructure,
            {"nom": "Centre social Le Tournesol."},
            "Le nom de la structure ne doit pas se terminer par un point.",
        ),
        (
            schemas.ValideurInputStructure,
            {"code_insee": "00000"},
            "Le code commune 00000 n'existe pas",
        ),
        (
            schemas.ValideurInputService,
            {"thematiques": ["logement-hebergement--louer-un-appartement"]},
            "La valeur 'logement-hebergement--louer-un-appartement' n'existe pas."
            " Vouliez-vous dire 'logement-hebergement--louer-un-logement' ?",
        ),
    ],
)
def test_list_errors(
    db_session,
    model,
    data,
    expected_message,
):
    assert has_error(
        message=expected_message,
        errors_details=services.list_errors(
            model=model, data=[data], db_session=db_session
        ),
    )


def test_list_errors_with_line_numbers():
    data = [
        {"line": 1, "source": "dora", "id": "foo", "date_maj": "2026-01-01"},
        {"line": 5, "source": "dora", "id": "bar", "date_maj": "2026-01-01"},
    ]

    assert services.list_errors(
        model=schemas.ValideurInputStructure, data=data, db_session=None
    ) == [
        {
            "schema": "Structure",
            "field": "nom",
            "line": 1,
            "value": None,
            "message": "Champ requis",
        },
        {
            "schema": "Structure",
            "field": "nom",
            "line": 5,
            "value": None,
            "message": "Champ requis",
        },
    ]


def list_errors_ignore_source_field():
    assert not has_error(
        fieldname="source",
        message="Champ requis",
        errors_details=services.list_errors(
            model=schemas.ValideurInputStructure,
            data=[{"id": "foo"}],
            db_session=None,
        ),
    )


def test_readers(fixtures_dir: Path):
    try:
        readers.read_file(file=fixtures_dir / "dataset.xlsx")
    except Exception as exc:
        pytest.fail(f"read_file raised an exception: {exc}")
