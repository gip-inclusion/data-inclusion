from datetime import datetime

import pandas as pd
import pytest

from data_inclusion.api.inclusion_data.v0 import commands as commands_v0
from data_inclusion.api.inclusion_data.v1 import commands as commands_v1
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
