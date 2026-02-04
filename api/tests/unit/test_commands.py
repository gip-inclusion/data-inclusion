from datetime import datetime

import pandas as pd
import pytest

from data_inclusion.api.inclusion_data.v1 import commands
from data_inclusion.schema import v1

structure_data = {
    **v1.Structure(
        id="dora--1",
        source="dora",
        nom="Structure 1",
        date_maj=datetime(2025, 1, 1),
    ).model_dump(),
    "_has_valid_address": True,
    "_is_closed": False,
}

service_data = {
    **v1.Service(
        id="dora--1",
        source="dora",
        description="." * 100,
        structure_id="dora--1",
        code_insee="59350",
        nom="Service 1",
        date_maj=datetime(2025, 1, 1),
    ).model_dump(),
    "_has_valid_address": True,
    "score_qualite": 0.9,
}


@pytest.mark.parametrize(
    ("structure_data", "ok"),
    [
        (structure_data, True),
        # schema violation (date_maj is not a date)
        ({**structure_data, "date_maj": "not-a-date"}, False),
        # valid schema, but structure flagged as closed
        ({**structure_data, "_is_closed": True}, False),
        # valid schema, but not an actual city code
        ({**structure_data, "code_insee": "00000"}, False),
    ],
)
def test_prepare_load_structures(db_session, structure_data, ok):
    structures_df, services_df = commands.prepare_load(
        db_session=db_session,
        structures_df=pd.DataFrame([structure_data]),
        services_df=pd.DataFrame([service_data]),
    )
    assert ok is not structures_df.empty

    # if the structure is invalid, all its services should be also be filtered
    assert ok is not services_df.empty


@pytest.mark.parametrize(
    ("service_data", "ok"),
    [
        (service_data, True),
        # schema violation (date_maj is not a date)
        ({**service_data, "date_maj": "not-a-date"}, False),
        # valid schema, but not an actual city code
        ({**service_data, "code_insee": "00000"}, False),
    ],
)
def test_prepare_load_services(db_session, service_data, ok):
    structures_df, services_df = commands.prepare_load(
        db_session=db_session,
        structures_df=pd.DataFrame([structure_data]),
        services_df=pd.DataFrame([service_data]),
    )
    assert not structures_df.empty
    assert ok is not services_df.empty
