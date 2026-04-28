from datetime import datetime

import polars as pl
import pytest

from data_inclusion.api.inclusion_data import commands
from data_inclusion.schema import v1

structure_data = {
    **v1.Structure(
        id="dora--1",
        source="dora",
        nom="Structure 1",
        date_maj=datetime(2025, 1, 1),
        code_insee="59350",
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
    ("structure_data", "expected_empty"),
    [
        (structure_data, False),
        # schema violation (date_maj is not a date)
        ({**structure_data, "date_maj": "not-a-date"}, True),
        # valid schema, but structure flagged as closed
        ({**structure_data, "_is_closed": True}, True),
        # valid schema, but not an actual city code
        ({**structure_data, "code_insee": "00000"}, True),
    ],
)
def test_prepare_load_structures(structure_data, expected_empty):
    structures_df, services_df = commands.prepare_load(
        cities_df=pl.DataFrame([{"code": "59350"}]),
        structures_df=pl.DataFrame([structure_data]),
        services_df=pl.DataFrame([service_data]),
    )
    assert structures_df.is_empty() is expected_empty

    # if the structure is invalid, all its services should be also be filtered
    assert services_df.is_empty() is expected_empty


@pytest.mark.parametrize(
    ("service_data", "expected_empty"),
    [
        (service_data, False),
        # schema violation (date_maj is not a date)
        ({**service_data, "date_maj": "not-a-date"}, True),
        # valid schema, but not an actual city code
        ({**service_data, "code_insee": "00000"}, True),
    ],
)
def test_prepare_load_services(service_data, expected_empty):
    structures_df, services_df = commands.prepare_load(
        cities_df=pl.DataFrame([{"code": "59350"}]),
        structures_df=pl.DataFrame([structure_data]),
        services_df=pl.DataFrame([service_data]),
    )
    assert not structures_df.is_empty()
    assert services_df.is_empty() is expected_empty
