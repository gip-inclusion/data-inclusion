import inspect
from pathlib import Path

import pytest
from factory import Factory

from . import factories


def get_factories():
    return [
        factory
        for (_, factory) in inspect.getmembers(
            factories, lambda o: inspect.isclass(o) and issubclass(o, Factory)
        )
    ]


@pytest.fixture(autouse=True)
def reset_factories_sequences():
    """Reset all sequences for predictable values."""

    for factory in get_factories():
        factory.reset_sequence()


@pytest.fixture
def structure_factory(db_session):
    def factory(**kwargs):
        structure_db_obj = factories.StructureFactory(**kwargs)
        db_session.add(structure_db_obj)
        db_session.commit()
        db_session.refresh(structure_db_obj)
        return structure_db_obj

    return factory


@pytest.fixture
def service_factory(db_session):
    def factory(**kwargs):
        service_db_obj = factories.ServiceFactory(**kwargs)
        db_session.add(service_db_obj)
        db_session.commit()
        db_session.refresh(service_db_obj)
        return service_db_obj

    return factory


@pytest.fixture(scope="session")
def admin_express_commune_nord(db_engine):
    import geopandas

    df = geopandas.read_file(Path(__file__).parent / "data" / "nord.sqlite")
    df = df.rename_geometry("geom")

    with db_engine.connect() as conn:
        df.to_postgis(
            "admin_express_communes",
            con=conn,
            if_exists="replace",
            index=False,
        )
