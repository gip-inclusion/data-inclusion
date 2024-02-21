from pathlib import Path

import factory.random
import pytest

from . import factories


@pytest.fixture(autouse=True)
def predictable_sequences():
    factory.random.reseed_random(0)
    factories.RequestFactory.reset_sequence()
    factories.CommuneFactory.reset_sequence()
    factories.ServiceFactory.reset_sequence()
    factories.SourceFactory.reset_sequence()
    factories.StructureFactory.reset_sequence()


@pytest.fixture(autouse=True)
def create_default_sources(test_session):
    factories.SourceFactory(slug="dora")
    factories.SourceFactory(slug="emplois-de-linclusion")


@pytest.fixture(scope="session")
def generate_communes_nord(db_engine):
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
