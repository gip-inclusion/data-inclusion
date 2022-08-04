import inspect

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
