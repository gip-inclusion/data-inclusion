import logging

import sqlalchemy as sa
from sqlalchemy import orm

from data_inclusion.api.inclusion_schema import models
from data_inclusion.schema import v1

logger = logging.getLogger(__name__)


def seed(db_session: orm.Session) -> None:
    for model, enum_class in [
        (models.Frais, v1.Frais),
        (models.ModeAccueil, v1.ModeAccueil),
        (models.ModeMobilisation, v1.ModeMobilisation),
        (models.PersonneMobilisatrice, v1.PersonneMobilisatrice),
        (models.ReseauPorteur, v1.ReseauPorteur),
        (models.Public, v1.Public),
        (models.Thematique, v1.Thematique),
        (models.TypeService, v1.TypeService),
    ]:
        values = enum_class.as_dict_list()

        logging.info(f"Seeding {len(values):3d} values into {model.__tablename__}...")

        db_session.execute(sa.delete(model))
        db_session.execute(sa.insert(model), values)
        db_session.commit()
