import geoalchemy2
from sqlalchemy.orm import Mapped, mapped_column

from data_inclusion.api.core.db import Base

# all fields are nullable or have a default value. These models will only be used to
# query valid data coming from the data pipeline.


class Commune(Base):
    __tablename__ = "admin_express_communes"

    code: Mapped[str] = mapped_column(primary_key=True)
    nom: Mapped[str]
    departement: Mapped[str]
    region: Mapped[str]
    siren_epci: Mapped[str]
    geom = mapped_column(geoalchemy2.Geometry("Geometry", srid=4326))


class EPCI(Base):
    __tablename__ = "admin_express_epcis"

    code: Mapped[str] = mapped_column(primary_key=True)
    nom: Mapped[str]
    nature: Mapped[str]
    geom = mapped_column(geoalchemy2.Geometry("Geometry", srid=4326))


class Departement(Base):
    __tablename__ = "admin_express_departements"

    code: Mapped[str] = mapped_column(primary_key=True)
    nom: Mapped[str]
    insee_reg: Mapped[str]
    geom = mapped_column(geoalchemy2.Geometry("Geometry", srid=4326))


class Region(Base):
    __tablename__ = "admin_express_regions"

    code: Mapped[str] = mapped_column(primary_key=True)
    nom: Mapped[str]
    geom = mapped_column(geoalchemy2.Geometry("Geometry", srid=4326))
