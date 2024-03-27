import geoalchemy2
from sqlalchemy.orm import Mapped, mapped_column

from data_inclusion.api.core.db import Base

# all fields are nullable or have a default value. These models will only be used to
# query valid data coming from the data pipeline.


class Commune(Base):
    code: Mapped[str] = mapped_column(primary_key=True)
    nom: Mapped[str]
    departement: Mapped[str]
    region: Mapped[str]
    siren_epci: Mapped[str]
    geom = mapped_column(
        geoalchemy2.Geometry("Geometry", srid=4326, spatial_index=False)
    )



class EPCI(Base):
    code: Mapped[str] = mapped_column(primary_key=True)
    nom: Mapped[str]
    nature: Mapped[str]
    geom = mapped_column(
        geoalchemy2.Geometry("Geometry", srid=4326, spatial_index=False)
    )



class Departement(Base):
    code: Mapped[str] = mapped_column(primary_key=True)
    nom: Mapped[str]
    insee_reg: Mapped[str]
    geom = mapped_column(
        geoalchemy2.Geometry("Geometry", srid=4326, spatial_index=False)
    )



class Region(Base):
    code: Mapped[str] = mapped_column(primary_key=True)
    nom: Mapped[str]
    geom = mapped_column(
        geoalchemy2.Geometry("Geometry", srid=4326, spatial_index=False)
    )
