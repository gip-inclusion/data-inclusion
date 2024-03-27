import geoalchemy2
import sqlalchemy as sqla
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

    __table_args__ = (
        sqla.Index(
            "ix_api__communes__geography",
            sqla.text("CAST(ST_Simplify(geom, 0.01) AS geography(geometry, 4326))"),
        ),
    )

    def __repr__(self) -> str:
        return f"<Commune(code={self.code}, nom={self.nom})>"


class EPCI(Base):
    code: Mapped[str] = mapped_column(primary_key=True)
    nom: Mapped[str]
    nature: Mapped[str]
    geom = mapped_column(
        geoalchemy2.Geometry("Geometry", srid=4326, spatial_index=False)
    )

    __table_args__ = (
        sqla.Index(
            "ix_api__epcis__geography",
            sqla.text("CAST(ST_Simplify(geom, 0.01) AS geography(geometry, 4326))"),
        ),
    )

    def __repr__(self) -> str:
        return f"<EPCI(code={self.code}, nom={self.nom})>"


class Departement(Base):
    code: Mapped[str] = mapped_column(primary_key=True)
    nom: Mapped[str]
    insee_reg: Mapped[str]
    geom = mapped_column(
        geoalchemy2.Geometry("Geometry", srid=4326, spatial_index=False)
    )

    __table_args__ = (
        sqla.Index(
            "ix_api__departements__geography",
            sqla.text("CAST(ST_Simplify(geom, 0.01) AS geography(geometry, 4326))"),
        ),
    )

    def __repr__(self) -> str:
        return f"<Departement(code={self.code}, nom={self.nom})>"


class Region(Base):
    code: Mapped[str] = mapped_column(primary_key=True)
    nom: Mapped[str]
    geom = mapped_column(
        geoalchemy2.Geometry("Geometry", srid=4326, spatial_index=False)
    )

    __table_args__ = (
        sqla.Index(
            "ix_api__regions__geography",
            sqla.text("CAST(ST_Simplify(geom, 0.01) AS geography(geometry, 4326))"),
        ),
    )

    def __repr__(self) -> str:
        return f"<Region(code={self.code}, nom={self.nom})>"
