import geoalchemy2
import sqlalchemy as sqla
from sqlalchemy.orm import Mapped, mapped_column

from data_inclusion.api.core.db import Base


class Commune(Base):
    code: Mapped[str] = mapped_column(primary_key=True)
    nom: Mapped[str]
    departement: Mapped[str]
    region: Mapped[str]
    siren_epci: Mapped[str | None]
    centre = mapped_column(
        geoalchemy2.Geometry("Geometry", srid=4326, spatial_index=False)
    )
    codes_postaux: Mapped[list[str] | None]

    __table_args__ = (sqla.Index(None, "centre", postgresql_using="gist"),)

    def __repr__(self) -> str:
        return f"<Commune(code={self.code}, nom={self.nom})>"
