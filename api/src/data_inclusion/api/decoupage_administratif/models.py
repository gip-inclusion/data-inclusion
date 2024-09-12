import geoalchemy2
from sqlalchemy.orm import Mapped, mapped_column

from data_inclusion.api.core.db import Base


class Commune(Base):
    code: Mapped[str] = mapped_column(primary_key=True)
    nom: Mapped[str]
    departement: Mapped[str]
    region: Mapped[str]
    siren_epci: Mapped[str | None]
    # FIXME(vperron) : This column should have an index (spatial_index=True)
    # but let's do it in a future migration
    centre = mapped_column(geoalchemy2.Geometry("Geometry", srid=4326))
    # FIXME(vperron) : This should not be nullable but at the time of migration
    # it's necessary as the info is not there yet. Let's clean up later.
    codes_postaux: Mapped[list[str] | None]

    def __repr__(self) -> str:
        return f"<Commune(code={self.code}, nom={self.nom})>"
