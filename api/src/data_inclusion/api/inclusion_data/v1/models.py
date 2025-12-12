from datetime import date

import sqlalchemy as sqla
from sqlalchemy import Computed, orm
from sqlalchemy.dialects.postgresql import TSVECTOR
from sqlalchemy.orm import Mapped, mapped_column, relationship

from data_inclusion.api.core.db import Base
from data_inclusion.api.decoupage_administratif.models import Commune

# all fields are nullable or have a default value. These models will only be used to
# query valid data coming from the data pipeline.


class HasAddress:
    adresse: Mapped[str | None]
    complement_adresse: Mapped[str | None]
    code_insee: Mapped[str | None] = mapped_column(
        sqla.ForeignKey(Commune.code, ondelete="CASCADE")
    )
    code_postal: Mapped[str | None]
    commune: Mapped[str | None]
    longitude: Mapped[float | None]
    latitude: Mapped[float | None]


class Structure(HasAddress, Base):
    __tablename__ = "api__structures_v1"

    # internal metadata
    _cluster_id: Mapped[str | None]

    # structure data
    id: Mapped[str] = mapped_column(primary_key=True)
    source: Mapped[str]
    nom: Mapped[str]
    score_qualite: Mapped[float]
    description: Mapped[str | None]
    accessibilite_lieu: Mapped[str | None]
    courriel: Mapped[str | None]
    date_maj: Mapped[date | None]
    # FIXME(vperron) : for now this is not validated against OSM opening hours format
    # see https://pypi.org/project/opening-hours-py/ when we stop using pl/Python
    horaires_accueil: Mapped[str | None]
    reseaux_porteurs: Mapped[list[str] | None]
    siret: Mapped[str | None]
    site_web: Mapped[str | None]
    lien_source: Mapped[str | None]
    telephone: Mapped[str | None]

    services: Mapped[list["v1.models.Service"]] = relationship(  # noqa: F821
        back_populates="structure"
    )
    commune_: Mapped[Commune] = relationship(back_populates="structures_v1")

    __table_args__ = (
        sqla.Index(None, "source"),
        sqla.Index(None, "_cluster_id"),
        sqla.Index(
            "ix_api__structures_v1__cluster_dedup",
            "_cluster_id",
            sqla.desc("score_qualite"),
            sqla.desc("date_maj"),
        ),
    )

    def __repr__(self) -> str:
        return f"<Structure(source={self.source}, id={self.id}, nom={self.nom})>"


Structure.doublons = relationship(
    Structure,
    primaryjoin=sqla.and_(
        orm.foreign(Structure._cluster_id) == orm.remote(Structure._cluster_id),
        orm.remote(Structure._cluster_id).isnot(None),
        orm.foreign(Structure.id) != orm.remote(Structure.id),
    ),
    uselist=True,
)


class Service(HasAddress, Base):
    __tablename__ = "api__services_v1"

    # service data
    id: Mapped[str] = mapped_column(primary_key=True)
    source: Mapped[str]
    structure_id: Mapped[str] = mapped_column(
        sqla.ForeignKey(Structure.id, ondelete="CASCADE")
    )
    nom: Mapped[str]
    score_qualite: Mapped[float]
    description: Mapped[str | None]
    date_maj: Mapped[date | None]
    type: Mapped[str | None]
    thematiques: Mapped[list[str] | None]
    publics: Mapped[list[str] | None]
    publics_precisions: Mapped[str | None]
    conditions_acces: Mapped[str | None]
    contact_nom_prenom: Mapped[str | None]
    courriel: Mapped[str | None]
    frais: Mapped[str | None]
    frais_precisions: Mapped[str | None]
    # FIXME(vperron) : for now this is not validated against OSM opening hours format
    # see https://pypi.org/project/opening-hours-py/ when we stop using pl/Python
    horaires_accueil: Mapped[str | None]
    modes_accueil: Mapped[list[str] | None]
    modes_mobilisation: Mapped[list[str] | None]
    mobilisation_precisions: Mapped[str | None]
    mobilisable_par: Mapped[list[str] | None]
    lien_mobilisation: Mapped[str | None]
    lien_source: Mapped[str | None]
    # generate_profils_precisions is a function that generates
    # a TSVECTOR from publics_precisions and publics.
    # cf: 20250107_172223_c947102bb23f_add_profils_autres_field_in_service.py
    searchable_index_publics_precisions: Mapped[str | None] = mapped_column(
        TSVECTOR,
        Computed(
            "generate_profils_precisions(publics_precisions, publics)", persisted=True
        ),
    )
    searchable_index_publics: Mapped[str | None] = mapped_column(
        TSVECTOR,
        Computed("generate_profils(publics)", persisted=True),
    )
    telephone: Mapped[str | None]
    volume_horaire_hebdomadaire: Mapped[float | None]
    zone_eligibilite: Mapped[list[str] | None]
    nombre_semaines: Mapped[int | None]

    commune_: Mapped[Commune] = relationship(back_populates="services_v1")
    structure: Mapped[Structure] = relationship(back_populates="services")

    __table_args__ = (
        sqla.Index(None, "structure_id"),
        sqla.Index(None, "source"),
        sqla.Index(None, "modes_accueil", postgresql_using="gin"),
        sqla.Index(None, "thematiques", postgresql_using="gin"),
        sqla.Index(
            "ix_api__services_v1__geography",
            sqla.text("ST_MakePoint(longitude, latitude)::geography(geometry, 4326)"),
            postgresql_using="gist",
        ),
        sqla.Index(None, "searchable_index_publics", postgresql_using="gin"),
        sqla.Index(None, "searchable_index_publics_precisions", postgresql_using="gin"),
    )

    def __repr__(self) -> str:
        return f"<Service(source={self.source}, id={self.id}, nom={self.nom})>"


Commune.services_v1 = relationship(Service, back_populates="commune_")
Commune.structures_v1 = relationship(Structure, back_populates="commune_")
