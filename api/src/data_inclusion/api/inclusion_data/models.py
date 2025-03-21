from datetime import date

import sqlalchemy as sqla
from sqlalchemy import Computed
from sqlalchemy.dialects.postgresql import TSVECTOR
from sqlalchemy.orm import Mapped, mapped_column, relationship

from data_inclusion.api.core.db import Base
from data_inclusion.api.decoupage_administratif.models import Commune

# all fields are nullable or have a default value. These models will only be used to
# query valid data coming from the data pipeline.


class Structure(Base):
    # internal metadata
    _di_surrogate_id: Mapped[str] = mapped_column(primary_key=True)

    # structure data
    accessibilite: Mapped[str | None]
    adresse: Mapped[str | None]
    antenne: Mapped[bool | None] = mapped_column(default=False)
    code_insee: Mapped[str | None] = mapped_column(sqla.ForeignKey(Commune.code))
    code_postal: Mapped[str | None]
    commune: Mapped[str | None]
    complement_adresse: Mapped[str | None]
    courriel: Mapped[str | None]
    date_maj: Mapped[date | None]
    horaires_ouverture: Mapped[str | None]
    id: Mapped[str]
    labels_autres: Mapped[list[str] | None]
    labels_nationaux: Mapped[list[str] | None]
    latitude: Mapped[float | None]
    lien_source: Mapped[str | None]
    longitude: Mapped[float | None]
    nom: Mapped[str]
    presentation_detail: Mapped[str | None]
    presentation_resume: Mapped[str | None]
    rna: Mapped[str | None]
    siret: Mapped[str | None]
    site_web: Mapped[str | None]
    source: Mapped[str]
    telephone: Mapped[str | None]
    thematiques: Mapped[list[str] | None]
    typologie: Mapped[str | None]

    score_qualite: Mapped[float]

    # Those should be self-refs: sqla.ForeignKey("api__structures._di_surrogate_id")
    # Unfortunately the current version of load_inclusion_data does not support
    # self-references as the "best duplicates" should be inserted first. See to
    # add this extra validation in a future revamp of load_inclusion_data.
    cluster_best_duplicate: Mapped[str | None]

    doublons: Mapped[list[dict] | None]

    services: Mapped[list["Service"]] = relationship(back_populates="structure")
    commune_: Mapped[Commune] = relationship(back_populates="structures")

    __table_args__ = (
        sqla.Index(None, "source"),
        sqla.Index(None, "cluster_best_duplicate"),
    )

    def __repr__(self) -> str:
        return f"<Structure(source={self.source}, id={self.id}, nom={self.nom})>"


class Service(Base):
    # internal metadata
    _di_surrogate_id: Mapped[str] = mapped_column(primary_key=True)
    _di_structure_surrogate_id: Mapped[str] = mapped_column(
        sqla.ForeignKey(Structure._di_surrogate_id)
    )
    structure: Mapped[Structure] = relationship(back_populates="services")

    # service data
    adresse: Mapped[str | None]
    code_insee: Mapped[str | None] = mapped_column(sqla.ForeignKey(Commune.code))
    code_postal: Mapped[str | None]
    commune: Mapped[str | None]
    complement_adresse: Mapped[str | None]
    contact_nom_prenom: Mapped[str | None]
    contact_public: Mapped[bool | None] = mapped_column(default=False)
    courriel: Mapped[str | None]
    cumulable: Mapped[bool | None] = mapped_column(default=False)
    date_creation: Mapped[date | None]
    date_maj: Mapped[date | None]
    date_suspension: Mapped[date | None]
    formulaire_en_ligne: Mapped[str | None]
    frais_autres: Mapped[str | None]
    frais: Mapped[list[str] | None]
    id: Mapped[str]
    justificatifs: Mapped[list[str] | None]
    latitude: Mapped[float | None]
    lien_source: Mapped[str | None]
    longitude: Mapped[float | None]
    modes_accueil: Mapped[list[str] | None]
    modes_orientation_accompagnateur_autres: Mapped[str | None]
    modes_orientation_accompagnateur: Mapped[list[str] | None]
    modes_orientation_beneficiaire_autres: Mapped[str | None]
    modes_orientation_beneficiaire: Mapped[list[str] | None]
    nom: Mapped[str]
    page_web: Mapped[str | None]
    pre_requis: Mapped[list[str] | None]
    presentation_detail: Mapped[str | None]
    presentation_resume: Mapped[str | None]
    prise_rdv: Mapped[str | None]
    profils: Mapped[list[str] | None]
    profils_precisions: Mapped[str | None]
    # generate_profils_precisions is a function that generates
    # a TSVECTOR from profils_precisions and profils
    # cf: 20250107_172223_c947102bb23f_add_profils_autres_field_in_service.py
    searchable_index_profils_precisions: Mapped[str | None] = mapped_column(
        TSVECTOR,
        Computed(
            "generate_profils_precisions(profils_precisions, profils)", persisted=True
        ),
    )
    searchable_index_profils: Mapped[str | None] = mapped_column(
        TSVECTOR,
        Computed("generate_profils(profils)", persisted=True),
    )
    recurrence: Mapped[str | None]
    source: Mapped[str]
    structure_id: Mapped[str]
    telephone: Mapped[str | None]
    thematiques: Mapped[list[str] | None]
    types: Mapped[list[str] | None]
    zone_diffusion_code: Mapped[str | None]
    zone_diffusion_nom: Mapped[str | None]
    zone_diffusion_type: Mapped[str | None]
    score_qualite: Mapped[float]

    commune_: Mapped[Commune] = relationship(back_populates="services")

    __table_args__ = (
        sqla.Index(None, "_di_structure_surrogate_id"),
        sqla.Index(None, "source"),
        sqla.Index(None, "modes_accueil", postgresql_using="gin"),
        sqla.Index(None, "thematiques", postgresql_using="gin"),
        sqla.Index(
            "ix_api__services__geography",
            sqla.text(
                "CAST(ST_MakePoint(longitude, latitude) AS geography(geometry, 4326))"
            ),
            postgresql_using="gist",
        ),
    )

    def __repr__(self) -> str:
        return f"<Service(source={self.source}, id={self.id}, nom={self.nom})>"


Commune.services = relationship(Service, back_populates="commune_")
Commune.structures = relationship(Structure, back_populates="commune_")
