from datetime import date

import sqlalchemy as sqla
from sqlalchemy.orm import Mapped, mapped_column, relationship

from data_inclusion.api.code_officiel_geo.models import Commune
from data_inclusion.api.core.db import Base

# all fields are nullable or have a default value. These models will only be used to
# query valid data coming from the data pipeline.


class Structure(Base):
    # internal metadata
    _di_surrogate_id: Mapped[str] = mapped_column(primary_key=True)
    _di_geocodage_code_insee: Mapped[str | None]
    _di_geocodage_score: Mapped[float | None]

    # structure data
    accessibilite: Mapped[str | None]
    adresse: Mapped[str | None]
    antenne: Mapped[bool | None] = mapped_column(default=False)
    code_insee: Mapped[str | None]
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

    services: Mapped[list["Service"]] = relationship(back_populates="structure")

    __table_args__ = (sqla.Index(None, "source"),)

    def __repr__(self) -> str:
        return f"<Structure(source={self.source}, id={self.id}, nom={self.nom})>"


class Service(Base):
    # internal metadata
    _di_surrogate_id: Mapped[str] = mapped_column(primary_key=True)
    _di_structure_surrogate_id: Mapped[str] = mapped_column(
        sqla.ForeignKey(Structure._di_surrogate_id)
    )
    _di_geocodage_code_insee: Mapped[str | None]
    _di_geocodage_score: Mapped[float | None]
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
    pre_requis: Mapped[list[str] | None]
    presentation_detail: Mapped[str | None]
    presentation_resume: Mapped[str | None]
    prise_rdv: Mapped[str | None]
    profils: Mapped[list[str] | None]
    recurrence: Mapped[str | None]
    source: Mapped[str]
    structure_id: Mapped[str]
    telephone: Mapped[str | None]
    thematiques: Mapped[list[str] | None]
    types: Mapped[list[str] | None]
    zone_diffusion_code: Mapped[str | None]
    zone_diffusion_nom: Mapped[str | None]
    zone_diffusion_type: Mapped[str | None]

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
