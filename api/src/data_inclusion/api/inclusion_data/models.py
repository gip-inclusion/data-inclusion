from datetime import date

import geoalchemy2
import sqlalchemy as sqla
from sqlalchemy.orm import Mapped, mapped_column, relationship

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

    services: Mapped[list["Service"]] = relationship(back_populates="commune_")


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


class Structure(Base):
    __tablename__ = "structure"

    # internal metadata
    _di_surrogate_id: Mapped[str] = mapped_column(primary_key=True)
    _di_geocodage_code_insee: Mapped[str | None]
    _di_geocodage_score: Mapped[float | None]

    # structure data
    source: Mapped[str]
    id: Mapped[str]
    siret: Mapped[str | None]
    rna: Mapped[str | None]
    nom: Mapped[str]
    commune: Mapped[str | None]
    code_postal: Mapped[str | None]
    code_insee: Mapped[str | None]
    adresse: Mapped[str | None]
    complement_adresse: Mapped[str | None]
    longitude: Mapped[float | None]
    latitude: Mapped[float | None]
    typologie: Mapped[str | None]
    telephone: Mapped[str | None]
    courriel: Mapped[str | None]
    site_web: Mapped[str | None]
    presentation_resume: Mapped[str | None]
    presentation_detail: Mapped[str | None]
    date_maj: Mapped[date | None]
    antenne: Mapped[bool | None] = mapped_column(default=False)
    lien_source: Mapped[str | None]
    horaires_ouverture: Mapped[str | None]
    accessibilite: Mapped[str | None]
    labels_nationaux: Mapped[list[str] | None]
    labels_autres: Mapped[list[str] | None]
    thematiques: Mapped[list[str] | None]

    services: Mapped[list["Service"]] = relationship(back_populates="structure")


class Service(Base):
    __tablename__ = "service"

    # internal metadata
    _di_surrogate_id: Mapped[str] = mapped_column(primary_key=True)
    _di_structure_surrogate_id: Mapped[str] = mapped_column(
        sqla.ForeignKey("structure._di_surrogate_id")
    )
    _di_geocodage_code_insee: Mapped[str | None]
    _di_geocodage_score: Mapped[float | None]
    structure: Mapped["Structure"] = relationship(back_populates="services")

    # service data
    source: Mapped[str]
    id: Mapped[str]
    structure_id: Mapped[str]
    nom: Mapped[str]
    presentation_resume: Mapped[str | None]
    presentation_detail: Mapped[str | None]
    types: Mapped[list[str] | None]
    thematiques: Mapped[list[str] | None]
    prise_rdv: Mapped[str | None]
    frais: Mapped[list[str] | None]
    frais_autres: Mapped[str | None]
    profils: Mapped[list[str] | None]
    pre_requis: Mapped[list[str] | None]
    cumulable: Mapped[bool | None] = mapped_column(default=False)
    justificatifs: Mapped[list[str] | None]
    formulaire_en_ligne: Mapped[str | None]
    commune: Mapped[str | None]
    code_postal: Mapped[str | None]
    code_insee: Mapped[str | None] = mapped_column(
        sqla.ForeignKey("admin_express_communes.code")
    )
    adresse: Mapped[str | None]
    complement_adresse: Mapped[str | None]
    longitude: Mapped[float | None]
    latitude: Mapped[float | None]
    recurrence: Mapped[str | None]
    date_creation: Mapped[date | None]
    date_suspension: Mapped[date | None]
    lien_source: Mapped[str | None]
    telephone: Mapped[str | None]
    courriel: Mapped[str | None]
    contact_public: Mapped[bool | None] = mapped_column(default=False)
    contact_nom_prenom: Mapped[str]
    date_maj: Mapped[date | None]
    modes_accueil: Mapped[list[str] | None]
    modes_orientation_accompagnateur: Mapped[list[str] | None]
    modes_orientation_accompagnateur_autres: Mapped[str | None]
    modes_orientation_beneficiaire: Mapped[list[str] | None]
    modes_orientation_beneficiaire_autres: Mapped[str | None]
    zone_diffusion_type: Mapped[str | None]
    zone_diffusion_code: Mapped[str | None]
    zone_diffusion_nom: Mapped[str | None]

    commune_: Mapped["Commune"] = relationship(back_populates="services")
