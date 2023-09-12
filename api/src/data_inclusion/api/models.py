import geoalchemy2
import sqlalchemy as sqla
from sqlalchemy import orm
from sqlalchemy.dialects.postgresql import ARRAY

from data_inclusion.api.core.db import Base

# all fields are nullable or have a default value. These models will only be used to
# query valid data coming from the data pipeline.


class Commune(Base):
    __tablename__ = "admin_express_communes"

    code = sqla.Column(sqla.Text, primary_key=True)
    nom = sqla.Column(sqla.Text)
    departement = sqla.Column(sqla.Text)
    region = sqla.Column(sqla.Text)
    siren_epci = sqla.Column(sqla.Text)
    geom = sqla.Column(geoalchemy2.Geometry("Geometry", srid=4326))

    services = orm.relationship("Service", back_populates="commune_")


class EPCI(Base):
    __tablename__ = "admin_express_epcis"

    code = sqla.Column(sqla.Text, primary_key=True)
    nom = sqla.Column(sqla.Text)
    nature = sqla.Column(sqla.Text)
    geom = sqla.Column(geoalchemy2.Geometry("Geometry", srid=4326))


class Departement(Base):
    __tablename__ = "admin_express_departements"

    code = sqla.Column(sqla.Text, primary_key=True)
    nom = sqla.Column(sqla.Text)
    insee_reg = sqla.Column(sqla.Text)
    geom = sqla.Column(geoalchemy2.Geometry("Geometry", srid=4326))


class Region(Base):
    __tablename__ = "admin_express_regions"

    code = sqla.Column(sqla.Text, primary_key=True)
    nom = sqla.Column(sqla.Text)
    geom = sqla.Column(geoalchemy2.Geometry("Geometry", srid=4326))


class Structure(Base):
    __tablename__ = "structure"

    # internal metadata
    _di_surrogate_id = sqla.Column(sqla.Text, primary_key=True)
    _di_geocodage_code_insee = sqla.Column(sqla.Text, nullable=True)
    _di_geocodage_score = sqla.Column(sqla.Float, nullable=True)

    # structure data
    id = sqla.Column(sqla.Text, nullable=True)
    siret = sqla.Column(sqla.Text, nullable=True)
    rna = sqla.Column(sqla.Text, nullable=True)
    nom = sqla.Column(sqla.Text, nullable=True)
    commune = sqla.Column(sqla.Text, nullable=True)
    code_postal = sqla.Column(sqla.Text, nullable=True)
    code_insee = sqla.Column(sqla.Text, nullable=True)
    adresse = sqla.Column(sqla.Text, nullable=True)
    complement_adresse = sqla.Column(sqla.Text, nullable=True)
    longitude = sqla.Column(sqla.Float, nullable=True)
    latitude = sqla.Column(sqla.Float, nullable=True)
    typologie = sqla.Column(sqla.Text, nullable=True)
    telephone = sqla.Column(sqla.Text, nullable=True)
    courriel = sqla.Column(sqla.Text, nullable=True)
    site_web = sqla.Column(sqla.Text, nullable=True)
    presentation_resume = sqla.Column(sqla.Text, nullable=True)
    presentation_detail = sqla.Column(sqla.Text, nullable=True)
    source = sqla.Column(sqla.Text, nullable=True)
    date_maj = sqla.Column(sqla.Date(), nullable=True)
    antenne = sqla.Column(sqla.Boolean, default=False)
    lien_source = sqla.Column(sqla.Text, nullable=True)
    horaires_ouverture = sqla.Column(sqla.Text, nullable=True)
    accessibilite = sqla.Column(sqla.Text, nullable=True)
    labels_nationaux = sqla.Column(ARRAY(sqla.Text), default=list)
    labels_autres = sqla.Column(ARRAY(sqla.Text), default=list)
    thematiques = sqla.Column(ARRAY(sqla.Text), default=list)

    services = orm.relationship("Service", back_populates="structure")


class Service(Base):
    __tablename__ = "service"

    # internal metadata
    _di_surrogate_id = sqla.Column(sqla.Text, primary_key=True)
    _di_structure_surrogate_id = sqla.Column(
        sqla.ForeignKey("structure._di_surrogate_id")
    )
    _di_geocodage_code_insee = sqla.Column(sqla.Text, nullable=True)
    _di_geocodage_score = sqla.Column(sqla.Float, nullable=True)
    structure = orm.relationship("Structure", back_populates="services")

    # service data
    id = sqla.Column(sqla.Text, nullable=True)
    structure_id = sqla.Column(sqla.Text, nullable=True)
    source = sqla.Column(sqla.Text, nullable=True)
    nom = sqla.Column(sqla.Text, nullable=True)
    presentation_resume = sqla.Column(sqla.Text, nullable=True)
    presentation_detail = sqla.Column(sqla.Text, nullable=True)
    types = sqla.Column(ARRAY(sqla.Text), default=list)
    thematiques = sqla.Column(ARRAY(sqla.Text), default=list)
    prise_rdv = sqla.Column(sqla.Text, nullable=True)
    frais = sqla.Column(ARRAY(sqla.Text), default=list)
    frais_autres = sqla.Column(sqla.Text, nullable=True)
    profils = sqla.Column(ARRAY(sqla.Text), default=list)
    pre_requis = sqla.Column(ARRAY(sqla.Text), default=list)
    cumulable = sqla.Column(sqla.Boolean, default=False)
    justificatifs = sqla.Column(ARRAY(sqla.Text), default=list)
    formulaire_en_ligne = sqla.Column(sqla.Text, nullable=True)
    commune = sqla.Column(sqla.Text, nullable=True)
    code_postal = sqla.Column(sqla.Text, nullable=True)
    code_insee = sqla.Column(
        sqla.ForeignKey("admin_express_communes.code"), nullable=True
    )
    adresse = sqla.Column(sqla.Text, nullable=True)
    complement_adresse = sqla.Column(sqla.Text, nullable=True)
    longitude = sqla.Column(sqla.Float, nullable=True)
    latitude = sqla.Column(sqla.Float, nullable=True)
    recurrence = sqla.Column(sqla.Text, nullable=True)
    date_creation = sqla.Column(sqla.Date(), nullable=True)
    date_suspension = sqla.Column(sqla.Date(), nullable=True)
    lien_source = sqla.Column(sqla.Text, nullable=True)
    telephone = sqla.Column(sqla.Text, nullable=True)
    courriel = sqla.Column(sqla.Text, nullable=True)
    contact_public = sqla.Column(sqla.Boolean, default=False)
    contact_nom_prenom = sqla.Column(sqla.Text, default=False)
    date_maj = sqla.Column(sqla.Date(), nullable=True)
    modes_accueil = sqla.Column(ARRAY(sqla.Text), default=list)
    modes_orientation_accompagnateur = sqla.Column(ARRAY(sqla.Text), default=list)
    modes_orientation_accompagnateur_autres = sqla.Column(sqla.Text, nullable=True)
    modes_orientation_beneficiaire = sqla.Column(ARRAY(sqla.Text), default=list)
    modes_orientation_beneficiaire_autres = sqla.Column(sqla.Text, nullable=True)
    zone_diffusion_type = sqla.Column(sqla.Text, nullable=True)
    zone_diffusion_code = sqla.Column(sqla.Text, nullable=True)
    zone_diffusion_nom = sqla.Column(sqla.Text, nullable=True)

    commune_ = orm.relationship("Commune", back_populates="services")
