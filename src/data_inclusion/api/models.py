import sqlalchemy as sqla
from sqlalchemy import orm
from sqlalchemy.dialects.postgresql import ARRAY

from data_inclusion.api.core.db import Base

# all fields are nullable or have a default value. These models will only be used to
# query valid data coming from the data pipeline.


class Structure(Base):
    __tablename__ = "structure"

    # metadata
    index = sqla.Column(sqla.Integer, primary_key=True)
    created_at = sqla.Column(
        sqla.DateTime(timezone=True), server_default=sqla.func.now()
    )
    batch_id = sqla.Column(sqla.Text)
    src_url = sqla.Column(sqla.Text)

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
    date_maj = sqla.Column(sqla.DateTime(timezone=True), nullable=True)
    structure_parente = sqla.Column(sqla.Text, nullable=True)
    lien_source = sqla.Column(sqla.Text, nullable=True)
    horaires_ouverture = sqla.Column(sqla.Text, nullable=True)
    accessibilite = sqla.Column(sqla.Text, nullable=True)
    labels_nationaux = sqla.Column(ARRAY(sqla.Text), default=list)
    labels_autres = sqla.Column(ARRAY(sqla.Text), default=list)
    thematiques = sqla.Column(ARRAY(sqla.Text), default=list)
    services = orm.relationship("Service", backref="structure", cascade="all, delete")


class Service(Base):
    __tablename__ = "service"

    # metadata
    index = sqla.Column(sqla.Integer, primary_key=True)
    structure_index = sqla.Column(sqla.Integer, sqla.ForeignKey("structure.index"))

    # service data
    nom = sqla.Column(sqla.Text, nullable=True)
    presentation_resume = sqla.Column(sqla.Text, nullable=True)
    types = sqla.Column(ARRAY(sqla.Text), default=list)
    thematiques = sqla.Column(ARRAY(sqla.Text), default=list)
    prise_rdv = sqla.Column(sqla.Text, nullable=True)
    frais = sqla.Column(ARRAY(sqla.Text), default=list)
    frais_autres = sqla.Column(sqla.Text, nullable=True)
