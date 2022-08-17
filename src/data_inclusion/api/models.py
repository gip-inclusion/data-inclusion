import sqlalchemy as sqla
from sqlalchemy.dialects.postgresql import ARRAY

from data_inclusion.api.core import db


class Structure(db.Base):
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
