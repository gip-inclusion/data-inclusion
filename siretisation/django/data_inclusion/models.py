from django.contrib.postgres.fields import ArrayField
from django.db import models


class Structure(models.Model):
    class Meta:
        db_table = "structure"
        managed = False

    di_surrogate_id = models.TextField(db_column="_di_surrogate_id", primary_key=True)
    di_sirene_date_fermeture = models.DateField(db_column="_di_sirene_date_fermeture")
    di_sirene_etab_successeur = models.TextField(db_column="_di_sirene_etab_successeur")
    di_geocodage_score = models.FloatField(db_column="_di_geocodage_score")
    di_geocodage_code_insee = models.TextField(db_column="_di_geocodage_code_insee")
    di_annotated_siret = models.TextField(db_column="_di_annotated_siret")
    di_annotated_antenne = models.BooleanField(db_column="_di_annotated_antenne")
    di_email_is_pii = models.BooleanField(db_column="_di_email_is_pii")
    di_has_deprecated_siret = models.BooleanField(db_column="_di_has_deprecated_siret")

    id = models.TextField()
    siret = models.TextField()
    antenne = models.BooleanField()
    rna = models.TextField()
    nom = models.TextField()
    commune = models.TextField()
    code_postal = models.TextField()
    code_insee = models.TextField()
    adresse = models.TextField()
    complement_adresse = models.TextField()
    longitude = models.FloatField()
    latitude = models.FloatField()
    telephone = models.TextField()
    courriel = models.TextField()
    site_web = models.TextField()
    source = models.TextField()
    lien_source = models.TextField()
    horaires_ouverture = models.TextField()
    accessibilite = models.TextField()
    labels_nationaux = ArrayField(models.TextField())
    labels_autres = ArrayField(models.TextField())
    thematiques = ArrayField(models.TextField())
    typologie = models.TextField()
    presentation_resume = models.TextField()
    presentation_detail = models.TextField()
    date_maj = models.DateField()
