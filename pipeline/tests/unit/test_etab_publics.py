import pandas as pd
import pytest

from data_inclusion.scripts.tasks.sources import etab_publics


@pytest.fixture
def etab_publics_sample_df():
    return pd.DataFrame(
        [
            {
                "plage_ouverture": [
                    {
                        "nom_jour_debut": "Lundi",
                        "nom_jour_fin": "Vendredi",
                        "valeur_heure_debut_1": "09:00:00",
                        "valeur_heure_fin_1": "12:00:00",
                        "valeur_heure_debut_2": "14:00:00",
                        "valeur_heure_fin_2": "17:00:00",
                        "commentaire": "",
                    },
                    {
                        "nom_jour_debut": "Samedi",
                        "nom_jour_fin": "Samedi",
                        "valeur_heure_debut_1": "09:00:00",
                        "valeur_heure_fin_1": "12:00:00",
                        "valeur_heure_debut_2": "",
                        "valeur_heure_fin_2": "",
                        "commentaire": "Accueil ouvert uniquement le 2e et 4e samedis de chaque mois.",
                    },
                ],
                "site_internet": [{"libelle": "", "valeur": "http://www.sandillon.fr"}],
                "copyright": "Direction de l'information légale et administrative (Première ministre)",
                "siren": None,
                "ancien_code_pivot": "mairie-45300-01",
                "texte_reference": [],
                "partenaire": None,
                "telecopie": ["01 02 03 04 05"],
                "nom": "Mairie - Sandillon",
                "siret": "01234567890123",
                "itm_identifiant": "374012",
                "sigle": None,
                "date_modification": "08/03/2021 14:14:36",
                "adresse_courriel": ["bob@sandillon.fr"],
                "service_disponible": None,
                "organigramme": [],
                "pivot": [
                    {
                        "type_service_local": "mairie",
                        "code_insee_commune": ["45300"],
                    }
                ],
                "partenaire_identifiant": None,
                "ancien_identifiant": [],
                "id": "b94f406e-0b1b-4902-974c-3439a08ec1b0",
                "ancien_nom": [],
                "commentaire_plage_ouverture": None,
                "annuaire": [],
                "hierarchie": [],
                "categorie": "SL",
                "sve": [],
                "version_type": "Publiable",
                "type_repertoire": None,
                "telephone": [{"valeur": "01 02 03 04 05", "description": ""}],
                "version_etat_modification": None,
                "date_creation": "18/01/2012 00:00:00",
                "partenaire_date_modification": None,
                "mission": None,
                "formulaire_contact": [],
                "version_source": None,
                "type_organisme": None,
                "code_insee_commune": "45300",
                "statut_de_diffusion": "true",
                "adresse": [
                    {
                        "type_adresse": "Adresse",
                        "complement1": "",
                        "complement2": "",
                        "numero_voie": "251 route d'Orléans",
                        "service_distribution": "",
                        "code_postal": "45640",
                        "nom_commune": "Sandillon",
                        "pays": "",
                        "continent": "",
                        "longitude": "2.02933001518",
                        "latitude": "47.8457984924",
                        "accessibilite": "ACC",
                        "note_accessibilite": "rampe d'accès",
                    }
                ],
                "information_complementaire": None,
                "date_diffusion": None,
            }
        ]
    )


def test_transform_mes_aides(etab_publics_sample_df):
    df = etab_publics.transform_structure_dataframe(etab_publics_sample_df)

    assert df.to_dict(orient="records") == [
        {
            "id": "b94f406e-0b1b-4902-974c-3439a08ec1b0",
            "siret": "01234567890123",
            "rna": None,
            "nom": "Mairie - Sandillon",
            "commune": "Sandillon",
            "code_postal": "45640",
            "code_insee": "45300",
            "adresse": "251 route d'Orléans",
            "complement_adresse": None,
            "longitude": "2.02933001518",
            "latitude": "47.8457984924",
            "typologie": "MUNI",
            "telephone": "01 02 03 04 05",
            "courriel": "bob@sandillon.fr",
            "site_web": "http://www.sandillon.fr",
            "presentation_resume": None,
            "presentation_detail": None,
            "source": "etab_publics",
            "date_maj": "2021-03-08T13:14:36+00:00",
            "antenne": False,
            "lien_source": None,
            "horaires_ouverture": '[{"nom_jour_debut": "Lundi", "nom_jour_fin": "Vendredi", "valeur_heure_debut_1": "09:00:00", "valeur_heure_fin_1": "12:00:00", "valeur_heure_debut_2": "14:00:00", "valeur_heure_fin_2": "17:00:00", "commentaire": ""}, {"nom_jour_debut": "Samedi", "nom_jour_fin": "Samedi", "valeur_heure_debut_1": "09:00:00", "valeur_heure_fin_1": "12:00:00", "valeur_heure_debut_2": "", "valeur_heure_fin_2": "", "commentaire": "Accueil ouvert uniquement le 2e et 4e samedis de chaque mois."}]',
            "accessibilite": None,
            "labels_nationaux": [],
            "labels_autres": None,
            "thematiques": None,
        }
    ]
