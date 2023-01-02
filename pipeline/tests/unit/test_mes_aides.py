import pandas as pd
import pytest

from data_inclusion.scripts.tasks.sources import mes_aides


@pytest.fixture
def mes_aides_garages_sample_df():
    return pd.DataFrame(
        [
            {
                "id": "rec00wCc3oOYu694W",
                "createdTime": "2022-06-27T11:50:39.000Z",
                "fields": {
                    "Nom": "Les mécanos du coeur",
                    "ID": "recMT8WSOxEsqH0NY",
                    "Région Nom": "Provence-Alpes-Côte d'Azur",
                    "Département Nom": "Bouches-du-Rhône",
                    "Adresse": "12 traverse Magnan",
                    "Code Postal": "13001",
                    "Ville Nom": "Marseille",
                    "Ville Latitude": "43.29990094",
                    "Ville Longitude": "5.38227870",
                    "Téléphone": "04 99 99 99 99",
                    "Email": "mecanosducoeur@email.fr",
                    "Url": "http://mecanosducoeur.fr/",
                    "Plan Url": "https://api.mapbox.com/styles/v1/mapbox/streets-v11/static/pin-s+555555(5.38227869795,43.2999009436)/5.38227869795,43.2999009436,9,0/600x400?access_token=pk.eyJ1IjoibWVzYWlkZXMiLCJhIjoiY2t5MDdyaXNuMDBodTJ4b2F5ZWplbWNjbSJ9.eCFoUPCSLOSQa0xPy4k5iw",
                    "Partenaire Nom": None,
                    "Type": "Garage solidaire",
                    "Critères d'éligibilité": "Le prix des prestations est de 15€ de l’heure de main d’œuvre pour les personnes qui perçoivent les minima sociaux, 30€ pour les personnes non imposables, 40€ pour les personnes imposables. Il existe des forfaits pour les vidanges, freins , etc.\nLes propriétaires des voitures peuvent être présents pendant les réparations pour participer si nécessaire aux travaux.\nPar ailleurs le tarif en auto-réparation varie entre 5€ et 15€ de l'heure selon l'autonomie de l'adhérent et l'utilisation ou non du pont.\n",
                    "Services": ["Achat", "Réparation"],
                    "En Ligne": True,
                    "Créé le": "2022-05-06T17:05:00.000Z",
                    "Modifié le": "2022-07-13T14:58:00.000Z",
                    "Types de véhicule": None,
                },
            }
        ]
    )


def test_transform_mes_aides_garages_structure(mes_aides_garages_sample_df):
    df = mes_aides.transform_structure_dataframe(mes_aides_garages_sample_df)

    assert df.to_dict(orient="records") == [
        {
            "id": "recMT8WSOxEsqH0NY",
            "siret": None,
            "rna": None,
            "nom": "Les mécanos du coeur",
            "commune": "Marseille",
            "code_postal": "13001",
            "code_insee": None,
            "adresse": "12 traverse Magnan",
            "complement_adresse": None,
            "longitude": "5.38227870",
            "latitude": "43.29990094",
            "typologie": None,
            "telephone": "04 99 99 99 99",
            "courriel": "mecanosducoeur@email.fr",
            "site_web": "http://mecanosducoeur.fr/",
            "presentation_resume": None,
            "presentation_detail": None,
            "source": "mes-aides",
            "date_maj": "2022-07-13T14:58:00+00:00",
            "antenne": False,
            "lien_source": None,
            "horaires_ouverture": None,
            "accessibilite": None,
            "labels_nationaux": None,
            "labels_autres": None,
            "thematiques": ["mobilite"],
        }
    ]


def test_transform_mes_aides_garages_service(mes_aides_garages_sample_df):
    df = mes_aides.transform_service_dataframe(mes_aides_garages_sample_df)

    assert df.to_dict(orient="records") == [
        {
            "id": "recMT8WSOxEsqH0NY-achat",
            "structure_id": "recMT8WSOxEsqH0NY",
            "source": "mes-aides",
            "nom": "Achat",
            "presentation_resume": None,
            "types": ["aide-materielle"],
            "thematiques": ["mobilite"],
            "prise_rdv": None,
            "frais": None,
            "frais_autres": None,
            "profils": None,
        },
        {
            "id": "recMT8WSOxEsqH0NY-réparation",
            "structure_id": "recMT8WSOxEsqH0NY",
            "source": "mes-aides",
            "nom": "Réparation",
            "presentation_resume": None,
            "types": ["aide-materielle"],
            "thematiques": ["mobilite"],
            "prise_rdv": None,
            "frais": None,
            "frais_autres": None,
            "profils": None,
        },
    ]
