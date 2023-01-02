import pandas as pd
import pytest

from data_inclusion.scripts.tasks.sources import cd72


@pytest.fixture
def cd72_sample_df():
    return pd.DataFrame(
        [
            {
                "Nom Structure": "ADGESTI",
                "SIRET": "32431199999999",
                "ID Structure": "7",
                "check action": "7",
                "Mis à jour le :": "2018-08-21 14:58:25",
                "Type de structure": "Association",
                "Typologie structure": "Associations",
                "Secteur d'activité": None,
                "Description": "L'association ADGESTI Espérance Sarthe, a mis en place le développe des actions pour favoriser l'insertion sociale et professionnelle de personnes en situation de handicap d'origine psychique.",
                "Partenariat": None,
                "Date de dernière rencontre ou échange important": None,
                "Adresse": "21 rue Albert Einstein",
                "Code postal": "72650",
                "Ville": "La Chapelle St Aubin",
                "Téléphone accueil": None,
                "E-mail accueil": "sec@adgesti.fr",
                "Site Internet": "http://www.adgesti.fr/",
                "Horaires": "9h/12h30 - 14h/18h (fermeture - 17h00 le vendredi)",
                "Nom Responsable": "John Doe",
                "Poste Responsable": "Directrice Générale",
                "Téléphone principal": "02 99 99 99 99",
                "Téléphone secondaire": None,
                "E-mail Responsable": "jdoe@adgesti.fr",
            }
        ]
    )


def test_transform_cd72(cd72_sample_df):
    df = cd72.transform_structure_dataframe(cd72_sample_df)

    assert df.to_dict(orient="records") == [
        {
            "id": "7",
            "siret": "32431199999999",
            "rna": None,
            "nom": "ADGESTI",
            "commune": "La Chapelle St Aubin",
            "code_postal": "72650",
            "code_insee": None,
            "adresse": "21 rue Albert Einstein",
            "complement_adresse": None,
            "longitude": None,
            "latitude": None,
            "typologie": "ASSO",
            "telephone": "02 99 99 99 99",
            "courriel": "sec@adgesti.fr",
            "site_web": "http://www.adgesti.fr/",
            "presentation_resume": "L'association ADGESTI Espérance Sarthe, a mis en place le développe des actions pour favoriser l'insertion sociale et professionnelle de personnes en situation de handicap d'origine psychique.",
            "presentation_detail": None,
            "source": "cd72",
            "date_maj": "2018-08-21T14:58:25",
            "antenne": False,
            "lien_source": None,
            "horaires_ouverture": "9h/12h30 - 14h/18h (fermeture - 17h00 le vendredi)",
            "accessibilite": None,
            "labels_nationaux": None,
            "labels_autres": None,
            "thematiques": None,
        }
    ]
