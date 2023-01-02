import pandas as pd
import pytest

from data_inclusion.scripts.tasks.sources import odspep


@pytest.fixture
def ressources_df():
    return pd.DataFrame(
        [
            {
                "ID_RES": "48368",
                "LIBELLE_SERVICE": None,
                "DESCRIPTION_SERVICE": "Accueil offre de service\n- Petite enfance\n- enfance et jeunesse\n- solidarité et insertion\n- logement et cadre de vie",
                "STRUCTURE": "CAF DE CREIL",
                "SERVICE_RSP": "Accueil Droits et Prestations - Droits et Informations sur aides",
                "ID_ADR": "24247",
                "L1_IDENTIFICATION_DEST_ADR": "CAF DE CREIL",
                "L2_IDENTITE_DEST_ADR": None,
                "L4_NUMERO_LIB_VOIE_ADR": "2 Rue Charles-Auguste Duguet",
                "L3_COMPLEMENT_ADR": None,
                "L5_MENTION_ADR": None,
                "L7_PAYS_ADR": "FRANCE",
                "LATITUDE_ADR": "49.260956",
                "LONGITUDE_ADR": "2.47524",
                "EST_NORMALISEE_ADR": "1",
                "CODE_COMMUNE_ADR": "60175",
                "CODE_POSTAL_ADR": "60100",
                "LIBELLE_COMMUNE_ADR": " Creil",
                "DATE DERNIERE MAJ": "2017-07-25 11:59:07",
            }
        ]
    )


@pytest.fixture
def contacts_df():
    return pd.DataFrame(
        [
            {
                "ID_RES": "48368",
                "ID_CTC": "23056",
                "TEL_1_CTC": "0810256080",
                "TEL_2_CTC": None,
                "FAX_CTC": None,
                "SITE_INTERNET_CTC": None,
                "MAIL_CTC": "www@caf.fr",
            }
        ]
    )


@pytest.fixture
def horaires_df():
    return pd.DataFrame()


@pytest.fixture
def familles_df():
    return pd.DataFrame(
        [
            {
                "ID_RES": "48368",
                "CODE_FAM": "1",
                "FamilleBesoin": "Contraintes personnelles",
            }
        ]
    )


@pytest.fixture
def categories_df():
    return pd.DataFrame(
        [
            {
                "ID_RES": "48368",
                "CODE_CAT": "3",
                "Besoin": "Se déplacer",
            },
            {
                "ID_RES": "48368",
                "CODE_CAT": "10",
                "Besoin": "Elaboration du projet professionnel",
            },
        ]
    )


@pytest.fixture
def sous_categories_df():
    return pd.DataFrame(
        [
            {
                "ID_RES": "48368",
                "CODE_SSC": "23",
                "Sous besoin": "Régler un problème administratif ou juridique",
            },
            {
                "ID_RES": "48368",
                "CODE_SSC": "16",
                "Sous besoin": "Bénéficier d'aides financières (hors celles de Pôle Emploi)",
            },
        ]
    )


def test_transform_odspep_structures(ressources_df, contacts_df, horaires_df):
    df = odspep.transform_structure_dataframe(
        ressources_df=ressources_df,
        contacts_df=contacts_df,
        horaires_df=horaires_df,
    )

    assert df.to_dict(orient="records") == [
        {
            "id": "48368",
            "siret": None,
            "rna": None,
            "nom": "CAF DE CREIL",
            "commune": " Creil",
            "code_postal": "60100",
            "code_insee": "60175",
            "adresse": "2 Rue Charles-Auguste Duguet",
            "complement_adresse": None,
            "longitude": "2.47524",
            "latitude": "49.260956",
            "typologie": "CAF",
            "telephone": "0810256080",
            "courriel": "www@caf.fr",
            "site_web": None,
            "presentation_resume": None,
            "presentation_detail": None,
            "source": "odspep",
            "date_maj": "2017-07-25T09:59:07+00:00",
            "antenne": False,
            "lien_source": None,
            "horaires_ouverture": None,
            "accessibilite": None,
            "labels_nationaux": None,
            "labels_autres": None,
            "thematiques": None,
        }
    ]


def test_transform_odspep_services(
    ressources_df,
    familles_df,
    categories_df,
    sous_categories_df,
):
    df = odspep.transform_service_dataframe(
        ressources_df=ressources_df,
        familles_df=familles_df,
        categories_df=categories_df,
        sous_categories_df=sous_categories_df,
    )

    assert df.to_dict(orient="records") == [
        {
            "id": "48368",
            "structure_id": "48368",
            "source": "odspep",
            "nom": "Accueil Droits et Prestations - Droits et Informations sur aides",
            "presentation_resume": "Accueil offre de service\n- Petite enfance\n- enfance et jeunesse\n- solidarité et insertion\n- logement et cadre de vie",
            "types": None,
            "thematiques": ["mobilite", "choisir-un-metier"],
            "prise_rdv": None,
            "frais": None,
            "frais_autres": None,
            "profils": None,
        }
    ]
