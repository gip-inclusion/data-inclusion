import pandas as pd
import pytest

from data_inclusion.scripts.tasks.sources import cd35


@pytest.fixture
def cd35_sample_df():
    return pd.DataFrame(
        [
            {
                "ORG_ID": "1",
                "THEM_NOM": "Mairies",
                "ORG_NOM": "Mairie d'Availles Sur Seiche",
                "ORG_SIGLE": "MAIRIE",
                "ORG_ADRES": "3 rue des Fontaines",
                "ORG_CP": "35130",
                "ORG_VILLE": "AVAILLES-SUR-SEICHE",
                "ORG_LONGITUDE": "-1.195570707321167",
                "ORG_LATITUDE": "47.96109390258789",
                "ORG_TEL": "1111111111",
                "ORG_MAIL": "availles.mairie@wanadoo.fr",
                "ORG_WEB": None,
                "ORG_HORAIRE": "  \t Lundi : 9h-12h30  \t Mardi et vendredi : 9h-12h30 et 14h-18h  \t Samedi : 9h-12h  ",
                "ORG_DESC": " Une commune est une collectivité locale gérée de manière autonome par le Maire. Il s'appuie sur ses adjoints et ses conseillers municipaux.      Les rôles du Maire et de son équipe sont définis par le Code Général des Collectivités Territoriales et les lois de transferts de compétences.      Les compétences :    Le champ d'intervention de la commune est très vaste. Outre la gestion de son domaine public, elle sert d'intermédiaire entre l'état et les citoyens.      Les responsabilités locales :    La commune est autonome pour de nombreuses actions:     \t En matière d'urbanisme, elle contrôle et planifie son urbanisme à l'aide du PLU (plan local d'urbanisme) et délivre les autorisations de construire,  \t Dans le domaine sanitaire et social, la commune met en œuvre l'action sociale facultative grâce aux centres communaux d'action sociale (CCAS : gestion des crèches, des foyers de personnes âgées),  \t Dans le domaine de l'enseignement, la commune a en charge les écoles préélémentaires et élémentaires (création et implantation, gestion et financement, à l'exception de la rémunération des enseignants),  \t Dans le domaine culturel, la commune crée et entretient des bibliothèques, musées, écoles de musique, salles de spectacle. Elle organise des manifestations culturelles,  \t Dans le domaine sportif et des loisirs, la commune crée et gère des équipements sportifs, elle subventionne des activités sportives, y compris les clubs sportifs professionnels, elle est en charge des aménagements touristiques.  \t Entretien de la voirie communale,  \t Protection de l'ordre public local par le biais du pouvoir de police du maire.  \t     La représentation de l'Etat :    Les maires et les adjoints accomplissent également des missions au nom de l'état, grâce aux moyens et aux personnels de la commune:     \t état civil (enregistrement des naissances, mariages et décès),  \t fonctions électorales (organisation des élections...),  \t recensement de la population française (organisé par l' INSEE ),  \t Elle est aussi chef de file pour fixer les modalités de l'action commune des collectivités territoriales et de leurs établissements publics pour l'exercice des compétences relatives àla mobilité durable,  \t l'organisation des services publics de proximité,  \t l'aménagement de l'espace et le développement local,  ",
                "ORG_DATECREA": "01-01-2019",
                "ORG_DATEMAJ": "03-07-2020",
                "PROF_NOM": None,
                "URL": "https://annuaire.ille-et-vilaine.fr/organisme/visualiser/1/0/0",
                "ASCOLLSIRET": "22350001800013",
                "ASCOLLNOM": "DEPARTEMENT D ILLE ET VILAINE",
                "ASCOLLMODIFICATION": "2022-06-30T17:36:24.3763874Z",
            }
        ]
    )


def test_preprocess_cd35(cd35_sample_df):
    df = cd35.transform_structure_dataframe(cd35_sample_df)

    assert df.to_dict(orient="records") == [
        {
            "id": "1",
            "siret": None,
            "rna": None,
            "nom": "Mairie d'Availles Sur Seiche",
            "commune": "AVAILLES-SUR-SEICHE",
            "code_postal": "35130",
            "code_insee": None,
            "adresse": "3 rue des Fontaines",
            "complement_adresse": None,
            "longitude": -1.195570707321167,
            "latitude": 47.96109390258789,
            "typologie": "MUNI",
            "telephone": "1111111111",
            "courriel": "availles.mairie@wanadoo.fr",
            "site_web": None,
            "presentation_resume": " Une commune est une collectivité locale gérée de manière autonome par le Maire. Il s'appuie sur ses adjoints et ses conseillers municipaux.      Les rôles du Maire et de son équipe sont définis par le Code Général des Collectivités Territoriales et les lois de transferts de com…",
            "presentation_detail": " Une commune est une collectivité locale gérée de manière autonome par le Maire. Il s'appuie sur ses adjoints et ses conseillers municipaux.      Les rôles du Maire et de son équipe sont définis par le Code Général des Collectivités Territoriales et les lois de transferts de compétences.      Les compétences :    Le champ d'intervention de la commune est très vaste. Outre la gestion de son domaine public, elle sert d'intermédiaire entre l'état et les citoyens.      Les responsabilités locales :    La commune est autonome pour de nombreuses actions:     \t En matière d'urbanisme, elle contrôle et planifie son urbanisme à l'aide du PLU (plan local d'urbanisme) et délivre les autorisations de construire,  \t Dans le domaine sanitaire et social, la commune met en œuvre l'action sociale facultative grâce aux centres communaux d'action sociale (CCAS : gestion des crèches, des foyers de personnes âgées),  \t Dans le domaine de l'enseignement, la commune a en charge les écoles préélémentaires et élémentaires (création et implantation, gestion et financement, à l'exception de la rémunération des enseignants),  \t Dans le domaine culturel, la commune crée et entretient des bibliothèques, musées, écoles de musique, salles de spectacle. Elle organise des manifestations culturelles,  \t Dans le domaine sportif et des loisirs, la commune crée et gère des équipements sportifs, elle subventionne des activités sportives, y compris les clubs sportifs professionnels, elle est en charge des aménagements touristiques.  \t Entretien de la voirie communale,  \t Protection de l'ordre public local par le biais du pouvoir de police du maire.  \t     La représentation de l'Etat :    Les maires et les adjoints accomplissent également des missions au nom de l'état, grâce aux moyens et aux personnels de la commune:     \t état civil (enregistrement des naissances, mariages et décès),  \t fonctions électorales (organisation des élections...),  \t recensement de la population française (organisé par l' INSEE ),  \t Elle est aussi chef de file pour fixer les modalités de l'action commune des collectivités territoriales et de leurs établissements publics pour l'exercice des compétences relatives àla mobilité durable,  \t l'organisation des services publics de proximité,  \t l'aménagement de l'espace et le développement local,  ",
            "source": "cd35",
            "date_maj": "2020-07-03",
            "antenne": False,
            "lien_source": "https://annuaire.ille-et-vilaine.fr/organisme/visualiser/1/0/0",
            "horaires_ouverture": "  \t Lundi : 9h-12h30  \t Mardi et vendredi : 9h-12h30 et 14h-18h  \t Samedi : 9h-12h  ",
            "accessibilite": None,
            "labels_nationaux": None,
            "labels_autres": None,
        }
    ]
