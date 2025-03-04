import pandas as pd

from data_inclusion.api.core import db
from data_inclusion.api.inclusion_data import commands, models


def test_store_inclusion_data_scores_doublons(db_session):
    structures_df = pd.DataFrame(
        {
            "_di_surrogate_id": ["s1", "s2", "s3"],
            "id": ["id1", "id2", "id3"],
            "nom": ["Structure 1", "Structure 2", "Structure 3"],
            "date_maj": ["2023-01-01", "2023-01-01", "2023-01-01"],
            "source": ["source1", "source2", "source1"],
            "cluster_id": ["cluster1", "cluster1", None],
        }
    )

    services_df = pd.DataFrame(
        {
            "_di_surrogate_id": ["sv1", "sv2", "sv3"],
            "_di_structure_surrogate_id": ["s1", "s1", "s2"],
            "id": ["sid1", "sid2", "sid3"],
            "source": ["source1", "source1", "source2"],
            "nom": ["Service 1", "Service 2", "Service 3"],
            "date_maj": ["2023-01-01", "2023-01-01", "2023-01-01"],
            "score_qualite": [0.7, 0.8, 0.5],
        }
    )

    commands.store_inclusion_data(db_session, structures_df, services_df)

    with db.SessionLocal() as session:
        db_structures = session.query(models.Structure).all()
        structures_by_id = {s.id: s for s in db_structures}
        assert structures_by_id["id1"].score_qualite == 0.75  # average of 0.7 and 0.8
        assert structures_by_id["id1"].doublons == [
            {
                "accessibilite": None,
                "adresse": None,
                "antenne": None,
                "code_insee": None,
                "code_postal": None,
                "commune": None,
                "complement_adresse": None,
                "courriel": None,
                "date_maj": "2023-01-01",
                "horaires_ouverture": None,
                "id": "id2",
                "labels_autres": None,
                "labels_nationaux": None,
                "latitude": None,
                "lien_source": None,
                "longitude": None,
                "nom": "Structure 2",
                "presentation_detail": None,
                "presentation_resume": None,
                "rna": None,
                "siret": None,
                "site_web": None,
                "source": "source2",
                "telephone": None,
                "thematiques": None,
                "typologie": None,
            }
        ]
        assert structures_by_id["id2"].score_qualite == 0.5
        assert structures_by_id["id2"].doublons[0]["id"] == "id1"
        assert structures_by_id["id3"].score_qualite == 0.0  # no services, default to 0
        assert structures_by_id["id3"].doublons == []
