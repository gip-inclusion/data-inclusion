import pandas as pd

from data_inclusion.api.inclusion_data import commands, models


def test_prepare_dataset_doublons():
    structures_df = pd.DataFrame(
        {c.name: None for c in models.Structure.__table__.columns}
        | {
            "_di_surrogate_id": ["s1", "s2", "s3"],
            "id": ["id1", "id2", "id3"],
            "nom": ["Structure 1", "Structure 2", "Structure 3"],
            "date_maj": ["2023-01-01", "2023-01-01", "2023-01-01"],
            "source": ["source1", "source2", "source1"],
            "cluster_id": ["cluster1", "cluster1", None],
        }
    )

    services_df = pd.DataFrame(
        {c.name: None for c in models.Service.__table__.columns}
        | {
            "_di_surrogate_id": ["sv1", "sv2", "sv3"],
            "_di_structure_surrogate_id": ["s1", "s1", "s2"],
            "structure_id": ["id1", "id2", "id3"],
            "id": ["sid1", "sid2", "sid3"],
            "source": ["source1", "source1", "source2"],
            "nom": ["Service 1", "Service 2", "Service 3"],
            "date_maj": ["2023-01-01", "2023-01-01", "2023-01-01"],
            "score_qualite": [0.7, 0.8, 0.5],
        }
    )

    structures_df, services_df = commands.prepare_dataset(structures_df, services_df)

    structures_df = structures_df.set_index("id")

    assert structures_df.loc["id1"].cluster_best_duplicate == "s1"
    assert structures_df.loc["id1"].score_qualite == 0.75  # average of 0.7 and 0.8
    assert structures_df.loc["id1"].doublons == [
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

    assert structures_df.loc["id2"].cluster_best_duplicate == "s1"
    assert structures_df.loc["id2"].score_qualite == 0.5
    assert structures_df.loc["id2"].doublons[0]["id"] == "id1"

    assert structures_df.loc["id3"].cluster_best_duplicate is None
    assert structures_df.loc["id3"].score_qualite == 0.0  # no services, default to 0
    assert structures_df.loc["id3"].doublons == []


def test_prepare_dataset_doublons_date_maj():
    structures_df = pd.DataFrame(
        {c.name: None for c in models.Structure.__table__.columns}
        | {
            "_di_surrogate_id": ["s1", "s2", "s3"],
            "id": ["id1", "id2", "id3"],
            "nom": ["Structure 1", "Structure 2", "Structure 3"],
            "date_maj": ["2023-01-01", "2025-06-21", "2023-01-01"],
            "source": ["source1", "source2", "source1"],
            "cluster_id": ["cluster1", "cluster1", None],
        }
    )

    services_df = pd.DataFrame(
        {c.name: None for c in models.Service.__table__.columns}
        | {
            "_di_surrogate_id": ["sv1"],
            "_di_structure_surrogate_id": ["s1"],
            "structure_id": ["id1"],
            "nom": ["Service 1"],
            "id": ["sid1"],
            "source": ["source1"],
            "score_qualite": [0.0],
        }
    )

    structures_df, services_df = commands.prepare_dataset(structures_df, services_df)

    structures_df = structures_df.set_index("id")

    assert structures_df.loc["id1"].cluster_best_duplicate == "s2"
    assert structures_df.loc["id1"].score_qualite == 0.0
    assert structures_df.loc["id2"].cluster_best_duplicate == "s2"
    assert structures_df.loc["id2"].score_qualite == 0.0
