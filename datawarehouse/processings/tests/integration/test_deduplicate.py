import pytest

from data_inclusion.processings import deduplicate

STRUCTURES = [
    {
        "_di_surrogate_id": "dora-29259c03-03c6-4a7a-979a-29e97ce1e1a3",
        "source": "dora",
        "siret": "31801050100092",
        "nom": "LE PONT",
        "commune": "Mâcon",
        "code_postal": "71000",
        "code_insee": "71270",
        "adresse": "80 Rue de Lyon",
        "longitude": 4.823484,
        "latitude": 46.298757,
        "telephone": "",
        "courriel": "",
        "site_web": "",
        "date_maj": "2024-02-23",
        "antenne": "f",
    },
    {
        "_di_surrogate_id": (
            "emplois-de-linclusion-3b2c8e75-d0f0-425f-b224-b4987e7b2d4d"
        ),
        "source": "emplois-de-linclusion",
        "siret": "31801050100092",
        "nom": "LE PONT",
        "commune": "Mâcon",
        "code_postal": "71000",
        "code_insee": "71270",
        "adresse": "80 Rue de Lyon",
        "longitude": 4.823484,
        "latitude": 46.298757,
        "telephone": "",
        "courriel": "",
        "date_maj": "2023-12-19",
        "antenne": "f",
    },
]


def test_deduplicate():
    result_df = deduplicate(data=STRUCTURES)

    assert result_df == [
        {
            "cluster_id": 0,
            "score": pytest.approx(0.99, 0.01),
            "structure_id": "dora-29259c03-03c6-4a7a-979a-29e97ce1e1a3",
        },
        {
            "cluster_id": 0,
            "score": pytest.approx(0.99, 0.01),
            "structure_id": (
                "emplois-de-linclusion-3b2c8e75-d0f0-425f-b224-b4987e7b2d4d"
            ),
        },
    ]
