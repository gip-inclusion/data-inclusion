from unittest.mock import ANY

import pytest

from common import annuaire_entreprises

pytestmark = pytest.mark.annuaire_entreprises_api


@pytest.fixture
def annuaire_entreprises_client():
    return annuaire_entreprises.AnnuaireEntreprisesClient(base_url="https://recherche-entreprises.api.gouv.fr")


def test_annuaire_entreprises_search(
    annuaire_entreprises_client: annuaire_entreprises.AnnuaireEntreprisesClient,
):
    response_data = annuaire_entreprises_client.search(
        q="Commune Lille",
        code_commune="59350",
        activite_principale=["84.11Z"],
    )

    assert response_data == {"results": ANY, "total_results": ANY, "page": 1, "per_page": 20, "total_pages": ANY}
    assert len(response_data["results"]) > 0
    assert len(response_data["results"][0]["matching_etablissements"]) > 0

    matching_sirets = [
        etablissement_data["siret"] for etablissement_data in response_data["results"][0]["matching_etablissements"]
    ]
    assert "21590350100017" in matching_sirets
