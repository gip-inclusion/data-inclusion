import json
from unittest.mock import ANY

import pytest

from data_inclusion.api.decoupage_administratif import constants
from data_inclusion.api.decoupage_administratif.constants import RegionEnum
from data_inclusion.api.decoupage_administratif.models import Commune
from data_inclusion.api.inclusion_data.v0 import models as models_v0
from data_inclusion.api.inclusion_data.v1 import models as models_v1
from data_inclusion.api.utils import soliguide
from data_inclusion.schema import v0, v1

from ...factories import v0 as v0_factories, v1 as v1_factories

DUNKERQUE = {"code_insee": "59183", "latitude": 51.0361, "longitude": 2.3770}
HAZEBROUCK = {"code_insee": "59295", "latitude": 50.7262, "longitude": 2.5387}
LILLE = {"code_insee": "59350", "latitude": 50.633333, "longitude": 3.066667}
MAUBEUGE = {"code_insee": "59392"}
PARIS = {"code_insee": "75056", "latitude": 48.866667, "longitude": 2.333333}
PARIS_11 = {"code_insee": "75111", "latitude": 48.86010, "longitude": 2.38160}
ROUBAIX = {"code_insee": "59512"}
STRASBOURG = {"code_insee": "67482"}


def list_resources_data(resp_data):
    return [item.get("service", item) for item in resp_data["items"]]


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
def test_list_structures_unauthenticated(api_client, schema_version):
    url = f"/api/{schema_version}/structures/"
    response = api_client.get(url)

    assert response.status_code == 403


@pytest.mark.parametrize(
    ("schema_version", "instance"),
    [
        (
            "v0",
            models_v0.Structure(
                _di_surrogate_id="04cfed4d-d3f7-4f9f-ad97-64bb6bbd7c96",
                accessibilite="https://acceslibre.beta.gouv.fr/app/kitchen-amount/",
                adresse="49, avenue de Pichon",
                code_insee="59350",
                code_postal="46873",
                commune="Sainte CharlotteBourg",
                complement_adresse=None,
                courriel="levyalexandre@example.org",
                date_maj="2023-01-01",
                doublons=[],
                horaires_ouverture='Mo-Fr 10:00-20:00 "sur rendez-vous"; PH off',
                id="lot-kitchen-amount",
                labels_autres=["Nièvre médiation numérique"],
                labels_nationaux=[],
                latitude=-20.074628,
                longitude=99.899603,
                nom="Perrin",
                presentation_detail="Or personne jambe.",
                presentation_resume="Image voie battre.",
                rna="W242194892",
                score_qualite=0.7,
                siret="76475938700658",
                site_web="https://www.le.net/",
                source="dora",
                telephone="0102030405",
                thematiques=["famille--garde-denfants"],
                typologie="ACI",
            ),
        ),
        (
            "v1",
            models_v1.Structure(
                accessibilite_lieu="https://acceslibre.beta.gouv.fr/app/kitchen-amount/",
                adresse="49, avenue de Pichon",
                code_insee="59350",
                code_postal="46873",
                commune="Sainte CharlotteBourg",
                complement_adresse=None,
                courriel="levyalexandre@example.org",
                date_maj="2023-01-01",
                doublons=[],
                horaires_accueil="Mo-Fr 10:00-20:00",
                id="dora--lot-kitchen-amount",
                reseaux_porteurs=[],
                latitude=-20.074628,
                longitude=99.899603,
                nom="Perrin",
                description="""Lorem ipsum dolor sit amet, consectetur adipiscing elit,
                sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.""",
                score_qualite=0.7,
                siret="76475938700658",
                site_web="https://www.le.net/",
                source="dora",
                telephone="0102030405",
            ),
        ),
    ],
)
@pytest.mark.parametrize("path", ["/structures"])
@pytest.mark.with_token
def test_list_structures_all(api_client, db_session, url, snapshot, instance):
    db_session.add(instance)
    db_session.commit()

    response = api_client.get(url)

    assert (
        json.dumps(response.json(), indent=2, ensure_ascii=False, sort_keys=True)
        == snapshot
    )


def assert_paginated_response_data(
    data,
    items=ANY,
    total=ANY,
    page=ANY,
    pages=ANY,
    size=ANY,
):
    assert data == {
        "items": items,
        "total": total,
        "page": page,
        "pages": pages,
        "size": size,
    }


@pytest.mark.parametrize("schema_version", ["v0"])
@pytest.mark.with_token
def test_list_structures_filter_by_typology(
    api_client,
    schema_version,
    structure_factory,
):
    structure_1 = structure_factory(typologie=v0.TypologieStructure.ASSO.value)
    structure_factory(typologie=v0.TypologieStructure.CCAS.value)

    url = f"/api/{schema_version}/structures/"
    response = api_client.get(
        url, params={"typologie": v0.TypologieStructure.ASSO.value}
    )

    resp_data = response.json()
    assert_paginated_response_data(response.json(), total=1)
    assert resp_data["items"][0]["id"] == structure_1.id

    response = api_client.get(
        url, params={"typologie": v0.TypologieStructure.MUNI.value}
    )
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.parametrize("schema_version", ["v0"])
@pytest.mark.with_token
def test_list_structures_filter_by_label(
    api_client,
    schema_version,
    structure_factory,
):
    structure_factory(labels_nationaux=[v0.LabelNational.FRANCE_TRAVAIL.value])
    structure_2 = structure_factory(
        labels_nationaux=[
            v0.LabelNational.MOBIN.value,
            v0.LabelNational.FRANCE_SERVICE.value,
        ]
    )

    url = f"/api/{schema_version}/structures/"
    response = api_client.get(
        url, params={"label_national": v0.LabelNational.FRANCE_SERVICE.value}
    )

    resp_data = response.json()
    assert_paginated_response_data(response.json(), total=1)
    assert resp_data["items"][0]["id"] == structure_2.id

    response = api_client.get(
        url, params={"label_national": v0.LabelNational.AFPA.value}
    )
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.parametrize("schema_version", ["v1"])
@pytest.mark.with_token
def test_list_structures_filter_by_reseaux_porteurs(
    api_client,
    schema_version,
    structure_factory,
):
    structure_1 = structure_factory(
        reseaux_porteurs=[v1.ReseauPorteur.FRANCE_TRAVAIL.value]
    )
    structure_factory(reseaux_porteurs=[v1.ReseauPorteur.MISSION_LOCALE.value])
    structure_factory(reseaux_porteurs=[])
    structure_factory(reseaux_porteurs=None)

    url = f"/api/{schema_version}/structures/"
    response = api_client.get(
        url, params={"reseaux_porteurs": v1.ReseauPorteur.FRANCE_TRAVAIL.value}
    )

    resp_data = response.json()
    assert_paginated_response_data(response.json(), total=1)
    assert resp_data["items"][0]["id"] == structure_1.id

    response = api_client.get(
        url, params={"reseaux_porteurs": v1.ReseauPorteur.AFPA.value}
    )
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.with_token
def test_list_sources(
    api_client,
    schema_version,
):
    url = f"/api/{schema_version}/sources/"
    response = api_client.get(url)

    resp_data = response.json()
    assert resp_data != []
    assert resp_data[0] == {"slug": ANY, "nom": ANY, "description": ANY}
    assert all(
        slug in [d["slug"] for d in resp_data]
        for slug in [
            "dora",
            "emplois-de-linclusion",
            "mediation-numerique",
        ]
    )


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
def test_list_services_unauthenticated(api_client, schema_version):
    url = f"/api/{schema_version}/services/"
    response = api_client.get(url)

    assert response.status_code == 403


@pytest.mark.parametrize(
    ("schema_version", "instances"),
    [
        (
            "v0",
            [
                models_v0.Structure(
                    _di_surrogate_id="04cfed4d-d3f7-4f9f-ad97-64bb6bbd7c96",
                    accessibilite="https://acceslibre.beta.gouv.fr/app/kitchen-amount/",
                    adresse="49, avenue de Pichon",
                    code_insee="59350",
                    code_postal="46873",
                    commune="Sainte CharlotteBourg",
                    complement_adresse=None,
                    courriel="levyalexandre@example.org",
                    date_maj="2023-01-01",
                    horaires_ouverture='Mo-Fr 10:00-20:00 "sur rendez-vous"; PH off',
                    id="lot-kitchen-amount",
                    labels_autres=["Nièvre médiation numérique"],
                    labels_nationaux=[],
                    latitude=-20.074628,
                    longitude=99.899603,
                    nom="Perrin",
                    presentation_detail="Or personne jambe.",
                    presentation_resume="Image voie battre.",
                    rna="W242194892",
                    score_qualite=0.3,
                    siret="76475938700658",
                    site_web="https://www.le.net/",
                    source="dora",
                    telephone="0102030405",
                    thematiques=["famille--garde-denfants"],
                    typologie="ACI",
                ),
                models_v0.Service(
                    _di_surrogate_id="eafdc798-18a6-446d-acda-7bc14ba83a39",
                    _di_structure_surrogate_id="04cfed4d-d3f7-4f9f-ad97-64bb6bbd7c96",
                    adresse="62, rue Eugène Rodrigues",
                    code_insee="59350",
                    code_postal="92950",
                    commune="Sainte Gabriel",
                    complement_adresse=None,
                    contact_nom_prenom="Thibaut de Michaud",
                    courriel="michelgerard@example.net",
                    date_maj="2023-01-01",
                    formulaire_en_ligne=None,
                    frais_autres="Camarade il.",
                    frais=["gratuit"],
                    id="be-water-scene-wind",
                    justificatifs=[],
                    latitude=-77.857573,
                    longitude=-62.54684,
                    modes_accueil=["a-distance"],
                    modes_orientation_accompagnateur_autres=None,
                    modes_orientation_accompagnateur=["telephoner"],
                    modes_orientation_beneficiaire_autres=None,
                    modes_orientation_beneficiaire=["telephoner"],
                    nom="Munoz",
                    page_web="http://aubert.net/",
                    pre_requis=[],
                    presentation_detail="Épaule élever un.",
                    presentation_resume="Puissant fine.",
                    prise_rdv="https://teixeira.fr/",
                    profils=["femmes"],
                    profils_precisions="Femme en situation d'insertion",
                    recurrence=None,
                    score_qualite=0.5,
                    source="dora",
                    structure_id="much-mention",
                    telephone="0102030405",
                    thematiques=["famille--garde-denfants"],
                    types=["formation"],
                    zone_diffusion_code=None,
                    zone_diffusion_nom=None,
                    zone_diffusion_type=None,
                    volume_horaire_hebdomadaire=1,
                    nombre_semaines=1,
                ),
            ],
        ),
        (
            "v1",
            [
                models_v1.Structure(
                    accessibilite_lieu="https://acceslibre.beta.gouv.fr/app/kitchen-amount/",
                    adresse="49, avenue de Pichon",
                    code_insee="59350",
                    code_postal="46873",
                    commune="Sainte CharlotteBourg",
                    complement_adresse=None,
                    courriel="levyalexandre@example.org",
                    date_maj="2023-01-01",
                    horaires_accueil="Mo-Fr 10:00-20:00",
                    id="dora--much-mention",
                    reseaux_porteurs=[],
                    latitude=-20.074628,
                    longitude=99.899603,
                    nom="Perrin",
                    description="""Lorem ipsum dolor sit amet, consectetur adipiscing
                    elit, sed do eiusmod tempor incididunt ut labore et dolore magna
                    aliqua.""",
                    score_qualite=0.3,
                    siret="76475938700658",
                    site_web="https://www.le.net/",
                    source="dora",
                    telephone="0102030405",
                ),
                models_v1.Service(
                    adresse="62, rue Eugène Rodrigues",
                    code_insee="59350",
                    code_postal="92950",
                    commune="Sainte Gabriel",
                    complement_adresse=None,
                    conditions_acces="Savoir lire et écrire",
                    contact_nom_prenom="Thibaut de Michaud",
                    courriel="michelgerard@example.net",
                    date_maj="2023-01-01",
                    frais_precisions="Camarade il.",
                    frais="gratuit",
                    horaires_accueil="Mo-Fr 10:00-20:00",
                    id="dora--be-water-scene-wind",
                    latitude=-77.857573,
                    longitude=-62.54684,
                    modes_accueil=["a-distance"],
                    modes_mobilisation=["telephoner"],
                    mobilisation_precisions=None,
                    mobilisable_par=["usagers"],
                    lien_mobilisation="https://teixeira.fr/",
                    nom="Munoz",
                    description="""Lorem ipsum dolor sit amet, consectetur adipiscing
                    elit, sed do eiusmod tempor incididunt ut labore et dolore magna
                    aliqua.""",
                    publics=["femmes"],
                    publics_precisions="Femme en situation d'insertion",
                    score_qualite=0.5,
                    source="dora",
                    structure_id="dora--much-mention",
                    telephone="0102030405",
                    thematiques=["famille--garde-denfants"],
                    type="formation",
                    zone_eligibilite=None,
                    volume_horaire_hebdomadaire=1,
                    nombre_semaines=1,
                ),
            ],
        ),
    ],
)
@pytest.mark.parametrize("path", ["/services"])
@pytest.mark.with_token
def test_list_services_all(api_client, db_session, url, snapshot, instances):
    db_session.add_all(instances)
    db_session.commit()

    response = api_client.get(url)
    assert (
        json.dumps(response.json(), indent=2, ensure_ascii=False, sort_keys=True)
        == snapshot
    )


@pytest.mark.parametrize("schema_version", ["v0"])
@pytest.mark.parametrize("path", ["/services", "/search/services"])
@pytest.mark.parametrize(
    ("profils_precisions", "input", "found"),
    [
        ("jeunes moins de 18 ans", "jeunes", True),
        ("jeune moins de 18 ans", "jeunes", True),
        ("jeunes et personne age", "vieux", False),
        ("jeunes et personne age", "personne OR vieux", True),
        ("jeunes et personne age", "personne jeune", True),
        ("jeunes et personne age", "jeunes -personne", False),
        ("jeunes et personne age", '"personne jeune"', False),
        ("jeunes et personne agee", "âgée", True),
    ],
)
@pytest.mark.with_token
def test_can_filter_resources_by_profils_precisions(
    api_client, profils_precisions, input, found, url, service_factory
):
    resource = service_factory(profils=None, profils_precisions=profils_precisions)
    service_factory(profils=None, profils_precisions="tests")

    response = api_client.get(url, params={"recherche_public": input})

    assert response.status_code == 200
    resp_data = response.json()
    if found:
        assert_paginated_response_data(resp_data, total=1)
        assert list_resources_data(resp_data)[0]["id"] in [resource.id]
    else:
        assert_paginated_response_data(resp_data, total=0)


@pytest.mark.parametrize("schema_version", ["v1"])
@pytest.mark.parametrize("path", ["/services", "/search/services"])
@pytest.mark.parametrize(
    ("publics_precisions", "input", "found"),
    [
        ("jeunes moins de 18 ans", "jeunes", True),
        ("jeune moins de 18 ans", "jeunes", True),
        ("jeunes et personne age", "vieux", False),
        ("jeunes et personne age", "personne OR vieux", True),
        ("jeunes et personne age", "personne jeune", True),
        ("jeunes et personne age", "jeunes -personne", False),
        ("jeunes et personne age", '"personne jeune"', False),
        ("jeunes et personne agee", "âgée", True),
    ],
)
@pytest.mark.with_token
def test_can_filter_resources_by_publics_precisions(
    api_client, publics_precisions, input, found, url, service_factory
):
    resource = service_factory(publics=None, publics_precisions=publics_precisions)
    service_factory(publics=None, publics_precisions="tests")

    response = api_client.get(url, params={"recherche_public": input})

    assert response.status_code == 200
    resp_data = response.json()
    if found:
        assert_paginated_response_data(resp_data, total=1)
        assert list_resources_data(resp_data)[0]["id"] in [resource.id]
    else:
        assert_paginated_response_data(resp_data, total=0)


@pytest.mark.parametrize("schema_version", ["v0"])
@pytest.mark.parametrize("path", ["/services", "/search/services"])
@pytest.mark.parametrize(
    "profils,input,found",
    [
        ([v0.Profil.FEMMES.value], "femme", True),
        ([v0.Profil.JEUNES_16_26.value], "jeune", True),
        ([v0.Profil.FEMMES.value], "jeune", False),
        (
            [v0.Profil.DEFICIENCE_VISUELLE.value],
            "deficience jeune difficulte",
            True,
        ),
    ],
)
@pytest.mark.with_token
def test_can_filter_resources_by_profils_precisions_with_only_profils_data_v0(
    api_client, profils, input, found, url
):
    resource = v0_factories.ServiceFactory(profils=profils, profils_precisions="")
    v0_factories.ServiceFactory(
        profils=[v0.Profil.ETUDIANTS.value], profils_precisions=""
    )

    response = api_client.get(url, params={"recherche_public": input})

    assert response.status_code == 200
    resp_data = response.json()
    if found:
        assert_paginated_response_data(resp_data, total=1)
        assert list_resources_data(resp_data)[0]["id"] in [resource.id]
    else:
        assert_paginated_response_data(resp_data, total=0)


@pytest.mark.parametrize("schema_version", ["v1"])
@pytest.mark.parametrize("path", ["/services", "/search/services"])
@pytest.mark.parametrize(
    "profils,input,found",
    [
        ([v1.Public.FEMMES.value], "femme", True),
        ([v1.Public.JEUNES.value], "jeune", True),
        ([v1.Public.FEMMES.value], "jeune", False),
        (
            [v1.Public.PERSONNES_EN_SITUATION_DE_HANDICAP.value],
            "handicap jeune difficulte",
            True,
        ),
    ],
)
@pytest.mark.with_token
def test_can_filter_resources_by_profils_precisions_with_only_profils_data_v1(
    api_client, profils, input, found, url
):
    resource = v1_factories.ServiceFactory(publics=profils, publics_precisions="")
    v1_factories.ServiceFactory(publics=[v1.Public.ETUDIANTS], publics_precisions="")

    response = api_client.get(url, params={"recherche_public": input})

    assert response.status_code == 200
    resp_data = response.json()
    if found:
        assert_paginated_response_data(resp_data, total=1)
        assert list_resources_data(resp_data)[0]["id"] in [resource.id]
    else:
        assert_paginated_response_data(resp_data, total=0)


@pytest.mark.parametrize(
    ("schema_version", "path"),
    [
        ("v0", "/services"),
        ("v0", "/search/services"),
        ("v0", "/structures"),
    ],
)
@pytest.mark.parametrize(
    "thematiques,input,found",
    [
        ([], [v0.Thematique.FAMILLE.value], False),
        ([v0.Thematique.FAMILLE.value], [v0.Thematique.FAMILLE.value], True),
        ([v0.Thematique.NUMERIQUE.value], [v0.Thematique.FAMILLE.value], False),
        (
            [v0.Thematique.NUMERIQUE.value, v0.Thematique.FAMILLE.value],
            [v0.Thematique.FAMILLE.value],
            True,
        ),
        (
            [v0.Thematique.SANTE.value, v0.Thematique.NUMERIQUE.value],
            [v0.Thematique.FAMILLE.value, v0.Thematique.NUMERIQUE.value],
            True,
        ),
        (
            [v0.Thematique.SANTE.value, v0.Thematique.NUMERIQUE.value],
            [v0.Thematique.FAMILLE.value, v0.Thematique.NUMERIQUE.value],
            True,
        ),
        (
            [v0.Thematique.FAMILLE__GARDE_DENFANTS.value],
            [v0.Thematique.FAMILLE.value],
            True,
        ),
    ],
)
@pytest.mark.with_token
def test_can_filter_resources_by_thematiques_v0(
    api_client, url, factory, thematiques, input, found
):
    # this checks that the fixture thematiques are in the current schema
    if any(t not in v0.Thematique for t in thematiques):
        raise ValueError("Invalid fixture param")

    resource = factory(thematiques=thematiques)
    factory(thematiques=[v0.Thematique.MOBILITE.value])

    response = api_client.get(url, params={"thematiques": input})

    assert response.status_code == 200
    resp_data = response.json()
    if found:
        assert_paginated_response_data(resp_data, total=1)
        assert list_resources_data(resp_data)[0]["id"] in [resource.id]
    else:
        assert_paginated_response_data(resp_data, total=0)


@pytest.mark.parametrize(
    ("schema_version", "path"),
    [
        ("v1", "/services"),
        ("v1", "/search/services"),
    ],
)
@pytest.mark.parametrize(
    "thematiques,input,found",
    [
        ([], [v1.Thematique.FAMILLE__GARDE_DENFANTS.value], False),
        (
            [v1.Thematique.FAMILLE__GARDE_DENFANTS.value],
            [v1.Thematique.FAMILLE__GARDE_DENFANTS.value],
            True,
        ),
        (
            [v1.Thematique.NUMERIQUE__ACQUERIR_UN_EQUIPEMENT.value],
            [v1.Thematique.FAMILLE__GARDE_DENFANTS.value],
            False,
        ),
        (
            [
                v1.Thematique.NUMERIQUE__ACQUERIR_UN_EQUIPEMENT.value,
                v1.Thematique.FAMILLE__GARDE_DENFANTS.value,
            ],
            [v1.Thematique.FAMILLE__GARDE_DENFANTS.value],
            True,
        ),
        (
            [
                v1.Thematique.SANTE__ACCES_AUX_SOINS.value,
                v1.Thematique.NUMERIQUE__ACQUERIR_UN_EQUIPEMENT.value,
            ],
            [
                v1.Thematique.FAMILLE__GARDE_DENFANTS.value,
                v1.Thematique.NUMERIQUE__ACQUERIR_UN_EQUIPEMENT.value,
            ],
            True,
        ),
        (
            [
                v1.Thematique.SANTE__ACCES_AUX_SOINS.value,
                v1.Thematique.NUMERIQUE__ACQUERIR_UN_EQUIPEMENT.value,
            ],
            [
                v1.Thematique.FAMILLE__GARDE_DENFANTS.value,
                v1.Thematique.NUMERIQUE__ACQUERIR_UN_EQUIPEMENT.value,
            ],
            True,
        ),
        (
            [v1.Thematique.FAMILLE__GARDE_DENFANTS.value],
            [v1.Categorie.FAMILLE.value],
            True,
        ),
    ],
)
@pytest.mark.with_token
def test_can_filter_resources_by_thematiques(
    api_client, url, factory, thematiques, input, found
):
    # this checks that the fixture thematiques are in the current schema
    if any(t not in v1.Thematique for t in thematiques):
        raise ValueError("Invalid fixture param")

    resource = factory(thematiques=thematiques)
    factory(thematiques=[v1.Thematique.MOBILITE__FINANCER_MA_MOBILITE.value])

    response = api_client.get(url, params={"thematiques": input})

    assert response.status_code == 200
    resp_data = response.json()
    if found:
        assert_paginated_response_data(resp_data, total=1)
        assert list_resources_data(resp_data)[0]["id"] in [resource.id]
    else:
        assert_paginated_response_data(resp_data, total=0)


@pytest.mark.with_token
@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/structures", "/services"])
def test_can_filter_resources_by_code_departement(api_client, url, factory):
    resource = factory(code_insee=PARIS["code_insee"])
    factory(code_insee=LILLE["code_insee"])
    factory(code_insee=None)

    response = api_client.get(url, params={"code_departement": "75"})

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["id"] == resource.id

    response = api_client.get(url, params={"code_departement": "62"})
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.with_token
@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/structures", "/services"])
def test_can_filter_resources_by_slug_departement(api_client, url, factory):
    resource = factory(code_insee=PARIS["code_insee"])
    factory(code_insee=LILLE["code_insee"])

    response = api_client.get(url, params={"slug_departement": "paris"})

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["id"] == resource.id

    response = api_client.get(url, params={"slug_departement": "pas-de-calais"})
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.with_token
@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/structures", "/services"])
def test_can_filter_resources_by_code_region(api_client, url, factory):
    resource = factory(code_insee=PARIS["code_insee"])
    factory(code_insee=LILLE["code_insee"])

    response = api_client.get(
        url, params={"code_region": RegionEnum.ILE_DE_FRANCE.value.code}
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["id"] == resource.id

    response = api_client.get(
        url, params={"code_region": RegionEnum.LA_REUNION.value.code}
    )
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.with_token
@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/structures", "/services"])
def test_can_filter_resources_by_slug_region(api_client, url, factory):
    resource = factory(code_insee=PARIS["code_insee"])
    factory(code_insee=LILLE["code_insee"])

    response = api_client.get(
        url, params={"slug_region": RegionEnum.ILE_DE_FRANCE.value.slug}
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["id"] == resource.id

    response = api_client.get(
        url, params={"slug_region": RegionEnum.LA_REUNION.value.slug}
    )
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.with_token
@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/structures", "/services"])
@pytest.mark.parametrize(
    "code_commune, input, found",
    [
        (None, DUNKERQUE["code_insee"], False),
        (DUNKERQUE["code_insee"], DUNKERQUE["code_insee"], True),
        (DUNKERQUE["code_insee"], "62041", False),
        (PARIS["code_insee"], "75056", True),
        (PARIS_11["code_insee"], "75111", True),
    ],
)
def test_can_filter_resources_by_code_commune(
    api_client, url, factory, code_commune, input, found
):
    resource = factory(code_insee=code_commune)
    factory(code_insee=LILLE["code_insee"])

    response = api_client.get(url, params={"code_commune": input})

    assert response.status_code == 200
    resp_data = response.json()
    if found:
        assert_paginated_response_data(resp_data, total=1)
        assert resp_data["items"][0]["id"] == resource.id
    else:
        assert_paginated_response_data(resp_data, total=0)


@pytest.mark.with_token
@pytest.mark.parametrize("schema_version", ["v0"])
@pytest.mark.parametrize("path", ["/services", "/search/services"])
def test_can_filter_services_by_profils(api_client, url, service_factory):
    service_1 = service_factory(profils=[v0.Profil.FEMMES.value])
    service_2 = service_factory(profils=[v0.Profil.JEUNES.value])
    service_factory(profils=[v0.Profil.PERSONNES_EN_SITUATION_DE_HANDICAP.value])
    service_factory(profils=[])
    service_factory(profils=None)

    response = api_client.get(
        url,
        params={
            "profils": [
                v0.Profil.FEMMES.value,
                v0.Profil.JEUNES.value,
            ],
        },
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=2)
    assert {d["id"] for d in list_resources_data(resp_data)} == {
        service_1.id,
        service_2.id,
    }

    response = api_client.get(
        url,
        params={
            "profils": v0.Profil.ETUDIANTS.value,
        },
    )
    assert_paginated_response_data(response.json(), total=0)


UNDEFINED = "UNDEFINED"


@pytest.mark.with_token
@pytest.mark.parametrize("schema_version", ["v1"])
@pytest.mark.parametrize("path", ["/services", "/search/services"])
@pytest.mark.parametrize(
    ("defined_publics", "requested_publics", "expected_total"),
    [
        ([v1.Public.FEMMES, v1.Public.SENIORS], [v1.Public.FEMMES], 1),
        # same, but use the second defined value
        ([v1.Public.FEMMES, v1.Public.SENIORS], [v1.Public.SENIORS], 1),
        # when requested values are overlapping, but not included,
        # they should match
        (
            [v1.Public.FEMMES, v1.Public.SENIORS],
            [v1.Public.FEMMES, v1.Public.JEUNES],
            1,
        ),
        # when `tous-publics` is defined, it should match any requested values
        ([v1.Public.TOUS_PUBLICS], [v1.Public.JEUNES], 1),
        # when `tous-publics` is requested, it should match any defined values
        ([v1.Public.JEUNES], [v1.Public.TOUS_PUBLICS], 1),
        # when there isn't a requested public, it should match all
        ([v1.Public.FEMMES], UNDEFINED, 1),
        ([v1.Public.FEMMES, v1.Public.SENIORS], [v1.Public.JEUNES], 0),
        ([], [v1.Public.FEMMES], 0),
        (None, [v1.Public.FEMMES], 0),
    ],
)
def test_can_filter_services_by_publics(
    api_client,
    url,
    service_factory,
    defined_publics,
    requested_publics,
    expected_total,
):
    service_factory(
        publics=[p.value for p in defined_publics]
        if isinstance(defined_publics, list)
        else defined_publics
    )

    if requested_publics == UNDEFINED:
        params = {}
    elif isinstance(requested_publics, list):
        params = {"publics": [p.value for p in requested_publics]}

    response = api_client.get(url, params=params)

    assert response.status_code == 200
    assert_paginated_response_data(response.json(), total=expected_total)


@pytest.mark.with_token
@pytest.mark.parametrize("schema_version", ["v0"])
@pytest.mark.parametrize("path", ["/services", "/search/services"])
def test_list_services_by_types_v0(api_client, url, service_factory):
    service_1 = service_factory(types=[v0.TypologieService.ACCUEIL.value])
    service_2 = service_factory(types=[v0.TypologieService.ACCOMPAGNEMENT.value])
    service_factory(types=[v0.TypologieService.AIDE_FINANCIERE.value])

    response = api_client.get(
        url,
        params={
            "types": [
                v0.TypologieService.ACCUEIL.value,
                v0.TypologieService.ACCOMPAGNEMENT.value,
            ],
        },
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=2)
    assert {d["id"] for d in list_resources_data(resp_data)} == {
        service_1.id,
        service_2.id,
    }

    response = api_client.get(
        url,
        params={
            "types": v0.TypologieService.ATELIER.value,
        },
    )
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.with_token
@pytest.mark.parametrize("schema_version", ["v1"])
@pytest.mark.parametrize("path", ["/services", "/search/services"])
def test_list_services_by_types(api_client, url, service_factory):
    service_1 = service_factory(type=v1.TypeService.INFORMATION.value)
    service_2 = service_factory(type=v1.TypeService.ACCOMPAGNEMENT.value)
    service_factory(type=v1.TypeService.AIDE_FINANCIERE.value)

    response = api_client.get(
        url,
        params={
            "types": [
                v1.TypeService.INFORMATION.value,
                v1.TypeService.ACCOMPAGNEMENT.value,
            ],
        },
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=2)
    assert {d["id"] for d in list_resources_data(resp_data)} == {
        service_1.id,
        service_2.id,
    }

    response = api_client.get(
        url,
        params={
            "types": v1.TypeService.ATELIER.value,
        },
    )
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.with_token
@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/services", "/search/services"])
def test_list_services_by_score_qualite(api_client, url, service_factory):
    service_1 = service_factory(score_qualite=0.5)
    service_2 = service_factory(score_qualite=0.7)
    service_factory(score_qualite=0.2)

    response = api_client.get(
        url,
        params={"score_qualite_minimum": service_1.score_qualite},
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=2)
    assert {d["id"] for d in list_resources_data(resp_data)} == {
        service_1.id,
        service_2.id,
    }


@pytest.mark.with_token
@pytest.mark.parametrize("schema_version", ["v0"])
@pytest.mark.parametrize("path", ["/services", "/search/services"])
def test_can_filter_services_by_frais_v0(api_client, url):
    service_1 = v0_factories.ServiceFactory(frais=[v0.Frais.GRATUIT.value])
    service_2 = v0_factories.ServiceFactory(frais=[v0.Frais.ADHESION.value])
    v0_factories.ServiceFactory(frais=[v0.Frais.PASS_NUMERIQUE.value])

    response = api_client.get(
        url,
        params={
            "frais": [
                v0.Frais.GRATUIT.value,
                v0.Frais.ADHESION.value,
            ],
        },
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=2)
    assert {d["id"] for d in list_resources_data(resp_data)} == {
        service_1.id,
        service_2.id,
    }

    response = api_client.get(
        url,
        params={
            "frais": v0.Frais.PAYANT.value,
        },
    )
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.with_token
@pytest.mark.parametrize("schema_version", ["v1"])
@pytest.mark.parametrize("path", ["/services", "/search/services"])
def test_can_filter_services_by_frais_v1(api_client, url):
    service_1 = v1_factories.ServiceFactory(frais=v1.Frais.GRATUIT.value)
    v1_factories.ServiceFactory(frais=v1.Frais.PAYANT.value)

    response = api_client.get(
        url,
        params={
            "frais": [
                v0.Frais.GRATUIT.value,
            ],
        },
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert {d["id"] for d in list_resources_data(resp_data)} == {
        service_1.id,
    }


@pytest.mark.with_token
@pytest.mark.parametrize("schema_version", ["v0"])
@pytest.mark.parametrize("path", ["/services", "/search/services"])
def test_can_provide_deprecated_inclure_suspendus_query_param(api_client, url):
    response = api_client.get(url)
    assert response.status_code == 200


@pytest.mark.with_token
@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/services", "/search/services"])
def test_can_filter_services_by_modes_accueil(api_client, url, service_factory):
    service_1 = service_factory(modes_accueil=[v0.ModeAccueil.EN_PRESENTIEL.value])
    service_factory(modes_accueil=[v0.ModeAccueil.A_DISTANCE.value])
    service_factory(modes_accueil=[])
    service_factory(modes_accueil=None)

    response = api_client.get(
        url,
        params={
            "modes_accueil": [
                v0.ModeAccueil.EN_PRESENTIEL.value,
            ],
        },
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)

    assert list_resources_data(resp_data)[0]["id"] == service_1.id


@pytest.mark.with_token
@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/services", "/search/services", "/structures"])
def test_can_filter_resources_by_sources(api_client, url, factory):
    service_1 = factory(source="dora")
    service_2 = factory(source="emplois-de-linclusion")
    factory(source="un-jeune-une-solution")

    response = api_client.get(
        url,
        params={
            "sources": ["dora", "emplois-de-linclusion"],
        },
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=2)
    assert {d["id"] for d in list_resources_data(resp_data)} == {
        service_1.id,
        service_2.id,
    }

    response = api_client.get(
        url,
        params={
            "sources": ["foobar"],
        },
    )
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize(
    ("is_dora",),
    [
        pytest.param(True, marks=pytest.mark.with_token("dora-test")),
        pytest.param(False, marks=pytest.mark.with_token("not-dora")),
    ],
)
@pytest.mark.parametrize("path", ["/services", "/search/services", "/structures"])
@pytest.mark.parametrize(
    ("source", "code_insee", "restricted"),
    [
        ("soliguide", PARIS["code_insee"], True),
        ("soliguide", None, True),
        ("emplois-de-linclusion", PARIS["code_insee"], False),
        ("emplois-de-linclusion", None, False),
        # 59 and 67 wew part of the 2025 soliguide experiment which has ended
        ("soliguide", LILLE["code_insee"], True),
        ("soliguide", STRASBOURG["code_insee"], True),
    ],
)
def test_soliguide_is_partially_available(
    api_client, is_dora, url, factory, source, code_insee, restricted
):
    factory(source=source, code_insee=code_insee)

    response = api_client.get(url)

    assert response.status_code == 200
    resp_data = response.json()

    assert (restricted and not is_dora) == (len(resp_data["items"]) == 0)


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/search/services"])
@pytest.mark.parametrize(
    "commune_data, input, found",
    [
        (None, DUNKERQUE["code_insee"], False),
        (DUNKERQUE, DUNKERQUE["code_insee"], True),
        (DUNKERQUE, MAUBEUGE["code_insee"], False),
        (PARIS, "75056", True),
        (PARIS_11, "75111", True),
    ],
)
@pytest.mark.with_token
def test_search_services_with_code_commune(
    api_client, commune_data, input, found, url, service_factory
):
    service = service_factory(
        modes_accueil=[v0.ModeAccueil.EN_PRESENTIEL.value],
        **(commune_data if commune_data is not None else {}),
    )

    response = api_client.get(url, params={"code_commune": input})

    assert response.status_code == 200
    resp_data = response.json()
    if found:
        assert_paginated_response_data(resp_data, total=1)
        assert resp_data["items"][0]["service"]["id"] == service.id
    else:
        assert_paginated_response_data(resp_data, total=0)


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/search/services"])
@pytest.mark.with_token
def test_search_services_with_code_commune_too_far(api_client, url, service_factory):
    # Dunkerque to Hazebrouck: <50km
    # Hazebrouck to Lille: <50km
    # Dunkerque to Lille: >50km
    service_1 = service_factory(
        commune="Lille",
        **LILLE,
        modes_accueil=[v0.ModeAccueil.EN_PRESENTIEL.value],
    )
    service_2 = service_factory(
        commune="Dunkerque",
        **DUNKERQUE,
        modes_accueil=[v0.ModeAccueil.EN_PRESENTIEL.value],
    )
    service_factory(
        code_insee=None,
        latitude=None,
        longitude=None,
        modes_accueil=[v0.ModeAccueil.EN_PRESENTIEL.value],
    )

    response = api_client.get(
        url,
        params={
            "code_commune": ROUBAIX["code_insee"],  # Roubaix (only close to Lille)
        },
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["service"]["id"] == service_1.id
    assert 0 < resp_data["items"][0]["distance"] < 50

    # Do the same but with lat/lon in addition to the code INSEE
    response = api_client.get(
        url,
        params={
            "code_commune": ROUBAIX["code_insee"],  # Roubaix
            # Coordinates for Le Mans. We don't enforce lat/lon to be within
            # the supplied 'code_commune' city limits.
            "lat": 48.003954,
            "lon": 0.199134,
        },
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=0)

    # This time with coordinates close to the services we know
    response = api_client.get(
        url,
        params={
            "code_commune": ROUBAIX["code_insee"],  # Roubaix
            # Coordinates for Hazebrouck, between Dunkirk & Lille
            "lat": 50.7262,
            "lon": 2.5387,
        },
    )

    assert response.status_code == 200
    resp_data = response.json()
    # Lille and Hazebrouck are less than 50km apart.
    assert_paginated_response_data(resp_data, total=2)
    assert resp_data["items"][0]["service"]["id"] == service_2.id
    assert 0 < resp_data["items"][0]["distance"] < 50

    # What about a request without code_commune but with lat/lon?
    response = api_client.get(
        url,
        params={
            # Coordinates for Le Mans, should be ignored.
            # the supplied 'code_commune' city limits.
            "lat": 48.003954,
            "lon": 0.199134,
        },
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=3)

    # Error cases
    response = api_client.get(
        url,
        params={
            "code_commune": MAUBEUGE["code_insee"],
            "lat": 48.003954,
        },
    )

    assert response.status_code == 422
    resp_data = response.json()
    assert resp_data["detail"] == "The `lat` and `lon` must be simultaneously filled."

    response = api_client.get(
        url,
        params={
            "code_commune": MAUBEUGE["code_insee"],
            "lon": 1.2563,
        },
    )

    assert response.status_code == 422
    resp_data = response.json()
    assert resp_data["detail"] == "The `lat` and `lon` must be simultaneously filled."


@pytest.mark.parametrize("schema_version", ["v0"])
@pytest.mark.parametrize("path", ["/search/services"])
@pytest.mark.with_token
def test_search_services_with_zone_diffusion_pays_v0(api_client, url, service_factory):
    service_1 = service_factory(
        commune="Dunkerque",
        code_insee=DUNKERQUE["code_insee"],
        latitude=51.034368,
        longitude=2.376776,
        modes_accueil=[v0.ModeAccueil.A_DISTANCE.value],
        zone_diffusion_type=v0.ZoneDiffusionType.PAYS.value,
        zone_diffusion_code=None,
        zone_diffusion_nom=None,
    )

    response = api_client.get(
        url,
        params={
            "code_commune": MAUBEUGE["code_insee"],  # Maubeuge
        },
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["service"]["id"] == service_1.id


@pytest.mark.parametrize("schema_version", ["v1"])
@pytest.mark.parametrize("path", ["/search/services"])
@pytest.mark.parametrize(
    "zone_eligibilite",
    [constants.PaysEnum.FRANCE.value.code, constants.PaysEnum.FRANCE.value.slug],
)
@pytest.mark.with_token
def test_search_services_with_zone_diffusion_pays(
    api_client, url, service_factory, zone_eligibilite
):
    service_1 = service_factory(
        commune="Dunkerque",
        code_insee=DUNKERQUE["code_insee"],
        latitude=51.034368,
        longitude=2.376776,
        modes_accueil=[v0.ModeAccueil.A_DISTANCE.value],
        zone_eligibilite=[zone_eligibilite],
    )

    response = api_client.get(
        url,
        params={
            "code_commune": MAUBEUGE["code_insee"],  # Maubeuge
        },
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["service"]["id"] == service_1.id


@pytest.mark.parametrize("schema_version", ["v0"])
@pytest.mark.parametrize("path", ["/search/services"])
@pytest.mark.with_token
def test_search_services_with_zone_diffusion_commune_v0(
    api_client, url, service_factory
):
    service_1 = service_factory(
        commune="Dunkerque",
        code_insee=DUNKERQUE["code_insee"],
        latitude=51.034368,
        longitude=2.376776,
        modes_accueil=[v0.ModeAccueil.EN_PRESENTIEL.value],
        zone_diffusion_type=v0.ZoneDiffusionType.COMMUNE.value,
        zone_diffusion_code=DUNKERQUE["code_insee"],
        zone_diffusion_nom="Dunkerque",
    )
    service_factory(
        commune="Lille",
        code_insee=LILLE["code_insee"],
        latitude=50.633333,
        longitude=3.066667,
        modes_accueil=[v0.ModeAccueil.EN_PRESENTIEL.value],
        zone_diffusion_type=v0.ZoneDiffusionType.COMMUNE.value,
        zone_diffusion_code=LILLE["code_insee"],
        zone_diffusion_nom="Lille",
    )

    response = api_client.get(
        url,
        params={
            "code_commune": DUNKERQUE["code_insee"],  # Dunkerque
        },
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["service"]["id"] == service_1.id


@pytest.mark.parametrize("schema_version", ["v1"])
@pytest.mark.parametrize("path", ["/search/services"])
@pytest.mark.with_token
def test_search_services_with_zone_diffusion_commune(api_client, url, service_factory):
    service_1 = service_factory(
        commune="Dunkerque",
        code_insee=DUNKERQUE["code_insee"],
        latitude=51.034368,
        longitude=2.376776,
        modes_accueil=[v0.ModeAccueil.EN_PRESENTIEL.value],
        zone_eligibilite=[DUNKERQUE["code_insee"]],
    )

    service_factory(
        commune="Lille",
        code_insee=LILLE["code_insee"],
        latitude=50.633333,
        longitude=3.066667,
        modes_accueil=[v0.ModeAccueil.EN_PRESENTIEL.value],
        zone_eligibilite=[LILLE["code_insee"]],
    )

    response = api_client.get(
        url,
        params={
            "code_commune": DUNKERQUE["code_insee"],  # Dunkerque
        },
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["service"]["id"] == service_1.id


@pytest.mark.parametrize("schema_version", ["v0"])
@pytest.mark.parametrize("path", ["/search/services"])
@pytest.mark.with_token
def test_search_services_with_zone_diffusion_epci_v0(api_client, url, service_factory):
    service_1 = service_factory(
        commune="Dunkerque",
        code_insee=DUNKERQUE["code_insee"],
        latitude=51.034368,
        longitude=2.376776,
        modes_accueil=[v0.ModeAccueil.EN_PRESENTIEL.value],
        zone_diffusion_type=v0.ZoneDiffusionType.EPCI.value,
        zone_diffusion_code="245900428",
        zone_diffusion_nom="CU de Dunkerque",
    )
    service_factory(
        commune="Lille",
        code_insee=LILLE["code_insee"],
        latitude=50.633333,
        longitude=3.066667,
        modes_accueil=[v0.ModeAccueil.EN_PRESENTIEL.value],
        zone_diffusion_type=v0.ZoneDiffusionType.EPCI.value,
        zone_diffusion_code="200093201",
        zone_diffusion_nom="Métropole Européenne de Lille",
    )

    response = api_client.get(
        url,
        params={
            "code_commune": DUNKERQUE["code_insee"],  # Dunkerque
        },
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["service"]["id"] == service_1.id


@pytest.mark.parametrize("schema_version", ["v1"])
@pytest.mark.parametrize("path", ["/search/services"])
@pytest.mark.with_token
def test_search_services_with_zone_diffusion_epci(api_client, url, service_factory):
    service_1 = service_factory(
        commune="Dunkerque",
        code_insee=DUNKERQUE["code_insee"],
        latitude=51.034368,
        longitude=2.376776,
        modes_accueil=[v0.ModeAccueil.EN_PRESENTIEL.value],
        zone_eligibilite=["245900428"],  # CU de Dunkerque
    )
    service_factory(
        commune="Lille",
        code_insee=LILLE["code_insee"],
        latitude=50.633333,
        longitude=3.066667,
        modes_accueil=[v0.ModeAccueil.EN_PRESENTIEL.value],
        zone_eligibilite=["200093201"],  # Métropole Européenne de Lille
    )

    response = api_client.get(
        url,
        params={
            "code_commune": DUNKERQUE["code_insee"],  # Dunkerque
        },
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["service"]["id"] == service_1.id


@pytest.mark.parametrize("schema_version", ["v0"])
@pytest.mark.parametrize("path", ["/search/services"])
@pytest.mark.with_token
def test_search_services_with_zone_diffusion_departement_v0(
    api_client, url, service_factory
):
    service_1 = service_factory(
        commune="Dunkerque",
        code_insee=DUNKERQUE["code_insee"],
        latitude=51.034368,
        longitude=2.376776,
        modes_accueil=[v0.ModeAccueil.EN_PRESENTIEL.value],
        zone_diffusion_type=v0.ZoneDiffusionType.DEPARTEMENT.value,
        zone_diffusion_code="59",
        zone_diffusion_nom="Nord",
    )
    service_factory(
        commune="Lille",
        code_insee=LILLE["code_insee"],
        latitude=50.633333,
        longitude=3.066667,
        modes_accueil=[v0.ModeAccueil.EN_PRESENTIEL.value],
        zone_diffusion_type=v0.ZoneDiffusionType.DEPARTEMENT.value,
        zone_diffusion_code="62",
        zone_diffusion_nom="Pas-de-Calais",
    )

    response = api_client.get(
        url,
        params={
            "code_commune": DUNKERQUE["code_insee"],  # Dunkerque
        },
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["service"]["id"] == service_1.id


@pytest.mark.parametrize("schema_version", ["v1"])
@pytest.mark.parametrize("path", ["/search/services"])
@pytest.mark.with_token
def test_search_services_with_zone_diffusion_departement(
    api_client, url, service_factory
):
    service_1 = service_factory(
        commune="Dunkerque",
        code_insee=DUNKERQUE["code_insee"],
        latitude=51.034368,
        longitude=2.376776,
        modes_accueil=[v0.ModeAccueil.EN_PRESENTIEL.value],
        zone_eligibilite=["59"],  # Nord
    )
    service_factory(
        commune="Lille",
        code_insee=LILLE["code_insee"],
        latitude=50.633333,
        longitude=3.066667,
        modes_accueil=[v0.ModeAccueil.EN_PRESENTIEL.value],
        zone_eligibilite=["62"],  # Pas-de-Calais
    )

    response = api_client.get(
        url,
        params={
            "code_commune": DUNKERQUE["code_insee"],  # Dunkerque
        },
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["service"]["id"] == service_1.id


@pytest.mark.parametrize("schema_version", ["v0"])
@pytest.mark.parametrize("path", ["/search/services"])
@pytest.mark.with_token
def test_search_services_with_zone_diffusion_region_v0(
    api_client, url, service_factory
):
    service_1 = service_factory(
        commune="Dunkerque",
        code_insee=DUNKERQUE["code_insee"],
        latitude=51.034368,
        longitude=2.376776,
        modes_accueil=[v0.ModeAccueil.EN_PRESENTIEL.value],
        zone_diffusion_type=v0.ZoneDiffusionType.REGION.value,
        zone_diffusion_code="32",
        zone_diffusion_nom="Nord",
    )
    service_factory(
        commune="Maubeuge",
        code_insee=MAUBEUGE["code_insee"],
        latitude=50.277500,
        longitude=3.973400,
        modes_accueil=[v0.ModeAccueil.EN_PRESENTIEL.value],
        zone_diffusion_type=v0.ZoneDiffusionType.REGION.value,
        zone_diffusion_code="44",
        zone_diffusion_nom="Grand Est",
    )

    response = api_client.get(
        url,
        params={
            "code_commune": DUNKERQUE["code_insee"],  # Dunkerque
        },
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["service"]["id"] == service_1.id


@pytest.mark.parametrize("schema_version", ["v1"])
@pytest.mark.parametrize("path", ["/search/services"])
@pytest.mark.with_token
def test_search_services_with_zone_diffusion_region(api_client, url, service_factory):
    service_1 = service_factory(
        commune="Dunkerque",
        code_insee=DUNKERQUE["code_insee"],
        latitude=51.034368,
        longitude=2.376776,
        modes_accueil=[v0.ModeAccueil.EN_PRESENTIEL.value],
        zone_eligibilite=["59"],  # Nord
    )
    service_factory(
        commune="Maubeuge",
        code_insee=MAUBEUGE["code_insee"],
        latitude=50.277500,
        longitude=3.973400,
        modes_accueil=[v0.ModeAccueil.EN_PRESENTIEL.value],
        zone_eligibilite=["44"],  # Grand Est
    )

    response = api_client.get(
        url,
        params={
            "code_commune": DUNKERQUE["code_insee"],  # Dunkerque
        },
    )

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["service"]["id"] == service_1.id


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/search/services"])
@pytest.mark.with_token
def test_search_services_with_bad_code_commune(api_client, url, service_factory):
    service_factory(
        commune="Lille",
        code_insee=LILLE["code_insee"],
        latitude=50.633333,
        longitude=3.066667,
        modes_accueil=[v0.ModeAccueil.A_DISTANCE.value],
    )

    response = api_client.get(
        url,
        params={
            "code_commune": "59999",  # Does not exist
        },
    )

    assert response.status_code == 422


@pytest.mark.parametrize("schema_version", ["v1"])
@pytest.mark.parametrize("path", ["/search/services"])
@pytest.mark.with_token
def test_search_services_with_commune_without_epci(
    api_client, url, service_factory, db_session
):
    commune_code = "97801"
    commune_without_epci = Commune(
        code=commune_code,
        nom="Cayenne",
        departement="973",
        region="03",
        siren_epci=None,
        codes_postaux=["97300"],
        centre="POINT (-52.3333 4.9333)",
    )
    db_session.add(commune_without_epci)
    db_session.commit()

    service_factory(
        commune="Cayenne",
        code_insee=commune_code,
        latitude=4.9333,
        longitude=-52.3333,
        modes_accueil=[v1.ModeAccueil.EN_PRESENTIEL.value],
    )

    response = api_client.get(url, params={"code_commune": commune_code})

    assert response.status_code == 200
    resp_data = response.json()
    assert resp_data["total"] >= 0


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/search/services"])
@pytest.mark.with_token
def test_search_services_with_code_commune_ordering(api_client, url, service_factory):
    service_1 = service_factory(
        commune="Hazebrouck",
        **HAZEBROUCK,
        modes_accueil=[v0.ModeAccueil.EN_PRESENTIEL.value],
    )
    service_2 = service_factory(
        commune="Lille",
        **LILLE,
        modes_accueil=[v0.ModeAccueil.EN_PRESENTIEL.value],
    )
    service_factory(
        code_insee=None,
        modes_accueil=[v0.ModeAccueil.EN_PRESENTIEL.value],
    )

    response = api_client.get(url, params={"code_commune": ROUBAIX["code_insee"]})

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=2)
    assert resp_data["items"][0]["service"]["id"] == service_2.id
    assert 0 < resp_data["items"][0]["distance"] < 50
    assert resp_data["items"][1]["service"]["id"] == service_1.id
    assert resp_data["items"][1]["distance"] < 50


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/search/services"])
@pytest.mark.with_token
def test_search_services_with_code_commune_sample_distance(
    api_client, url, service_factory
):
    service_1 = service_factory(
        commune="Lille",
        **LILLE,
        modes_accueil=[v0.ModeAccueil.EN_PRESENTIEL.value],
    )
    service_factory(
        code_insee=None,
        modes_accueil=[v0.ModeAccueil.EN_PRESENTIEL.value],
    )

    response = api_client.get(url, params={"code_commune": HAZEBROUCK["code_insee"]})

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=1)
    assert resp_data["items"][0]["service"]["id"] == service_1.id
    assert resp_data["items"][0]["distance"] == 39


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/search/services"])
@pytest.mark.with_token
def test_search_services_with_code_commune_a_distance(api_client, url, service_factory):
    service_1 = service_factory(
        commune="Dunkerque",
        code_insee=DUNKERQUE["code_insee"],
        modes_accueil=[v0.ModeAccueil.A_DISTANCE.value],
    )
    service_2 = service_factory(
        commune="Maubeuge",
        code_insee=MAUBEUGE["code_insee"],
        modes_accueil=[v0.ModeAccueil.A_DISTANCE.value],
    )

    response = api_client.get(url, params={"code_commune": DUNKERQUE["code_insee"]})

    assert response.status_code == 200
    resp_data = response.json()
    assert_paginated_response_data(resp_data, total=2)
    assert sorted([d["service"]["id"] for d in resp_data["items"]]) == sorted(
        [
            service_1.id,
            service_2.id,
        ]
    )
    assert resp_data["items"][0]["distance"] is None
    assert resp_data["items"][1]["distance"] is None


@pytest.mark.parametrize("schema_version", ["v0"])
@pytest.mark.parametrize("path", ["/services"])
@pytest.mark.with_token
def test_retrieve_service_v0(api_client, url, service_factory):
    service_1 = service_factory(source="foo", id="1")
    service_2 = service_factory(source="bar", id="1")
    service_3 = service_factory(source="foo", id="2")

    response = api_client.get(url + f"/{service_1.source}/{service_1.id}")

    assert response.status_code == 200
    resp_data = response.json()
    assert resp_data["id"] == service_1.id
    assert "structure" in resp_data
    assert resp_data["structure"]["id"] == service_1.structure.id

    response = api_client.get(url + f"{service_2.source}/{service_3.id}")
    assert response.status_code == 404


@pytest.mark.parametrize("schema_version", ["v1"])
@pytest.mark.parametrize("path", ["/services"])
@pytest.mark.with_token
def test_retrieve_service_v1(api_client, url, service_factory):
    service_1 = service_factory(source="foo", id="foo--1")
    # ensure there is another service to not trivially select "the latest"
    service_factory(source="bar", id="bar--1")

    response = api_client.get(url + f"/{service_1.id}")

    assert response.status_code == 200
    resp_data = response.json()
    assert resp_data["id"] == service_1.id
    assert "structure" in resp_data
    assert resp_data["structure"]["id"] == service_1.structure.id

    response = api_client.get(url + "bar--42")
    assert response.status_code == 404


@pytest.mark.parametrize("schema_version", ["v0"])
@pytest.mark.parametrize("path", ["/structures"])
@pytest.mark.with_token
def test_retrieve_structure_v0(api_client, url, structure_factory, service_factory):
    structure_1 = structure_factory(
        source="foo", id="1", cluster_id="1", is_best_duplicate=True
    )
    service_1 = service_factory(structure=structure_1)
    structure_2 = structure_factory(
        source="bar", id="1", cluster_id="1", is_best_duplicate=False
    )
    service_factory(structure=structure_2)
    structure_3 = structure_factory(source="foo", id="2")

    response = api_client.get(url + f"/{structure_1.source}/{structure_1.id}")

    assert response.status_code == 200
    resp_data = response.json()
    assert resp_data["id"] == structure_1.id
    assert "services" in resp_data
    assert len(resp_data["services"]) == 1
    assert resp_data["services"][0]["id"] == service_1.id
    assert "doublons" in resp_data
    assert len(resp_data["doublons"]) == 1
    assert resp_data["doublons"][0]["id"] == structure_2.id

    response = api_client.get(url + f"{structure_2.source}/{structure_3.id}")
    assert response.status_code == 404


@pytest.mark.parametrize("schema_version", ["v1"])
@pytest.mark.parametrize("path", ["/structures"])
@pytest.mark.with_token
def test_retrieve_structure_v1(api_client, url, structure_factory, service_factory):
    structure_1 = structure_factory(
        source="foo", id="foo--1", cluster_id="1", is_best_duplicate=True
    )
    service_1 = service_factory(structure=structure_1)
    structure_2 = structure_factory(
        source="bar", id="bar--1", cluster_id="1", is_best_duplicate=False
    )
    service_factory(structure=structure_2)

    response = api_client.get(url + f"/{structure_1.id}")

    assert response.status_code == 200
    resp_data = response.json()
    assert resp_data["id"] == structure_1.id
    assert "services" in resp_data
    assert len(resp_data["services"]) == 1
    assert resp_data["services"][0]["id"] == service_1.id
    assert "doublons" in resp_data
    assert len(resp_data["doublons"]) == 1
    assert resp_data["doublons"][0]["id"] == structure_2.id

    response = api_client.get(url + "/foo--42")
    assert response.status_code == 404


class FakeSoliguideClient(soliguide.SoliguideAPIClient):
    def __init__(self):
        self.retrieved_ids = []

    def retrieve_place(self, place_id: str):
        self.retrieved_ids.append(place_id)


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/structures"])
@pytest.mark.with_token
def test_retrieve_structure_and_notify_soliguide(
    schema_version, api_client, app, url, structure_factory
):
    structure_1 = structure_factory(
        source="soliguide",
        id="place-id" if schema_version == "v0" else "soliguide--place-id",
    )

    fake_soliguide_client = FakeSoliguideClient()
    app.dependency_overrides[soliguide.SoliguideAPIClient] = (
        lambda: fake_soliguide_client
    )

    if "v0" in url:
        response = api_client.get(url + f"/{structure_1.source}/{structure_1.id}")
    else:
        response = api_client.get(url + f"/{structure_1.id}")

    assert response.status_code == 200
    assert fake_soliguide_client.retrieved_ids == ["place-id"]


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/services"])
@pytest.mark.parametrize(
    ("requested_id", "status_code", "retrieved_ids"),
    [
        ("soliguide--service-id", 200, ["place-id"]),
        ("soliguide--42", 404, []),
    ],
)
@pytest.mark.with_token
def test_retrieve_service_and_notify_soliguide(
    schema_version,
    api_client,
    app,
    url,
    requested_id,
    status_code,
    retrieved_ids,
    service_factory,
):
    service_id = "soliguide--service-id"
    structure_id = "soliguide--place-id"

    if schema_version == "v0":
        # convert to v0 ids
        service_id = service_id.split("--", 1)[1]
        structure_id = structure_id.split("--", 1)[1]
        requested_id = requested_id.split("--", 1)[1]

    service_1 = service_factory(
        source="soliguide",
        id=service_id,
        structure__source="soliguide",
        structure__id=structure_id,
    )

    fake_soliguide_client = FakeSoliguideClient()
    app.dependency_overrides[soliguide.SoliguideAPIClient] = (
        lambda: fake_soliguide_client
    )

    path = f"/{service_1.source}/{requested_id}" if "v0" in url else f"/{requested_id}"
    response = api_client.get(url + path)

    assert response.status_code == status_code
    assert fake_soliguide_client.retrieved_ids == retrieved_ids


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/structures"])
@pytest.mark.parametrize(
    ("exclure_doublons", "expected_ids"),
    [
        (False, {"src_1--first", "src_2--second", "src_2--third", "src_3--fourth"}),
        (True, {"src_2--second", "src_3--fourth"}),
    ],
)
@pytest.mark.with_token
def test_list_structures_deduplicate_flag(
    api_client, url, exclure_doublons, expected_ids, structure_factory
):
    structure_factory(
        source="src_1",
        id="src_1--first",
        cluster_id="cluster_1",
        is_best_duplicate=False,
        score_qualite=0.5,
    )
    structure_factory(
        source="src_2",
        id="src_2--second",
        cluster_id="cluster_1",
        is_best_duplicate=True,
        score_qualite=0.9,
    )
    structure_factory(
        source="src_2",
        id="src_2--third",
        cluster_id="cluster_1",
        is_best_duplicate=False,
        score_qualite=0.3,
    )
    structure_factory(
        source="src_3",
        id="src_3--fourth",
        score_qualite=0.8,
        cluster_id=None,
        is_best_duplicate=None,
    )

    response = api_client.get(url, params={"exclure_doublons": exclure_doublons})

    assert_paginated_response_data(response.json(), total=len(expected_ids))
    assert {d["id"] for d in response.json()["items"]} == expected_ids


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/structures"])
@pytest.mark.with_token
def test_list_structures_deduplicate_flag_equal(api_client, url, structure_factory):
    # all these structiures have no services so we assume they have a score of 0
    structure_factory(
        date_maj="2020-01-01",
        source="src_1",
        id="src_1--first",
        cluster_id="cluster_1",
        is_best_duplicate=False,
    )
    structure_factory(
        date_maj="2024-01-01",  # the latest update
        source="src_2",
        id="src_2--second",
        cluster_id="cluster_1",
        is_best_duplicate=True,
    )
    structure_factory(
        date_maj="2008-01-01",
        source="src_3",
        id="src_3--third",
        cluster_id="cluster_1",
        is_best_duplicate=False,
    )
    structure_factory(
        source="src_3",
        id="src_3--fourth",
        cluster_id=None,
        is_best_duplicate=None,
    )

    response = api_client.get(url, params={"exclure_doublons": True})

    assert_paginated_response_data(response.json(), total=2)
    assert {d["id"] for d in response.json()["items"]} == {
        "src_2--second",
        "src_3--fourth",
    }


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/search/services"])
@pytest.mark.with_token
def test_search_services_deduplicate_flag(
    api_client, url, structure_factory, service_factory
):
    s1 = structure_factory(
        date_maj="2020-01-01",
        source="src_1",
        id="src_1--first",
        cluster_id="cluster_1",
        is_best_duplicate=False,
    )
    s2 = structure_factory(
        date_maj="2024-01-01",  # the latest update
        source="src_2",
        id="src_2--second",
        cluster_id="cluster_1",
        is_best_duplicate=True,
    )
    s3 = structure_factory(
        date_maj="2008-01-01",
        source="src_3",
        id="src_3--third",
        cluster_id="cluster_1",
        is_best_duplicate=False,
    )
    s4 = structure_factory(
        source="src_3",
        id="src_3--fourth",
        cluster_id=None,
        is_best_duplicate=None,
    )

    service_factory(
        source="src_1",
        id="src_1--first",
        structure=s1,
    )
    service_factory(
        source="src_2",
        id="src_2--second",
        structure=s2,
    )
    service_factory(
        source="src_2",
        id="src_2--third",
        structure=s3,
    )
    service_factory(
        source="src_3",
        id="src_3--fourth",
        structure=s4,
    )

    response = api_client.get(url, params={"exclure_doublons": True})

    assert_paginated_response_data(response.json(), total=2)
    assert {d["service"]["id"] for d in response.json()["items"]} == {
        "src_2--second",
        "src_3--fourth",
    }


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/structures"])
@pytest.mark.with_token
def test_list_structures_deduplicate(api_client, url, structure_factory):
    best_in_cluster_1_for_src_1 = structure_factory(
        source="src_1",
        id="src_1--best",
        cluster_id="cluster_1",
        is_best_duplicate=False,
        score_qualite=0.5,
    )
    structure_factory(
        source="src_1",
        id="src_1--lower",
        cluster_id="cluster_1",
        is_best_duplicate=False,
        score_qualite=0.3,
    )
    best_in_cluster_1_global = structure_factory(
        source="src_2",
        id="src_2--best",
        cluster_id="cluster_1",
        is_best_duplicate=True,
        score_qualite=0.9,
    )
    best_in_cluster_2 = structure_factory(
        source="src_1",
        id="src_1--latest",
        cluster_id="cluster_2",
        score_qualite=0.6,
        date_maj="2024-06-01",
    )
    structure_factory(
        source="src_1",
        id="src_1--older",
        cluster_id="cluster_2",
        score_qualite=0.6,
        date_maj="2020-01-01",
    )
    no_cluster = structure_factory(
        source="src_1",
        id="src_1--no-cluster",
        score_qualite=0.8,
        cluster_id=None,
        is_best_duplicate=None,
    )

    response = api_client.get(
        url,
        params={"sources": ["src_1"], "exclure_doublons": True},
    )
    assert_paginated_response_data(response.json(), total=3)
    assert {d["id"] for d in response.json()["items"]} == {
        best_in_cluster_1_for_src_1.id,
        best_in_cluster_2.id,
        no_cluster.id,
    }

    response = api_client.get(
        url,
        params={"exclure_doublons": True},
    )
    assert_paginated_response_data(response.json(), total=3)
    assert {d["id"] for d in response.json()["items"]} == {
        best_in_cluster_1_global.id,
        best_in_cluster_2.id,
        no_cluster.id,
    }


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/search/services"])
@pytest.mark.with_token
def test_search_services_deduplicate(
    api_client, url, structure_factory, service_factory
):
    best_in_cluster_1_for_src_1 = structure_factory(
        source="src_1",
        id="src_1--best",
        cluster_id="cluster_1",
        is_best_duplicate=False,
        score_qualite=0.5,
    )
    lower_in_cluster_1 = structure_factory(
        source="src_1",
        id="src_1--lower",
        cluster_id="cluster_1",
        is_best_duplicate=False,
        score_qualite=0.3,
    )
    best_in_cluster_1_global = structure_factory(
        source="src_2",
        id="src_2--best",
        cluster_id="cluster_1",
        is_best_duplicate=True,
        score_qualite=0.9,
    )
    latest_in_cluster_2 = structure_factory(
        source="src_1",
        id="src_1--latest",
        cluster_id="cluster_2",
        score_qualite=0.6,
        date_maj="2024-06-01",
    )
    older_in_cluster_2 = structure_factory(
        source="src_1",
        id="src_1--older",
        cluster_id="cluster_2",
        score_qualite=0.6,
        date_maj="2020-01-01",
    )
    no_cluster = structure_factory(
        source="src_1",
        id="src_1--no-cluster",
        cluster_id=None,
        is_best_duplicate=None,
        score_qualite=0.4,
    )

    best_structure_svc_a = service_factory(
        source="src_1",
        id="src_1--svc-a",
        structure=best_in_cluster_1_for_src_1,
        thematiques=["famille--garde-denfants"],
    )
    best_structure_svc_b = service_factory(
        source="src_1",
        id="src_1--svc-b",
        structure=best_in_cluster_1_for_src_1,
        thematiques=["mobilite--acceder-a-un-vehicule"],
    )
    service_factory(
        source="src_1",
        id="src_1--svc-excluded",
        structure=lower_in_cluster_1,
        thematiques=["famille--garde-denfants"],
    )
    latest_structure_svc = service_factory(
        source="src_1",
        id="src_1--svc-latest",
        structure=latest_in_cluster_2,
        thematiques=["famille--garde-denfants"],
    )
    service_factory(
        source="src_1",
        id="src_1--svc-older",
        structure=older_in_cluster_2,
        thematiques=["famille--garde-denfants"],
    )
    no_cluster_structure_svc = service_factory(
        source="src_1",
        id="src_1--svc-no-cluster",
        structure=no_cluster,
        thematiques=["mobilite--acceder-a-un-vehicule"],
    )
    global_best_structure_svc = service_factory(
        source="src_2",
        id="src_2--svc-global-best",
        structure=best_in_cluster_1_global,
        thematiques=["famille--garde-denfants"],
    )

    response = api_client.get(
        url,
        params={"sources": ["src_1"], "exclure_doublons": True},
    )
    assert_paginated_response_data(response.json(), total=4)
    assert {d["service"]["id"] for d in response.json()["items"]} == {
        best_structure_svc_a.id,
        best_structure_svc_b.id,
        latest_structure_svc.id,
        no_cluster_structure_svc.id,
    }

    response = api_client.get(
        url,
        params={"exclure_doublons": True},
    )
    assert_paginated_response_data(response.json(), total=3)
    assert {d["service"]["id"] for d in response.json()["items"]} == {
        global_best_structure_svc.id,
        latest_structure_svc.id,
        no_cluster_structure_svc.id,
    }

    response = api_client.get(
        url,
        params={
            "sources": ["src_1"],
            "thematiques": ["famille--garde-denfants"],
            "exclure_doublons": True,
        },
    )
    assert_paginated_response_data(response.json(), total=2)
    assert {d["service"]["id"] for d in response.json()["items"]} == {
        best_structure_svc_a.id,
        latest_structure_svc.id,
    }

    response = api_client.get(
        url,
        params={"thematiques": ["famille--garde-denfants"], "exclure_doublons": True},
    )
    assert_paginated_response_data(response.json(), total=2)
    assert {d["service"]["id"] for d in response.json()["items"]} == {
        global_best_structure_svc.id,
        latest_structure_svc.id,
    }

    response = api_client.get(
        url,
        params={
            "sources": ["src_1"],
            "thematiques": ["mobilite--acceder-a-un-vehicule"],
            "exclure_doublons": True,
        },
    )
    assert_paginated_response_data(response.json(), total=2)
    assert {d["service"]["id"] for d in response.json()["items"]} == {
        best_structure_svc_b.id,
        no_cluster_structure_svc.id,
    }


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/search/services"])
@pytest.mark.with_token
def test_search_services_deduplicate_best_structure_has_no_service(
    api_client, url, structure_factory, service_factory
):
    structure_factory(
        source="src_1",
        id="src_1--best-no-service",
        cluster_id="cluster_1",
        score_qualite=0.9,
    )
    lower_structure_with_service = structure_factory(
        source="src_1",
        id="src_1--lower-with-service",
        cluster_id="cluster_1",
        score_qualite=0.7,
    )
    service_on_lower_structure = service_factory(
        source="src_1",
        id="src_1--svc",
        structure=lower_structure_with_service,
        thematiques=["famille--garde-denfants"],
    )

    response = api_client.get(url, params={"exclure_doublons": True})

    assert response.status_code == 200
    assert {d["service"]["id"] for d in response.json()["items"]} == {
        service_on_lower_structure.id,
    }


@pytest.mark.parametrize("schema_version", ["v0"])
@pytest.mark.parametrize("path", ["/search/services", "/services", "/structures"])
@pytest.mark.with_token
def test_ressources_ordered_by_surrogate_id(api_client, url, factory):
    # Create 10 rows with ids in **reverse** order
    for i in reversed(range(10)):
        factory_kwargs = {
            "id": str(i),
            "source": str(i // 2),
        }
        if "search" in url:
            factory_kwargs["modes_accueil"] = [v0.ModeAccueil.EN_PRESENTIEL]
        factory(**factory_kwargs)

    response = api_client.get(url)
    assert response.status_code == 200
    items_data = response.json()["items"]
    assert [d["service"]["id"] if "search" in url else d["id"] for d in items_data] == [
        str(i) for i in range(10)
    ]


@pytest.mark.parametrize("schema_version", ["v1"])
@pytest.mark.parametrize("path", ["/search/services", "/services", "/structures"])
@pytest.mark.with_token
def test_ressources_ordered_by_id(api_client, url, factory):
    # Create 10 rows with ids in **reverse** order
    for i in reversed(range(10)):
        factory_kwargs = {
            "id": f"{str(i // 2)}--{str(i)}",
            "source": str(i // 2),
        }
        if "search" in url:
            factory_kwargs["modes_accueil"] = [v0.ModeAccueil.EN_PRESENTIEL]
        factory(**factory_kwargs)

    response = api_client.get(url)
    assert response.status_code == 200
    items_data = response.json()["items"]
    assert [d["service"]["id"] if "search" in url else d["id"] for d in items_data] == [
        "0--0",
        "0--1",
        "1--2",
        "1--3",
        "2--4",
        "2--5",
        "3--6",
        "3--7",
        "4--8",
        "4--9",
    ]
