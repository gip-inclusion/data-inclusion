import json
from unittest.mock import ANY

import pytest

from data_inclusion.api.decoupage_administratif.constants import RegionEnum
from data_inclusion.api.inclusion_data.v0 import models as models_v0
from data_inclusion.api.inclusion_data.v1 import models as models_v1
from data_inclusion.api.utils import soliguide
from data_inclusion.schema import v0

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
                thematiques=["choisir-un-metier"],
                typologie="ACI",
            ),
        ),
        (
            "v1",
            models_v1.Structure(
                _di_surrogate_id="04cfed4d-d3f7-4f9f-ad97-64bb6bbd7c96",
                accessibilite_lieu="https://acceslibre.beta.gouv.fr/app/kitchen-amount/",
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
                description="""Lorem ipsum dolor sit amet, consectetur adipiscing elit,
                sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.""",
                rna="W242194892",
                score_qualite=0.7,
                siret="76475938700658",
                site_web="https://www.le.net/",
                source="dora",
                telephone="0102030405",
                typologie="ACI",
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


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
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


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
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
                    thematiques=["choisir-un-metier"],
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
                    thematiques=["choisir-un-metier"],
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
                    _di_surrogate_id="04cfed4d-d3f7-4f9f-ad97-64bb6bbd7c96",
                    accessibilite_lieu="https://acceslibre.beta.gouv.fr/app/kitchen-amount/",
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
                    description="""Lorem ipsum dolor sit amet, consectetur adipiscing
                    elit, sed do eiusmod tempor incididunt ut labore et dolore magna
                    aliqua.""",
                    rna="W242194892",
                    score_qualite=0.3,
                    siret="76475938700658",
                    site_web="https://www.le.net/",
                    source="dora",
                    telephone="0102030405",
                    typologie="ACI",
                ),
                models_v1.Service(
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
                    description="""Lorem ipsum dolor sit amet, consectetur adipiscing
                    elit, sed do eiusmod tempor incididunt ut labore et dolore magna
                    aliqua.""",
                    prise_rdv="https://teixeira.fr/",
                    profils=["femmes"],
                    profils_precisions="Femme en situation d'insertion",
                    recurrence=None,
                    score_qualite=0.5,
                    source="dora",
                    structure_id="much-mention",
                    telephone="0102030405",
                    thematiques=["choisir-un-metier"],
                    types=["formation"],
                    zone_diffusion_code=None,
                    zone_diffusion_nom=None,
                    zone_diffusion_type=None,
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


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/services", "/search/services"])
@pytest.mark.parametrize(
    "profils_precisions,input,found",
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


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
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
def test_can_filter_resources_by_profils_precisions_with_only_profils_data(
    api_client, profils, input, found, url, service_factory
):
    resource = service_factory(profils=profils, profils_precisions="")
    service_factory(profils=[v0.Profil.RETRAITES], profils_precisions="")

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
        ("v1", "/services"),
        ("v0", "/search/services"),
        ("v1", "/search/services"),
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
def test_can_filter_resources_by_thematiques(
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
@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/services", "/search/services"])
def test_can_filter_services_by_profils(
    api_client,
    url,
    service_factory,
):
    service_1 = service_factory(profils=[v0.Profil.FEMMES.value])
    service_2 = service_factory(profils=[v0.Profil.JEUNES_16_26.value])
    service_factory(profils=[v0.Profil.ADULTES.value])
    service_factory(profils=[])
    service_factory(profils=None)

    response = api_client.get(
        url,
        params={
            "profils": [
                v0.Profil.FEMMES.value,
                v0.Profil.JEUNES_16_26.value,
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
            "profils": v0.Profil.BENEFICIAIRES_RSA.value,
        },
    )
    assert_paginated_response_data(response.json(), total=0)


@pytest.mark.with_token
@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/services", "/search/services"])
def test_list_services_by_types(api_client, url, service_factory):
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
@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/services", "/search/services"])
def test_can_filter_services_by_frais(api_client, url, service_factory):
    service_1 = service_factory(frais=[v0.Frais.GRATUIT.value])
    service_2 = service_factory(frais=[v0.Frais.ADHESION.value])
    service_factory(frais=[v0.Frais.PASS_NUMERIQUE.value])

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
        ("soliguide", LILLE["code_insee"], False),
        ("soliguide", STRASBOURG["code_insee"], False),
        ("emplois-de-linclusion", PARIS["code_insee"], False),
        ("emplois-de-linclusion", None, False),
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


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/search/services"])
@pytest.mark.with_token
def test_search_services_with_zone_diffusion_pays(api_client, url, service_factory):
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


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/search/services"])
@pytest.mark.with_token
def test_search_services_with_zone_diffusion_commune(api_client, url, service_factory):
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


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/search/services"])
@pytest.mark.with_token
def test_search_services_with_zone_diffusion_epci(api_client, url, service_factory):
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


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
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


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/search/services"])
@pytest.mark.with_token
def test_search_services_with_zone_diffusion_region(api_client, url, service_factory):
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


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/services"])
@pytest.mark.with_token
def test_retrieve_service(api_client, url, service_factory):
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


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/structures"])
@pytest.mark.with_token
def test_retrieve_structure(api_client, url, structure_factory, service_factory):
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


class FakeSoliguideClient(soliguide.SoliguideAPIClient):
    def __init__(self):
        self.retrieved_ids = []

    def retrieve_place(self, place_id: str):
        self.retrieved_ids.append(place_id)


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/structures"])
@pytest.mark.with_token
def test_retrieve_structure_and_notify_soliguide(
    api_client, app, url, structure_factory
):
    structure_1 = structure_factory(source="soliguide", id="soliguide-structure-id")

    fake_soliguide_client = FakeSoliguideClient()
    app.dependency_overrides[soliguide.SoliguideAPIClient] = (
        lambda: fake_soliguide_client
    )

    response = api_client.get(url + f"/{structure_1.source}/{structure_1.id}")

    assert response.status_code == 200
    assert fake_soliguide_client.retrieved_ids == ["soliguide-structure-id"]


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/services"])
@pytest.mark.parametrize(
    ("requested_id", "status_code", "retrieved_ids"),
    [
        ("soliguide-service-id", 200, ["soliguide-structure-id"]),
        ("not-a-soliguide-service-id", 404, []),
    ],
)
@pytest.mark.with_token
def test_retrieve_service_and_notify_soliguide(
    api_client,
    app,
    url,
    requested_id,
    status_code,
    retrieved_ids,
    structure_factory,
    service_factory,
):
    structure_factory(source="soliguide", id="soliguide-structure-id")
    service_1 = service_factory(
        source="soliguide",
        id="soliguide-service-id",
        structure_id="soliguide-structure-id",
    )

    fake_soliguide_client = FakeSoliguideClient()
    app.dependency_overrides[soliguide.SoliguideAPIClient] = (
        lambda: fake_soliguide_client
    )

    response = api_client.get(url + f"/{service_1.source}/{requested_id}")

    assert response.status_code == status_code
    assert fake_soliguide_client.retrieved_ids == retrieved_ids


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/structures"])
@pytest.mark.parametrize(
    ("exclure_doublons", "expected_ids"),
    [(False, {"first", "second", "third", "fourth"}), (True, {"second", "fourth"})],
)
@pytest.mark.with_token
def test_list_structures_deduplicate_flag(
    api_client, url, exclure_doublons, expected_ids, structure_factory
):
    structure_factory(
        source="src_1",
        id="first",
        cluster_id="cluster_1",
        is_best_duplicate=False,
        score_qualite=0.5,
    )
    structure_factory(
        source="src_2",
        id="second",
        cluster_id="cluster_1",
        is_best_duplicate=True,
        score_qualite=0.9,
    )
    structure_factory(
        source="src_2",
        id="third",
        cluster_id="cluster_1",
        is_best_duplicate=False,
        score_qualite=0.3,
    )
    structure_factory(
        source="src_3",
        id="fourth",
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
        id="first",
        cluster_id="cluster_1",
        is_best_duplicate=False,
    )
    structure_factory(
        date_maj="2024-01-01",  # the latest update
        source="src_2",
        id="second",
        cluster_id="cluster_1",
        is_best_duplicate=True,
    )
    structure_factory(
        date_maj="2008-01-01",
        source="src_3",
        id="third",
        cluster_id="cluster_1",
        is_best_duplicate=False,
    )
    structure_factory(
        source="src_3",
        id="fourth",
        cluster_id=None,
        is_best_duplicate=None,
    )

    response = api_client.get(url, params={"exclure_doublons": True})

    assert_paginated_response_data(response.json(), total=2)
    assert {d["id"] for d in response.json()["items"]} == {"second", "fourth"}


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
@pytest.mark.parametrize("path", ["/search/services"])
@pytest.mark.with_token
def test_search_services_deduplicate_flag(
    api_client, url, structure_factory, service_factory
):
    s1 = structure_factory(
        date_maj="2020-01-01",
        source="src_1",
        id="first",
        cluster_id="cluster_1",
        is_best_duplicate=False,
    )
    s2 = structure_factory(
        date_maj="2024-01-01",  # the latest update
        source="src_2",
        id="second",
        cluster_id="cluster_1",
        is_best_duplicate=True,
    )
    s3 = structure_factory(
        date_maj="2008-01-01",
        source="src_3",
        id="third",
        cluster_id="cluster_1",
        is_best_duplicate=False,
    )
    s4 = structure_factory(
        source="src_3",
        id="fourth",
        cluster_id=None,
        is_best_duplicate=None,
    )

    service_factory(
        source="src_1",
        id="first",
        structure=s1,
    )
    service_factory(
        source="src_2",
        id="second",
        structure=s2,
    )
    service_factory(
        source="src_2",
        id="third",
        structure=s3,
    )
    service_factory(
        source="src_3",
        id="fourth",
        structure=s4,
    )

    response = api_client.get(url, params={"exclure_doublons": True})

    assert_paginated_response_data(response.json(), total=2)
    assert {d["service"]["id"] for d in response.json()["items"]} == {
        "second",
        "fourth",
    }


@pytest.mark.parametrize("schema_version", ["v0", "v1"])
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
