import functools
import json
from collections import defaultdict
from pathlib import Path

import geoalchemy2
import sqlalchemy as sqla
from sqlalchemy import orm
from sqlalchemy.dialects.postgresql import TSQUERY

from data_inclusion.api.decoupage_administratif import constants
from data_inclusion.api.decoupage_administratif.models import Commune
from data_inclusion.api.inclusion_data import models, parameters
from data_inclusion.schema import v1

THEMATIQUE_ALIASES: list[tuple[str, str]] = [
    ("choisir-un-metier--confirmer-son-choix-de-metier", "immersion"),
    ("choisir-un-metier--confirmer-son-choix-de-metier", "mise en situation"),
    ("choisir-un-metier--confirmer-son-choix-de-metier", "stage"),
    ("choisir-un-metier--connaitre-les-opportunites-demploi", "débouchés"),
    ("choisir-un-metier--connaitre-les-opportunites-demploi", "emploi local"),
    ("choisir-un-metier--connaitre-les-opportunites-demploi", "marché du travail"),
    ("choisir-un-metier--connaitre-les-opportunites-demploi", "secteurs qui recrutent"),
    (
        "choisir-un-metier--decouvrir-un-metier-ou-un-secteur-dactivite",
        "découverte métier",
    ),
    ("choisir-un-metier--decouvrir-un-metier-ou-un-secteur-dactivite", "forum métiers"),
    ("choisir-un-metier--decouvrir-un-metier-ou-un-secteur-dactivite", "reconversion"),
    (
        "choisir-un-metier--identifier-ses-points-forts-et-ses-competences",
        "bilan de compétences",
    ),
    (
        "choisir-un-metier--identifier-ses-points-forts-et-ses-competences",
        "savoir-faire",
    ),
    ("choisir-un-metier--identifier-ses-points-forts-et-ses-competences", "évaluation"),
    (
        "creer-une-entreprise--definir-son-projet-de-creation-dentreprise",
        "auto-entrepreneur",
    ),
    (
        "creer-une-entreprise--definir-son-projet-de-creation-dentreprise",
        "entrepreneur",
    ),
    ("creer-une-entreprise--definir-son-projet-de-creation-dentreprise", "freelance"),
    (
        "creer-une-entreprise--definir-son-projet-de-creation-dentreprise",
        "micro-entreprise",
    ),
    ("creer-une-entreprise--developper-son-entreprise", "croissance"),
    ("creer-une-entreprise--developper-son-entreprise", "développement commercial"),
    ("creer-une-entreprise--developper-son-entreprise", "gestion entreprise"),
    (
        "creer-une-entreprise--structurer-son-projet-de-creation-dentreprise",
        "business plan",
    ),
    (
        "creer-une-entreprise--structurer-son-projet-de-creation-dentreprise",
        "statut juridique",
    ),
    (
        "difficultes-administratives-ou-juridiques--accompagnement-aux-demarches-administratives",
        "administratif",
    ),
    (
        "difficultes-administratives-ou-juridiques--accompagnement-aux-demarches-administratives",
        "formalités",
    ),
    (
        "difficultes-administratives-ou-juridiques--accompagnement-pour-lacces-a-la-citoyennete",
        "naturalisation",
    ),
    (
        "difficultes-administratives-ou-juridiques--accompagnement-pour-lacces-a-la-citoyennete",
        "titre de séjour",
    ),
    (
        "difficultes-administratives-ou-juridiques--accompagnement-pour-lacces-a-la-citoyennete",
        "vie civique",
    ),
    (
        "difficultes-administratives-ou-juridiques--prendre-en-compte-une-problematique-judiciaire",
        "avocat",
    ),
    (
        "difficultes-administratives-ou-juridiques--prendre-en-compte-une-problematique-judiciaire",
        "juridique",
    ),
    (
        "difficultes-administratives-ou-juridiques--prendre-en-compte-une-problematique-judiciaire",
        "justice",
    ),
    (
        "difficultes-administratives-ou-juridiques--prendre-en-compte-une-problematique-judiciaire",
        "recours",
    ),
    ("difficultes-financieres--acquerir-une-autonomie-budgetaire", "compte bancaire"),
    ("difficultes-financieres--acquerir-une-autonomie-budgetaire", "gestion argent"),
    ("difficultes-financieres--acquerir-une-autonomie-budgetaire", "épargne"),
    ("difficultes-financieres--ameliorer-sa-gestion-budgetaire", "budget"),
    ("difficultes-financieres--ameliorer-sa-gestion-budgetaire", "finance"),
    ("difficultes-financieres--ameliorer-sa-gestion-budgetaire", "trésorerie"),
    (
        "difficultes-financieres--mettre-en-place-une-mesure-de-protection-financiere",
        "curatelle",
    ),
    (
        "difficultes-financieres--mettre-en-place-une-mesure-de-protection-financiere",
        "protection juridique",
    ),
    (
        "difficultes-financieres--mettre-en-place-une-mesure-de-protection-financiere",
        "tutelle",
    ),
    (
        "difficultes-financieres--prevenir-une-degradation-de-la-situation-financiere",
        "microcrédit",
    ),
    (
        "difficultes-financieres--prevenir-une-degradation-de-la-situation-financiere",
        "prêt solidaire",
    ),
    ("difficultes-financieres--situation-dendettement-surendettement", "dette"),
    ("difficultes-financieres--situation-dendettement-surendettement", "impayé"),
    ("equipement-et-alimentation--aide-menagere", "aide à domicile"),
    ("equipement-et-alimentation--aide-menagere", "entretien logement"),
    ("equipement-et-alimentation--alimentation", "alimentaire"),
    ("equipement-et-alimentation--alimentation", "repas"),
    ("famille--garde-denfants", "assistante maternelle"),
    ("famille--garde-denfants", "crèche"),
    ("famille--garde-denfants", "nourrice"),
    ("famille--prise-en-charge-personne-dependante", "aidant"),
    ("famille--prise-en-charge-personne-dependante", "aide soignant"),
    ("famille--prise-en-charge-personne-dependante", "personne âgée"),
    ("famille--soutien-a-la-parentalite-et-a-leducation", "famille"),
    ("famille--soutien-aidants", "aide soignant"),
    ("famille--soutien-aidants", "handicap"),
    ("famille--surmonter-conflits-separation-violence", "divorce"),
    ("famille--surmonter-conflits-separation-violence", "pension alimentaire"),
    ("lecture-ecriture-calcul--maitriser-le-calcul", "remise à niveau"),
    ("lecture-ecriture-calcul--maitriser-le-calcul", "savoirs fondamentaux"),
    ("lecture-ecriture-calcul--maitriser-le-francais", "alphabétisation"),
    ("lecture-ecriture-calcul--maitriser-le-francais", "cours de français"),
    ("lecture-ecriture-calcul--maitriser-le-francais", "fle"),
    ("lecture-ecriture-calcul--maitriser-le-francais", "français langue étrangère"),
    ("lecture-ecriture-calcul--maitriser-le-francais", "illettrisme"),
    ("logement-hebergement--acheter-un-logement", "prêt immobilier"),
    ("logement-hebergement--changer-de-logement", "déménagement"),
    ("logement-hebergement--louer-un-logement", "bail"),
    ("logement-hebergement--louer-un-logement", "hlm"),
    ("logement-hebergement--louer-un-logement", "logements sociaux"),
    (
        "logement-hebergement--rechercher-une-solution-dhebergement-temporaire",
        "centre d'accueil",
    ),
    ("logement-hebergement--rechercher-une-solution-dhebergement-temporaire", "foyer"),
    (
        "logement-hebergement--rechercher-une-solution-dhebergement-temporaire",
        "sans-abri",
    ),
    ("logement-hebergement--reduire-les-impayes-de-loyer", "expulsion"),
    ("logement-hebergement--reduire-les-impayes-de-loyer", "impayé"),
    ("logement-hebergement--se-maintenir-dans-le-logement", "insalubrité"),
    ("logement-hebergement--se-maintenir-dans-le-logement", "rénovation"),
    ("logement-hebergement--se-maintenir-dans-le-logement", "travaux"),
    (
        "logement-hebergement--sinformer-sur-les-demarches-liees-a-lacces-au-logement",
        "domiciliation",
    ),
    ("mobilite--acceder-a-un-vehicule", "deux-roues"),
    ("mobilite--acceder-a-un-vehicule", "voiture"),
    ("mobilite--acceder-a-un-vehicule", "location voiture"),
    ("mobilite--entretenir-reparer-son-vehicule", "contrôle technique"),
    ("mobilite--entretenir-reparer-son-vehicule", "garage"),
    ("mobilite--entretenir-reparer-son-vehicule", "réparation"),
    ("mobilite--etre-accompagne-dans-son-parcours-mobilite", "déplacement"),
    ("mobilite--etre-accompagne-dans-son-parcours-mobilite", "itinéraire"),
    ("mobilite--etre-accompagne-dans-son-parcours-mobilite", "trajet"),
    ("mobilite--financer-ma-mobilite", "aide transport"),
    ("mobilite--mobilite-douce-partagee-collective", "bus"),
    ("mobilite--mobilite-douce-partagee-collective", "covoiturage"),
    ("mobilite--mobilite-douce-partagee-collective", "transport"),
    ("mobilite--mobilite-douce-partagee-collective", "trottinette"),
    ("mobilite--mobilite-douce-partagee-collective", "vélo"),
    ("mobilite--preparer-un-permis", "auto-école"),
    ("mobilite--preparer-un-permis", "code de la route"),
    ("mobilite--preparer-un-permis", "conduite"),
    ("numerique--acceder-a-des-services-en-ligne", "dématérialisation"),
    ("numerique--acceder-a-des-services-en-ligne", "informatique"),
    ("numerique--acceder-a-des-services-en-ligne", "internet"),
    ("numerique--acceder-a-une-connexion-internet", "wifi"),
    ("numerique--acquerir-un-equipement", "ordinateur"),
    ("numerique--acquerir-un-equipement", "smartphone"),
    ("numerique--acquerir-un-equipement", "tablette"),
    ("numerique--maitriser-les-fondamentaux-du-numerique", "bureautique"),
    ("numerique--maitriser-les-fondamentaux-du-numerique", "digital"),
    ("numerique--maitriser-les-fondamentaux-du-numerique", "illectronisme"),
    ("preparer-sa-candidature--developper-son-reseau", "mentorat"),
    ("preparer-sa-candidature--developper-son-reseau", "parrainage"),
    (
        "preparer-sa-candidature--organiser-ses-demarches-de-recherche-demploi",
        "candidature",
    ),
    (
        "preparer-sa-candidature--realiser-un-cv-et-ou-une-lettre-de-motivation",
        "portfolio",
    ),
    ("preparer-sa-candidature--valoriser-ses-competences", "bilan de compétences"),
    ("preparer-sa-candidature--valoriser-ses-competences", "validation acquis"),
    ("remobilisation--activites-sportives-et-culturelles", "arts"),
    ("remobilisation--activites-sportives-et-culturelles", "atelier créatif"),
    ("remobilisation--activites-sportives-et-culturelles", "loisirs"),
    ("remobilisation--benevolat-action-citoyenne", "service civique"),
    ("remobilisation--benevolat-action-citoyenne", "volontariat"),
    ("remobilisation--bien-etre-confiance-en-soi", "estime de soi"),
    ("remobilisation--lien-social", "socialisation"),
    ("sante--acces-aux-soins", "hygiène"),
    ("sante--acces-aux-soins", "médecin"),
    ("sante--acces-aux-soins", "santé"),
    ("sante--addictions", "alcool"),
    ("sante--addictions", "drogue"),
    ("sante--addictions", "sevrage"),
    ("sante--addictions", "désintoxication"),
    ("sante--constituer-un-dossier-mdph-invalidite", "handicap"),
    ("sante--constituer-un-dossier-mdph-invalidite", "rqth"),
    ("sante--sante-mentale", "psychologue"),
    ("sante--sante-mentale", "thérapie"),
    ("sante--sante-mentale", "psychatre"),
    ("sante--sante-mentale", "cmp"),
    ("sante--sante-sexuelle", "contraception"),
    ("sante--sante-sexuelle", "dépistage"),
    ("sante--sante-sexuelle", "planning familial"),
    ("se-former--monter-son-dossier-de-formation", "certification"),
    ("se-former--monter-son-dossier-de-formation", "cpf"),
    ("se-former--monter-son-dossier-de-formation", "financement formation"),
    ("se-former--trouver-sa-formation", "certification"),
    ("se-former--trouver-sa-formation", "cpf"),
    (
        "souvrir-a-linternational--connaitre-les-opportunites-demploi-a-letranger",
        "expatriation",
    ),
    (
        "souvrir-a-linternational--connaitre-les-opportunites-demploi-a-letranger",
        "international",
    ),
    (
        "souvrir-a-linternational--connaitre-les-opportunites-demploi-a-letranger",
        "visa",
    ),
    (
        "souvrir-a-linternational--sinformer-sur-les-aides-pour-travailler-a-letranger",
        "mobilité européenne",
    ),
    (
        "souvrir-a-linternational--sinformer-sur-les-aides-pour-travailler-a-letranger",
        "mobilité internationale",
    ),
    (
        "souvrir-a-linternational--sorganiser-suite-a-son-retour-en-france",
        "rapatriement",
    ),
    ("trouver-un-emploi--convaincre-un-recruteur-en-entretien", "entretien embauche"),
    ("trouver-un-emploi--convaincre-un-recruteur-en-entretien", "pitch"),
    ("trouver-un-emploi--convaincre-un-recruteur-en-entretien", "simulation entretien"),
    ("trouver-un-emploi--faire-des-candidatures-spontanees", "démarchage"),
    ("trouver-un-emploi--faire-des-candidatures-spontanees", "prospection"),
    ("trouver-un-emploi--maintien-dans-lemploi", "travail"),
    ("trouver-un-emploi--repondre-a-des-offres-demploi", "candidature"),
    ("trouver-un-emploi--repondre-a-des-offres-demploi", "embauche"),
    ("trouver-un-emploi--repondre-a-des-offres-demploi", "recrutement"),
]


@functools.cache
def get_thematiques_by_group() -> dict[str, list[str]]:
    thematiques = defaultdict(list)
    for thematique in v1.Thematique:
        try:
            theme, _ = str(thematique.value).split("--")
        except ValueError:
            continue
        thematiques[theme].append(thematique.value)
    return thematiques


def list_structures_query(
    params: parameters.ListStructuresQueryParams,
    include_soliguide: bool,
) -> sqla.Select[tuple[models.Structure]]:
    query = sqla.select(models.Structure).options(
        orm.joinedload(models.Structure.doublons),
    )

    if not include_soliguide:
        query = query.filter(models.Structure.source != "soliguide")

    if params.sources is not None:
        query = query.filter(
            models.Structure.source == sqla.any_(sqla.literal(params.sources))
        )

    if params.code_commune is not None:
        query = query.filter_by(code_insee=params.code_commune)

    if params.departement is not None:
        query = query.filter(
            models.Structure.code_insee.startswith(params.departement.code)
        )

    if params.region is not None:
        query = query.join(Commune).options(
            orm.contains_eager(models.Structure.commune_)
        )
        query = query.filter(Commune.region == params.region.code)

    if params.reseaux_porteurs is not None:
        query = query.filter(
            sqla.exists(
                sqla.select(sqla.literal(1))
                .select_from(
                    sqla.func.unnest(models.Structure.reseaux_porteurs).alias("item")
                )
                .where(
                    sqla.literal_column("item")
                    == sqla.any_(
                        sqla.literal([f.value for f in params.reseaux_porteurs])
                    )
                )
            )
        )

    if params.exclure_doublons:
        cluster_key = sqla.func.coalesce(
            models.Structure._cluster_id,
            models.Structure.id,
        )
        query = query.distinct(cluster_key).order_by(
            cluster_key,
            models.Structure.score_qualite.desc(),
            models.Structure.date_maj.desc().nulls_last(),
            models.Structure.id,
        )

    query = query.order_by(models.Structure.id)
    return query


def retrieve_structure_query(
    params: parameters.RetrieveStructurePathParams,
) -> sqla.Select[tuple[models.Structure]]:
    return (
        sqla.select(models.Structure)
        .options(orm.selectinload(models.Structure.services))
        .options(orm.selectinload(models.Structure.doublons))
        .filter_by(id=params.id)
        .limit(1)
    )


def retrieve_structure(
    db_session: orm.Session,
    params: parameters.RetrieveStructurePathParams,
) -> models.Structure | None:
    return db_session.execute(
        retrieve_structure_query(params=params)
    ).scalar_one_or_none()


@functools.cache
def list_sources() -> list[dict]:
    return json.loads((Path(__file__).parent / "sources.json").read_text())


def filter_services(
    query: sqla.Select,
    params: parameters.ListServicesQueryParams
    | parameters.SearchServicesQueryParams
    | parameters.SearchQueryParams,
) -> sqla.Select:
    if params.sources is not None:
        query = query.filter(
            models.Service.source == sqla.any_(sqla.literal(params.sources))
        )

    if params.thematiques is not None:
        thematiques = [
            get_thematiques_by_group()[t.value]
            if isinstance(t, v1.Categorie)
            else t.value
            for t in params.thematiques
        ]
        query = query.filter(
            sqla.text(
                f"{models.Service.__tablename__}.thematiques && :thematiques"
            ).bindparams(thematiques=thematiques),
        )

    if params.frais is not None:
        query = query.filter(
            models.Service.frais == sqla.any_(sqla.literal(params.frais))
        )

    if params.publics is not None and v1.Public.TOUS_PUBLICS not in params.publics:
        # also match services for all publics
        publics = params.publics + [v1.Public.TOUS_PUBLICS]

        query = query.filter(
            sqla.exists(
                sqla.select(sqla.literal(1))
                .select_from(sqla.func.unnest(models.Service.publics).alias("item"))
                .where(
                    sqla.literal_column("item")
                    == sqla.any_(sqla.literal([p.value for p in publics]))
                )
            )
        )

    if params.modes_accueil is not None:
        query = query.filter(
            sqla.exists(
                sqla.select(sqla.literal(1))
                .select_from(
                    sqla.func.unnest(models.Service.modes_accueil).alias("item")
                )
                .where(
                    sqla.literal_column("item")
                    == sqla.any_(sqla.literal([f.value for f in params.modes_accueil]))
                )
            )
        )

    if params.types is not None:
        query = query.filter(
            models.Service.type == sqla.any_(sqla.literal(params.types))
        )

    if params.score_qualite_minimum is not None:
        query = query.filter(
            models.Service.score_qualite >= params.score_qualite_minimum
        )

    if (
        isinstance(
            params,
            (
                parameters.SearchServicesQueryParams,
                parameters.ListServicesQueryParams,
            ),
        )
        and params.recherche_public is not None
    ):
        websearch_to_tsquery = sqla.func.websearch_to_tsquery(
            "public.french", params.recherche_public
        )
        query = query.filter(
            sqla.or_(
                models.Service.searchable_index_publics.bool_op("@@")(
                    websearch_to_tsquery
                ),
                models.Service.searchable_index_publics_precisions.bool_op("@@")(
                    websearch_to_tsquery
                ),
            )
        )

    return query


def list_services_query(
    params: parameters.ListServicesQueryParams,
    include_soliguide: bool,
):
    query = (
        sqla.select(models.Service)
        .join(models.Structure)
        .options(orm.contains_eager(models.Service.structure))
    )

    if not include_soliguide:
        query = query.filter(models.Structure.source != "soliguide")

    if params.departement is not None:
        query = query.filter(
            models.Service.code_insee.startswith(params.departement.code)
        )

    if params.region is not None:
        query = query.join(models.Service.commune_).options(
            orm.contains_eager(models.Service.commune_)
        )
        query = query.filter(Commune.region == params.region.code)

    if params.code_commune is not None:
        query = query.filter(models.Service.code_insee == params.code_commune)

    query = filter_services(query=query, params=params)

    query = query.order_by(models.Service.id)

    return query


def search_services_query(
    params: parameters.SearchServicesQueryParams,
    include_soliguide: bool,
    include_remote_services: bool = True,
    commune_instance: Commune | None = None,
) -> tuple[sqla.Select[tuple[models.Service, int]], tuple[str, str]]:
    query = (
        sqla.select(models.Service)
        .join(models.Structure)
        .options(orm.contains_eager(models.Service.structure))
    )

    if not include_soliguide:
        query = query.filter(models.Structure.source != "soliguide")

    if commune_instance is not None:
        zone_eligibilite_codes = [
            commune_instance.code,
            commune_instance.departement,
            constants.PaysEnum.FRANCE.value.code,
            constants.PaysEnum.FRANCE.value.slug,
        ]
        if commune_instance.siren_epci is not None:
            zone_eligibilite_codes.append(commune_instance.siren_epci)

        query = query.filter(
            sqla.or_(
                models.Service.zone_eligibilite.is_(None),
                models.Service.zone_eligibilite.op("&&")(
                    sqla.literal(zone_eligibilite_codes)
                ),
            )
        )

        src_geometry = sqla.cast(
            geoalchemy2.functions.ST_MakePoint(
                models.Service.longitude, models.Service.latitude
            ),
            geoalchemy2.Geography(geometry_type="GEOMETRY", srid=4326),
        )

        if params.lon is not None and params.lat is not None:
            dest_geometry = f"POINT({params.lon} {params.lat})"
        else:
            dest_geometry = commune_instance.centre

        is_within_range = geoalchemy2.functions.ST_DWithin(
            src_geometry, dest_geometry, 50_000
        )
        is_available_on_site = models.Service.modes_accueil.contains(
            sqla.literal([v1.ModeAccueil.EN_PRESENTIEL.value])
        )
        is_available_remotely = models.Service.modes_accueil.contains(
            sqla.literal([v1.ModeAccueil.A_DISTANCE.value])
        )

        if include_remote_services:
            query = query.filter(sqla.or_(is_within_range, is_available_remotely))
        else:
            query = query.filter(is_within_range, is_available_on_site)

        distance_km = (
            geoalchemy2.functions.ST_Distance(src_geometry, dest_geometry) / 1000
        ).cast(sqla.Integer)
        query = query.add_columns(
            sqla.case(
                (sqla.and_(is_available_on_site, is_within_range), distance_km),
                else_=sqla.null().cast(sqla.Integer),
            ).label("distance")
        )

    else:
        query = query.add_columns(sqla.null().cast(sqla.Integer).label("distance"))

    query = filter_services(query=query, params=params)

    if params.exclure_doublons:
        cluster_key = sqla.func.coalesce(
            models.Structure._cluster_id,
            models.Structure.id,
        )
        structure_rank = (
            sqla.func.dense_rank()
            .over(
                partition_by=cluster_key,
                order_by=[
                    models.Structure.score_qualite.desc(),
                    models.Structure.date_maj.desc().nulls_last(),
                ],
            )
            .label("_structure_rank")
        )
        ranked_subq = query.add_columns(structure_rank).subquery()

        query = query.where(
            models.Service.id.in_(
                sqla.select(ranked_subq.c.id).where(ranked_subq.c._structure_rank == 1)
            )
        )

    query = query.order_by(
        sqla.column("distance").nulls_last(),
        models.Service.id,
    )

    return query, ("service", "distance")


def retrieve_service_query(
    params: parameters.RetrieveServicePathParams,
) -> sqla.Select[tuple[models.Service]]:
    return (
        sqla.select(models.Service)
        .join(models.Structure)
        .filter(models.Service.id == params.id)
        .limit(1)
    )


def retrieve_service(
    db_session: orm.Session,
    params: parameters.RetrieveServicePathParams,
) -> models.Service | None:
    return db_session.execute(
        retrieve_service_query(params=params)
    ).scalar_one_or_none()


def to_vector_with_variants(db_session: orm.Session, q: str) -> sqla.ColumnElement:
    """A typo-tolerant tsquery.

    Method recommended by the pg_trgm doc
    https://www.postgresql.org/docs/18/pgtrgm.html#PGTRGM-TEXT-SEARCH
    It needs a lexicon of words and their frequency in the database, here
    `api__search_lexicon_v1`, filled by `build_search_index`

    PG indexes lexemes, ie roots without accent or ending:
    "GARAGE Solidaire" becomes "garag" and "solidair".
    An error in the query or in the database gives a different lexeme
    such as "solidiar", which matches nothing.
    So we look for "close enough" lexemes through trigrams
    """
    tsquery = db_session.execute(
        sqla.text("""
            SELECT STRING_AGG(
                '(' || CONCAT_WS(
                    '|',
                    QUOTE_LITERAL(lexeme),
                    expansion.variants
                ) || ')',
                ' & '
            )
            FROM UNNEST(
                TSVECTOR_TO_ARRAY(TO_TSVECTOR('public.french', :q))
            ) AS lexeme
            LEFT JOIN api__search_lexicon_v1 AS common_word
                ON common_word.word = lexeme
                AND common_word.ndoc >= :max_ndoc
            CROSS JOIN LATERAL (
                SELECT STRING_AGG(QUOTE_LITERAL(lex.word), '|') AS variants
                FROM (
                    SELECT lex.word
                    FROM api__search_lexicon_v1 AS lex
                    WHERE
                        common_word.word IS NULL
                        AND LENGTH(lexeme) >= :min_length
                        AND lex.word % lexeme
                    ORDER BY
                        SIMILARITY(lex.word, lexeme)
                        + :freq_weight * LN(lex.ndoc + 1)
                        DESC
                    LIMIT :n_variants
                ) AS lex
            ) AS expansion
        """),
        {
            "q": q,
            # NOTE: those thresholds have been found though data analysis.
            # A word found in more than 100 services is probably legit
            # The most widespread typo in the database "illetr" is found
            # in 37 services
            "max_ndoc": 100,
            # keep at most 4 variants of each lexeme
            "n_variants": 4,
            # Make typos lean towards common words, not other typos !
            # Lower it and "solidiar" picks "solidiair" which is a typo
            # Increase it and a correct but rare word no longer comes up
            "freq_weight": 0.05,
            # do not find close lexemes for words shorter than 4 characters
            "min_length": 4,
        },
    ).scalar()

    return sqla.cast(tsquery or "", TSQUERY)


# Every thematique alias gets attached to the document with a fabricated
# unique token (very much like a hash)
CONCATENATED_TOKEN_PREFIX = "zz"
CONCATENATED_ALIAS_SQL = (
    f"'{CONCATENATED_TOKEN_PREFIX}' || replace(unaccent(lower(alias)), ' ', '')"
)


def to_vector_concatenated(db_session: orm.Session, q: str) -> sqla.ColumnElement:
    """Tsquery for an exact match on a concatenated multi-word alias token."""
    tsquery = db_session.execute(
        sqla.text("""
            SELECT plainto_tsquery(
                'simple',
                :prefix || replace(unaccent(lower(:q)), ' ', '')
            )
        """),
        {"q": q, "prefix": CONCATENATED_TOKEN_PREFIX},
    ).scalar()
    return sqla.cast(tsquery or "", TSQUERY)


def build_thematique_aliases(db_session: orm.Session) -> None:
    db_session.execute(sqla.text("TRUNCATE api__thematique_aliases_v1"))
    for thematique_code, alias in THEMATIQUE_ALIASES:
        db_session.execute(
            sqla.text("""
                INSERT INTO api__thematique_aliases_v1
                    (thematique_code, alias, lexemes)
                VALUES (
                    :thematique_code,
                    :alias,
                    tsvector_to_array(to_tsvector('public.french', :alias))
                )
            """),
            {"thematique_code": thematique_code, "alias": alias},
        )
    db_session.commit()


def search_query(
    db_session: orm.Session,
    params: parameters.SearchQueryParams,
    include_soliguide: bool,
) -> tuple[sqla.Select[tuple[models.Service, int]], tuple[str, str, str]]:
    query = (
        sqla.select(models.Service)
        .join(models.Structure)
        .options(orm.contains_eager(models.Service.structure))
    )

    if not include_soliguide:
        query = query.filter(models.Structure.source != "soliguide")

    query = filter_services(query=query, params=params)

    if params.departement is not None:
        query = query.filter(
            models.Service.code_insee.startswith(params.departement.code)
        )

    if params.region is not None:
        query = query.join(models.Service.commune_).options(
            orm.contains_eager(models.Service.commune_)
        )
        query = query.filter(Commune.region == params.region.code)

    if params.code_commune is not None:
        query = query.filter(models.Service.code_insee == params.code_commune)

    score_recherche_expr = None
    if params.q is None:
        query = query.add_columns(
            sqla.null().cast(sqla.Numeric).label("score_recherche")
        )
    else:
        variants_vector = to_vector_with_variants(db_session=db_session, q=params.q)
        text_match = models.Service.search_vector.bool_op("@@")(variants_vector)
        score = sqla.func.ts_rank_cd(
            models.Service.search_vector,
            variants_vector,
            32,
        )
        if " " not in params.q.strip():  # single-word
            query = query.filter(text_match)
        else:  # multi-word: look for thematique aliases as well
            concatenated_vector = to_vector_concatenated(
                db_session=db_session, q=params.q
            )
            thematique_aliases_match = models.Service.search_vector.bool_op("@@")(
                concatenated_vector
            )
            query = query.filter(
                sqla.or_(
                    text_match,
                    thematique_aliases_match,
                )
            )
            score = sqla.func.greatest(
                score,
                sqla.func.ts_rank_cd(
                    models.Service.search_vector,
                    concatenated_vector,
                    32,
                ),
            )
        score_expr = sqla.func.round(
            sqla.cast(score, sqla.Numeric),
            2,
        ).label("score_recherche")
        score_recherche_expr = score_expr

        query = query.add_columns(score_expr)
        query = query.order_by((sqla.func.round(score_expr * 10) / 2).desc())

    query = query.order_by(models.Service.score_qualite.desc())

    if params.lat is not None and params.lon is not None:
        src_geometry = sqla.cast(
            geoalchemy2.functions.ST_MakePoint(
                models.Service.longitude, models.Service.latitude
            ),
            geoalchemy2.Geography(geometry_type="GEOMETRY", srid=4326),
        )
        dest_geometry = f"POINT({params.lon} {params.lat})"
        distance_km = (
            geoalchemy2.functions.ST_Distance(src_geometry, dest_geometry) / 1000
        )
        query = query.filter(
            geoalchemy2.functions.ST_DWithin(
                src_geometry, dest_geometry, params.distance * 1000
            )
        )
        query = query.add_columns(distance_km.cast(sqla.Integer).label("distance"))

        score_distance_expr = 1 - distance_km / params.distance
        if score_recherche_expr is not None:
            score_expr = score_recherche_expr * 0.5 + score_distance_expr * 0.5
        else:
            score_expr = score_distance_expr
        query = query.order_by(None).order_by(score_expr.desc())
    else:
        query = query.add_columns(sqla.null().cast(sqla.Integer).label("distance"))

    if params.exclure_doublons:
        cluster_key = sqla.func.coalesce(
            models.Structure._cluster_id,
            models.Structure.id,
        )
        structure_rank = (
            sqla.func.dense_rank()
            .over(
                partition_by=cluster_key,
                order_by=[
                    models.Structure.score_qualite.desc(),
                    models.Structure.date_maj.desc().nulls_last(),
                ],
            )
            .label("_structure_rank")
        )
        ranked_subq = query.add_columns(structure_rank).subquery()

        query = query.where(
            models.Service.id.in_(
                sqla.select(ranked_subq.c.id).where(ranked_subq.c._structure_rank == 1)
            )
        )

    return query, ("service", "score_recherche", "distance")


def build_search_index(
    db_session: orm.Session,
) -> None:
    db_session.execute(
        sqla.text(f"""
            WITH single_word_aliases AS (
                SELECT
                    thematique_code,
                    string_agg(alias, ' ') AS aliases
                FROM api__thematique_aliases_v1
                WHERE alias NOT LIKE '% %'
                GROUP BY thematique_code
            ),
            concatenated_aliases AS (
                SELECT
                    thematique_code,
                    string_agg(
                        {CONCATENATED_ALIAS_SQL},
                        ' '
                    ) AS aliases
                FROM api__thematique_aliases_v1
                WHERE alias LIKE '% %'
                GROUP BY thematique_code
            ),
            thematiques AS (
            SELECT
                api__services_v1.id AS service_id,
                STRING_AGG(
                    api__thematiques_v1.label
                    || ' '
                    || COALESCE(single_word_aliases.aliases, ''),
                    ', '
                ) AS labels,
                STRING_AGG(
                    COALESCE(concatenated_aliases.aliases, ''), ' '
                ) AS concatenated_aliases
            FROM api__services_v1,
                UNNEST(api__services_v1.thematiques) AS item
            INNER JOIN api__thematiques_v1
                ON api__thematiques_v1.value = item
            LEFT JOIN single_word_aliases
                ON single_word_aliases.thematique_code = item
            LEFT JOIN concatenated_aliases
                ON concatenated_aliases.thematique_code = item
            GROUP BY api__services_v1.id
        ),
        publics AS (
            SELECT
                api__services_v1.id AS service_id,
                STRING_AGG(api__publics_v1.label, ', ') AS labels
            FROM api__services_v1,
                UNNEST(api__services_v1.publics) AS item
            INNER JOIN api__publics_v1
                ON api__publics_v1.value = item
            GROUP BY api__services_v1.id
        ),
        reseaux_porteurs AS (
            SELECT
                api__structures_v1.id AS structure_id,
                STRING_AGG(api__reseaux_porteurs_v1.label, ', ') AS labels
            FROM api__structures_v1,
                UNNEST(api__structures_v1.reseaux_porteurs) AS item
            INNER JOIN api__reseaux_porteurs_v1
                ON api__reseaux_porteurs_v1.value = item
            GROUP BY api__structures_v1.id
        ),
        types AS (
            SELECT
                api__services_v1.id AS service_id,
                api__types_services_v1.label AS label
            FROM api__services_v1
            LEFT JOIN api__types_services_v1
                ON api__types_services_v1.value = api__services_v1.type
        )
        UPDATE api__services_v1
        SET search_vector =
            SETWEIGHT(TO_TSVECTOR('public.french', COALESCE(thematiques.labels,                  '')), 'A') ||
            SETWEIGHT(TO_TSVECTOR('simple', COALESCE(thematiques.concatenated_aliases,           '')), 'A') ||
            SETWEIGHT(TO_TSVECTOR('public.french', COALESCE(api__services_v1.nom,                '')), 'A') ||
            SETWEIGHT(TO_TSVECTOR('public.french', COALESCE(reseaux_porteurs.labels,             '')), 'B') ||
            SETWEIGHT(TO_TSVECTOR('public.french', COALESCE(api__structures_v1.nom,              '')), 'B') ||
            SETWEIGHT(TO_TSVECTOR('public.french', COALESCE(api__services_v1.description,        '')), 'B') ||
            SETWEIGHT(TO_TSVECTOR('public.french', COALESCE(api__structures_v1.description,      '')), 'B') ||
            SETWEIGHT(TO_TSVECTOR('public.french', COALESCE(publics.labels,                      '')), 'C') ||
            SETWEIGHT(TO_TSVECTOR('public.french', COALESCE(api__services_v1.publics_precisions, '')), 'C')
        FROM api__structures_v1
        LEFT JOIN thematiques ON TRUE
        LEFT JOIN publics ON TRUE
        LEFT JOIN reseaux_porteurs ON reseaux_porteurs.structure_id = api__structures_v1.id
        LEFT JOIN types ON TRUE
        WHERE api__services_v1.structure_id = api__structures_v1.id
            AND thematiques.service_id = api__services_v1.id
            AND publics.service_id = api__services_v1.id
            AND types.service_id = api__services_v1.id
    """)  # noqa: E501
    )

    # refill the lexeme/frequency lexicon
    db_session.execute(
        sqla.text(f"""
        TRUNCATE api__search_lexicon_v1;
        INSERT INTO api__search_lexicon_v1 (word, ndoc)
        SELECT word, ndoc
        FROM TS_STAT('SELECT search_vector FROM api__services_v1')
        WHERE word NOT LIKE '{CONCATENATED_TOKEN_PREFIX}%'
    """)
    )

    db_session.commit()
