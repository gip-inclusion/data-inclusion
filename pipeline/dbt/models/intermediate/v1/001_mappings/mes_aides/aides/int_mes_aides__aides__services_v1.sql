WITH aides AS (
    SELECT * FROM {{ ref('stg_mes_aides__aides') }}
),

telephones AS (
    SELECT DISTINCT ON (aide_id)
        aide_id,
        value
    FROM {{ ref('stg_mes_aides__aides__telephones') }}
    ORDER BY aide_id ASC, index ASC
),

emails AS (
    SELECT DISTINCT ON (aide_id)
        aide_id,
        value
    FROM {{ ref('stg_mes_aides__aides__emails') }}
    ORDER BY aide_id ASC, index ASC
),

regions AS (
    SELECT
        regions.aide_id,
        ARRAY_AGG(departements.code) AS "codes_departements"
    FROM {{ ref('stg_mes_aides__aides__regions') }} AS regions
    LEFT JOIN {{ ref('stg_decoupage_administratif__departements') }} AS departements
        ON regions.code = departements.code_region
    GROUP BY regions.aide_id
),

departements AS (
    SELECT
        aide_id,
        ARRAY_AGG(code) AS "codes"
    FROM {{ ref('stg_mes_aides__aides__departements') }}
    GROUP BY aide_id
),

epcis AS (
    SELECT
        aide_id,
        ARRAY_AGG(code) AS "codes"
    FROM {{ ref('stg_mes_aides__aides__epcis') }}
    GROUP BY aide_id
),

villes AS (
    SELECT
        aide_id,
        ARRAY_AGG(code) AS "codes"
    FROM {{ ref('stg_mes_aides__aides__villes') }}
    WHERE code IS NOT NULL
    GROUP BY aide_id
),

conditions AS (
    SELECT
        *,
        BOOL_OR(value ~* 'tou. public') OVER (PARTITION BY aide_id) AS "aide_tous_publics"
    FROM {{ ref('stg_mes_aides__aides__conditions') }}
),

publics AS (
    SELECT
        aide_id,
        NULLIF(
            ARRAY_REMOVE(
                ARRAY_AGG(
                    DISTINCT
                    CASE
                        WHEN value ~* 'tou. public'
                            THEN 'tous-publics'
                        WHEN value ~* 'inscrit france travail|insertion professionnelle|en recherche d''emploi|etre inscrit en mission locale'
                            THEN 'demandeurs-emploi'
                        WHEN value ~* 'allocataire rsa|beneficiaire des minimas sociaux'
                            THEN 'beneficiaires-des-minimas-sociaux'
                        WHEN value ~* 'en situation de handicap'
                            THEN 'personnes-en-situation-de-handicap'
                        WHEN value ~* 'entre \d{2} et [1-3]\d ans|moins de [2-3]\d ans|de \d{2} a [1-3]\d ans'
                            THEN 'jeunes'
                        WHEN value ~* 'plus de [5-6]\d ans'
                            THEN 'seniors'
                        WHEN value ~* 'etudiant|stagiaire|lyceen|apprenti|alternant|cfa'
                            THEN 'etudiants'
                        WHEN value ~* 'interimaire|saisonnier|alternant'
                            THEN 'actifs'
                    END
                ),
                NULL
            ),
            '{}'
        )                                       AS "publics",
        ARRAY_TO_STRING(ARRAY_AGG(value), ', ') AS "publics_precisions"
    FROM conditions
    WHERE NOT aide_tous_publics OR value ~* 'tou. public'
    GROUP BY aide_id
),

frais AS (
    SELECT DISTINCT ON (natures.aide_id)
        natures.aide_id,
        mapping_.frais
    FROM {{ ref('stg_mes_aides__aides__natures') }} AS natures
    LEFT JOIN {{ ref('_map_mes_aides__frais_v1') }} AS mapping_ ON natures.value = mapping_.nature
    ORDER BY
        natures.aide_id,
        mapping_.frais = 'payant' DESC
),

types AS (
    SELECT DISTINCT ON (types_aides.aide_id)
        types_aides.aide_id,
        mapping_.type AS "type"
    FROM {{ ref('stg_mes_aides__aides__types_aides') }} AS types_aides
    LEFT JOIN {{ ref('_map_mes_aides__types_v1') }} AS mapping_ ON types_aides.value = mapping_.type_aide
    ORDER BY
        types_aides.aide_id,
        mapping_.type = 'aide-financiere' DESC,
        mapping_.type = 'aide-materielle' DESC,
        mapping_.type = 'accompagnement' DESC

),

thematiques AS (
    SELECT
        besoins.aide_id,
        ARRAY_REMOVE(ARRAY_AGG(DISTINCT mapping_.thematique), NULL) AS "thematiques"
    FROM {{ ref('stg_mes_aides__aides__besoins') }} AS besoins
    LEFT JOIN {{ ref('_map_mes_aides__thematiques_v1') }} AS mapping_ ON besoins.value = mapping_.besoin
    GROUP BY besoins.aide_id
),

justificatifs AS (
    SELECT
        aide_id,
        ARRAY_TO_STRING(ARRAY_AGG(value), ', ') AS "justificatifs"
    FROM {{ ref('stg_mes_aides__aides__justificatifs') }}
    GROUP BY aide_id
),

final AS (
    SELECT
        'mes-aides'                                       AS "source",
        'mes-aides--' || aides.id                         AS "id",
        NULL                                              AS "adresse_id",
        'mes-aides--' || aides.id                         AS "structure_id",
        aides.nom                                         AS "nom",
        NULLIF(ARRAY_TO_STRING(ARRAY[
            aides.description,
            '## Montant' || E'\n\n' || FORMAT('%s €', aides.montant),
            '## Bon à savoir' || E'\n\n' || aides.bon_a_savoir
        ], E'\n\n'), '')                                  AS "description",
        aides.voir_l_aide                                 AS "lien_source",
        COALESCE(aides.mise_a_jour_le, aides.modifiee_le) AS "date_maj",
        types.type                                        AS "type",
        thematiques.thematiques                           AS "thematiques",
        frais.frais                                       AS "frais",
        NULL                                              AS "frais_precisions",
        publics.publics                                   AS "publics",
        publics.publics_precisions                        AS "publics_precisions",
        ARRAY_TO_STRING(
            ARRAY[
                'Autres conditions : ' || aides.autres_conditions,
                'Justificatifs : ' || justificatifs.justificatifs,
                'Autres justificatifs : ' || aides.autres_justificatifs
            ],
            E'\n\n'
        )                                                 AS "conditions_acces",
        telephones.value                                  AS "telephone",
        emails.value                                      AS "courriel",
        NULL                                              AS "contact_nom_prenom",
        ARRAY['a-distance']                               AS "modes_accueil",
        CASE
            WHEN aides.zone_geographique ~ 'commun.' THEN villes.codes
            WHEN aides.zone_geographique ~ 'intercommun.' THEN epcis.codes
            WHEN aides.zone_geographique ~ 'region' THEN regions.codes_departements
            WHEN aides.zone_geographique ~ 'departement' THEN departements.codes
            WHEN aides.zone_geographique ~ 'france' THEN ARRAY['france']
            WHEN aides.zone_geographique ~ 'national' THEN ARRAY['france']
        END                                               AS "zone_eligibilite",
        aides.formulaire_url                              AS "lien_mobilisation",
        NULLIF(ARRAY_REMOVE(
            ARRAY[
                CASE WHEN aides.formulaire_url IS NOT NULL THEN 'utiliser-lien-mobilisation' END,
                CASE WHEN telephones.value IS NOT NULL THEN 'telephoner' END,
                CASE WHEN emails.value IS NOT NULL THEN 'envoyer-un-courriel' END
            ],
            NULL
        ), '{}')                                          AS "modes_mobilisation",
        ARRAY['usagers', 'professionnels']                AS "mobilisable_par",
        aides.demarches                                   AS "mobilisation_precisions",
        NULL                                              AS "volume_horaire_hebdomadaire",
        NULL                                              AS "nombre_semaines",
        NULL                                              AS "horaires_accueil"
    FROM aides
    LEFT JOIN telephones ON aides.id = telephones.aide_id
    LEFT JOIN emails ON aides.id = emails.aide_id
    LEFT JOIN publics ON aides.id = publics.aide_id
    LEFT JOIN frais ON aides.id = frais.aide_id
    LEFT JOIN types ON aides.id = types.aide_id
    LEFT JOIN thematiques ON aides.id = thematiques.aide_id
    LEFT JOIN regions ON aides.id = regions.aide_id
    LEFT JOIN departements ON aides.id = departements.aide_id
    LEFT JOIN epcis ON aides.id = epcis.aide_id
    LEFT JOIN villes ON aides.id = villes.aide_id
    LEFT JOIN justificatifs ON aides.id = justificatifs.aide_id
    WHERE aides.en_ligne
)

SELECT * FROM final
