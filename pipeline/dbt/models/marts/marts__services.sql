WITH services AS (
    SELECT * FROM {{ ref('int__services') }}
),

courriels_personnels AS (
    SELECT * FROM {{ ref('int__courriels_personnels') }}
),

criteres AS (
    SELECT * FROM {{ ref('int__criteres_qualite') }}
),

erreurs AS (
    SELECT * FROM {{ ref('int__erreurs_validation') }}
    WHERE resource_type = 'service'
),

scores AS (
    SELECT
        criteres.service_id                                                     AS "service_id",
        AVG(criteres.score_ligne) FILTER (WHERE criteres.schema_version = 'v0') AS "score_v0",
        AVG(criteres.score_ligne) FILTER (WHERE criteres.schema_version = 'v1') AS "score_v1"
    FROM criteres
    GROUP BY 1
),

final AS (
    SELECT
        {{
            dbt_utils.star(
                relation_alias='services',
                from=ref('int__services'),
                except=[
                    '_di_adresse_surrogate_id',
                    'adresse_id',
                ]
            )
        }},
        -- keep this column for backward compatibility
        -- TODO: remove after metabase migration on v1
        scores.score_v0                                 AS "score_qualite",
        scores.score_v0                                 AS "score_qualite_v0",
        scores.score_v1                                 AS "score_qualite_v1",
        courriels_personnels.courriel IS NOT NULL       AS "_has_pii",
        services.source NOT IN ('soliguide', 'agefiph') AS "_in_opendata",
        NOT EXISTS (
            SELECT
            FROM erreurs
            WHERE
                erreurs._di_surrogate_id = services._di_surrogate_id
                AND erreurs.schema_version = 'v0'
        )                                               AS "_is_valid_v0",
        NOT EXISTS (
            SELECT
            FROM erreurs
            WHERE
                erreurs._di_surrogate_id = services._di_surrogate_id
                AND erreurs.schema_version = 'v1'
        )                                               AS "_is_valid_v1"
    FROM services
    LEFT JOIN courriels_personnels ON services.courriel = courriels_personnels.courriel
    LEFT JOIN scores ON services._di_surrogate_id = scores.service_id
)

SELECT * FROM final
