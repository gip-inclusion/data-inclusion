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
    SELECT DISTINCT _di_surrogate_id
    FROM {{ ref('int__erreurs_validation') }}
    WHERE resource_type = 'service'
),

scores AS (
    SELECT
        criteres.service_id       AS "service_id",
        AVG(criteres.score_ligne) AS "score"
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
        scores.score                                    AS "score_qualite",
        courriels_personnels.courriel IS NOT NULL       AS "_has_pii",
        services.source NOT IN ('soliguide', 'agefiph') AS "_in_opendata",
        erreurs._di_surrogate_id IS NULL                AS "_is_valid",
        -- the following is kept for retrocompatibility
        CAST(NULL AS BOOLEAN)                           AS "contact_public",
        CAST(NULL AS BOOLEAN)                           AS "cumulable",
        CAST(NULL AS DATE)                              AS "date_creation",
        CAST(NULL AS DATE)                              AS "date_suspension"
    FROM services
    LEFT JOIN courriels_personnels ON services.courriel = courriels_personnels.courriel
    LEFT JOIN scores ON services._di_surrogate_id = scores.service_id
    LEFT JOIN erreurs ON services._di_surrogate_id = erreurs._di_surrogate_id
)

SELECT * FROM final
