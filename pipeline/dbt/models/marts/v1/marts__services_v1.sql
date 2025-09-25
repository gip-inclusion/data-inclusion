WITH services AS (
    SELECT * FROM {{ ref('int__services_v1') }}
),

courriels_personnels AS (
    SELECT * FROM {{ ref('int__courriels_personnels_v1') }}
),

criteres AS (
    SELECT * FROM {{ ref('int__criteres_qualite_v1') }}
),

erreurs AS (
    SELECT DISTINCT id
    FROM {{ ref('int__erreurs_validation_v1') }}
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
                from=ref('int__services_v1'),
                except=[
                    'adresse_id',
                ]
            )
        }},
        scores.score                                    AS "score_qualite",
        courriels_personnels.courriel IS NOT NULL       AS "_has_pii",
        services.source NOT IN ('soliguide', 'agefiph') AS "_in_opendata",
        erreurs.id IS NULL                              AS "_is_valid"
    FROM services
    LEFT JOIN courriels_personnels ON services.courriel = courriels_personnels.courriel
    LEFT JOIN scores ON services.id = scores.service_id
    LEFT JOIN erreurs ON services.id = erreurs.id
)

SELECT * FROM final
