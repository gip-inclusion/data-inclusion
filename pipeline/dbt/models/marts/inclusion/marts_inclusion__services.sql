-- depends_on: {{ ref('marts_inclusion__structures') }}

WITH services AS (
    SELECT * FROM {{ ref('int__union_services__enhanced') }}
),

criteres AS (
    SELECT * FROM {{ ref('int__criteres_qualite') }}
),

scores AS (
    SELECT DISTINCT ON (1)
        criteres.service_id  AS "service_id",
        criteres.score_ligne AS "score"
    FROM criteres
),

final AS (
    SELECT
        {{
            dbt_utils.star(
                relation_alias='services',
                from=ref('int__union_services__enhanced'),
                except=[
                    '_di_adresse_surrogate_id',
                    'adresse_id',
                ]
            )
        }},
        scores.score AS "score_qualite"
    FROM services
    LEFT JOIN scores ON services._di_surrogate_id = scores.service_id
    -- TODO(vmttn): services that pass SQL validation, but fail pydantic validation
    -- don't have a score... scoring must be done on pydantic validated data
    -- this filter is a temporary workaround until validation is done consistently
    -- with pydantic
    WHERE scores.score IS NOT NULL
)

SELECT * FROM final
