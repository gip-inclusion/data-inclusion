WITH services AS (
    SELECT * FROM {{ ref('int__services') }}
),

structures AS (
    SELECT * FROM {{ ref('marts_inclusion__structures', version=1) }}
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
    WHERE resource_type = 'service' AND schema_version = 'v0'
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
                from=ref('int__services'),
                except=[
                    '_di_adresse_surrogate_id',
                    'adresse_id',
                ]
            )
        }},
        scores.score                                    AS "score_qualite",
        courriels_personnels.courriel IS NOT NULL       AS "_has_pii",
        services.source NOT IN ('soliguide', 'agefiph') AS "_in_opendata"
    FROM services
    LEFT JOIN courriels_personnels ON services.courriel = courriels_personnels.courriel
    LEFT JOIN scores ON services._di_surrogate_id = scores.service_id
    LEFT JOIN erreurs ON services._di_surrogate_id = erreurs._di_surrogate_id
    LEFT JOIN structures ON services._di_structure_surrogate_id = structures._di_surrogate_id
    WHERE
        -- TODO(vmttn): services that pass SQL validation, but fail pydantic validation
        -- don't have a score... scoring must be done on pydantic validated data
        -- this filter is a temporary workaround until validation is done consistently
        -- with pydantic
        scores.score IS NOT NULL
        AND erreurs._di_surrogate_id IS NULL
        AND structures._di_surrogate_id IS NOT NULL
)

SELECT * FROM final
