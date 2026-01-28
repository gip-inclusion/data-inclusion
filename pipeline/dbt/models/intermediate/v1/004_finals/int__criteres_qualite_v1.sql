WITH services AS (
    SELECT * FROM {{ ref('int__services_v1') }}
),

final AS (
    SELECT
        services.id          AS "service_id",
        scores.score_critere AS "score_critere",
        scores.nom_critere   AS "nom_critere",
        scores.score_ligne   AS "score_ligne",
        'v1'                 AS "schema_version"
    FROM
        services,
        LATERAL (SELECT * FROM processings.score(TO_JSONB(services))) AS scores
)

SELECT * FROM final
