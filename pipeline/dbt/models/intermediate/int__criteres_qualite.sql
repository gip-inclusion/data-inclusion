WITH services AS (
    SELECT * FROM {{ ref('int__services') }}
),

criteres_v0 AS (
    SELECT
        services._di_surrogate_id AS "service_id",
        scores.score_critere      AS "score_critere",
        scores.nom_critere        AS "nom_critere",
        scores.score_ligne        AS "score_ligne",
        'v0'                      AS "schema_version"
    FROM
        services,
        LATERAL (SELECT * FROM processings.score('v0', TO_JSONB(services))) AS scores
),

criteres_v1 AS (
    SELECT
        services._di_surrogate_id AS "service_id",
        scores.score_critere      AS "score_critere",
        scores.nom_critere        AS "nom_critere",
        scores.score_ligne        AS "score_ligne",
        'v1'                      AS "schema_version"
    FROM
        services,
        LATERAL (SELECT * FROM processings.score('v1', TO_JSONB(services))) AS scores
),

final AS (
    SELECT * FROM criteres_v0
    UNION ALL
    SELECT * FROM criteres_v1
)

SELECT * FROM final
