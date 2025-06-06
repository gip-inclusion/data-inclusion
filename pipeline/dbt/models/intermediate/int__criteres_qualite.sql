WITH services AS (
    SELECT * FROM {{ ref('int__services') }}
),

final AS (
    SELECT
        services._di_surrogate_id AS "service_id",
        scores.score_critere      AS "score_critere",
        scores.nom_critere        AS "nom_critere",
        scores.score_ligne        AS "score_ligne"
    FROM
        services,
        LATERAL (SELECT * FROM processings.score(TO_JSONB(services))) AS scores
)

SELECT * FROM final
