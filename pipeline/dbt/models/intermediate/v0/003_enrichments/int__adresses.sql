WITH adresses AS (
    SELECT * FROM {{ ref('int__union_adresses') }}
),

geocodages AS (
    SELECT * FROM {{ ref('int__geocodages') }}
),

final AS (
    SELECT
        adresses._di_surrogate_id   AS "_di_surrogate_id",
        adresses.id                 AS "id",
        adresses.source             AS "source",
        adresses.complement_adresse AS "complement_adresse",
        CASE
            WHEN geocodages.type = 'municipality'
                THEN adresses.adresse
            ELSE geocodages.adresse
        END                         AS "adresse",
        geocodages.longitude        AS "longitude",
        geocodages.latitude         AS "latitude",
        geocodages.commune          AS "commune",
        geocodages.code_postal      AS "code_postal",
        geocodages.code_commune     AS "code_insee",
        geocodages.score            AS "score_geocodage"
    FROM adresses
    LEFT JOIN geocodages
        ON adresses._di_surrogate_id = geocodages.adresse_id
    WHERE geocodages.score > 0.75
)

SELECT * FROM final
