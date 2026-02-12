WITH adresses AS (
    SELECT * FROM {{ ref('int__union_adresses_v1') }}
),

geocodages AS (
    SELECT * FROM {{ ref('int__geocodages_v1') }}
),

communes AS (
    SELECT * FROM {{ ref('stg_decoupage_administratif__communes') }}
),

adresses_geocodees AS (
    SELECT
        adresses.id                 AS "id",
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
        ON adresses.id = geocodages.adresse_id
    WHERE geocodages.score > 0.75
),

adresses_originales AS (
    SELECT
        adresses.id                 AS "id",
        adresses.complement_adresse AS "complement_adresse",
        adresses.adresse            AS "adresse",
        adresses.longitude          AS "longitude",
        adresses.latitude           AS "latitude",
        adresses.commune            AS "commune",
        adresses.code_postal        AS "code_postal",
        adresses.code_insee         AS "code_insee",
        CAST(NULL AS FLOAT)         AS "score_geocodage"
    FROM adresses
    WHERE SPLIT_PART(adresses.id, '--', 1) = 'dora'
),

adresses_finales AS (
    SELECT DISTINCT ON (id) *
    FROM (
        SELECT * FROM adresses_geocodees
        UNION ALL
        SELECT * FROM adresses_originales
    )
    ORDER BY
        id ASC,
        score_geocodage DESC NULLS LAST
),

final AS (
    SELECT
        adresses_finales.*,
        communes.code_departement,
        communes.code_region,
        communes.code_epci
    FROM adresses_finales
    LEFT JOIN communes ON adresses_finales.code_insee = communes.code
)

SELECT * FROM final
