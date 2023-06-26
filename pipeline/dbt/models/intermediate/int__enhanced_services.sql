WITH services AS (
    SELECT * FROM {{ ref('int__services') }}
),

adresses_geocoded AS (
    SELECT * FROM {{ ref('int__adresses_geocoded') }}
),

final AS (
    SELECT
        services.*,
        adresses_geocoded.longitude          AS "longitude",
        adresses_geocoded.latitude           AS "latitude",
        adresses_geocoded.complement_adresse AS "complement_adresse",
        adresses_geocoded.commune            AS "commune",
        adresses_geocoded.adresse            AS "adresse",
        adresses_geocoded.code_postal        AS "code_postal",
        adresses_geocoded.code_insee         AS "code_insee",
        adresses_geocoded.result_score       AS "_di_geocodage_score",
        adresses_geocoded.result_citycode    AS "_di_geocodage_code_insee"
    FROM
        services
    LEFT JOIN adresses_geocoded ON services._di_adresse_surrogate_id = adresses_geocoded._di_surrogate_id
)

SELECT * FROM final
