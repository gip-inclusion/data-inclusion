WITH adresses AS (
    SELECT * FROM {{ ref('int__union_adresses_v1') }}
),

geocodages AS (
    SELECT * FROM {{ ref('int__geocodages_v1') }}
),

final AS (
    SELECT
        adresses.id                                            AS "id",
        adresses.complement_adresse                            AS "complement_adresse",
        CASE
            WHEN geocodages.type = 'municipality'
                THEN adresses.adresse
            ELSE COALESCE(geocodages.adresse, adresses.adresse)
        END                                                    AS "adresse",
        COALESCE(geocodages.longitude, adresses.longitude)     AS "longitude",
        COALESCE(geocodages.latitude, adresses.latitude)       AS "latitude",
        COALESCE(geocodages.commune, adresses.commune)         AS "commune",
        COALESCE(geocodages.code_postal, adresses.code_postal) AS "code_postal",
        COALESCE(geocodages.code_commune, adresses.code_insee) AS "code_insee",
        geocodages.score                                       AS "score_geocodage"
    FROM adresses
    LEFT JOIN geocodages
        ON
            adresses._di_surrogate_id = geocodages.adresse_id
            AND geocodages.score >= 0.8
)

SELECT * FROM final
