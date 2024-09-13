WITH adresses AS (
    SELECT * FROM {{ ref('int__union_adresses') }}
),

geocodages AS (
    SELECT * FROM {{ ref('int__geocodages') }}
),

overriden_adresses AS (
    SELECT
        adresses._di_surrogate_id                              AS "_di_surrogate_id",
        adresses.id                                            AS "id",
        adresses.source                                        AS "source",
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
        COALESCE(geocodages.code_insee, adresses.code_insee)   AS "code_insee",
        geocodages.score                                       AS "score_geocodage"
    FROM adresses
    LEFT JOIN geocodages
        ON
            adresses._di_surrogate_id = geocodages.adresse_id
            AND geocodages.score >= 0.8
),

final AS (
    SELECT overriden_adresses.*
    FROM overriden_adresses
    LEFT JOIN
        LATERAL
        LIST_ADRESSE_ERRORS(
            overriden_adresses.adresse,
            overriden_adresses.code_insee,
            overriden_adresses.code_postal,
            overriden_adresses.commune,
            overriden_adresses.complement_adresse,
            overriden_adresses.id,
            overriden_adresses.latitude,
            overriden_adresses.longitude,
            overriden_adresses.source
        ) AS errors ON TRUE
    WHERE errors.field IS NULL
)

SELECT * FROM final
