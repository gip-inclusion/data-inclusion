WITH adresses AS (
    SELECT * FROM {{ ref('int__union_adresses') }}
),

valid_adresses AS (
    SELECT adresses.*
    FROM adresses
    LEFT JOIN
        LATERAL
        LIST_ADRESSE_ERRORS(
            adresses.adresse,
            adresses.code_insee,
            adresses.code_postal,
            adresses.commune,
            adresses.complement_adresse,
            adresses.id,
            adresses.latitude,
            adresses.longitude,
            adresses.source
        ) AS errors ON TRUE
    WHERE errors.field IS NULL
),

geocoded_results AS (
    SELECT * FROM {{ ref('int_extra__geocoded_results') }}
),

final AS (
    SELECT
        valid_adresses._di_surrogate_id                                        AS "_di_surrogate_id",
        valid_adresses.id                                                      AS "id",
        valid_adresses.source                                                  AS "source",
        valid_adresses.complement_adresse                                      AS "complement_adresse",
        CASE
            WHEN geocoded_results.result_type = 'municipality'
                THEN valid_adresses.adresse
            ELSE COALESCE(geocoded_results.result_name, valid_adresses.adresse)
        END                                                                    AS "adresse",
        COALESCE(geocoded_results.longitude, valid_adresses.longitude)         AS "longitude",
        COALESCE(geocoded_results.latitude, valid_adresses.latitude)           AS "latitude",
        COALESCE(geocoded_results.result_city, valid_adresses.commune)         AS "commune",
        COALESCE(geocoded_results.result_postcode, valid_adresses.code_postal) AS "code_postal",
        COALESCE(geocoded_results.result_citycode, valid_adresses.code_insee)  AS "code_insee",
        geocoded_results.result_score                                          AS "result_score"
    FROM valid_adresses
    LEFT JOIN geocoded_results
        ON
            valid_adresses._di_surrogate_id = geocoded_results._di_surrogate_id
            AND geocoded_results.result_score >= 0.8
)

SELECT * FROM final
