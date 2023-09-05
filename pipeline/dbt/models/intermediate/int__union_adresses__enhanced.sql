WITH adresses AS (
    SELECT * FROM {{ ref('int__union_adresses') }}
),

valid_adresses AS (
    SELECT adresses.*
    FROM adresses
    LEFT JOIN LATERAL
        LIST_ADRESSE_ERRORS(
            adresse,
            code_insee,
            code_postal,
            commune,
            complement_adresse,
            id,
            latitude,
            longitude,
            source
        ) AS errors ON TRUE
    WHERE errors.field IS NULL
),

geocoded_results AS (
    SELECT * FROM {{ ref('int_extra__geocoded_results') }}
),

final AS (
    SELECT
        {{
            dbt_utils.star(
                relation_alias='valid_adresses',
                from=ref('int__union_adresses'),
                except=['longitude', 'latitude'])
        }},
        geocoded_results.result_score,
        geocoded_results.result_citycode,
        COALESCE(valid_adresses.longitude, geocoded_results.longitude) AS "longitude",
        COALESCE(valid_adresses.latitude, geocoded_results.latitude)   AS "latitude"
    FROM valid_adresses
    LEFT JOIN geocoded_results ON valid_adresses._di_surrogate_id = geocoded_results._di_surrogate_id
)

SELECT * FROM final
