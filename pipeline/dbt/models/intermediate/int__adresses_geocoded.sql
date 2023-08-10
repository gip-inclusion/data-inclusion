WITH adresses AS (
    SELECT * FROM {{ ref('int__adresses') }}
),

geocoded_results AS (
    SELECT * FROM {{ ref('int_extra__geocoded_results') }}
),

final AS (
    SELECT
        {{
            dbt_utils.star(
                relation_alias='adresses',
                from=ref('int__adresses'),
                except=['longitude', 'latitude'])
        }},
        geocoded_results.result_score,
        geocoded_results.result_citycode,
        COALESCE(adresses.longitude, geocoded_results.longitude) AS "longitude",
        COALESCE(adresses.latitude, geocoded_results.latitude)   AS "latitude"
    FROM adresses
    LEFT JOIN geocoded_results ON adresses._di_surrogate_id = geocoded_results._di_surrogate_id
)

SELECT * FROM final
