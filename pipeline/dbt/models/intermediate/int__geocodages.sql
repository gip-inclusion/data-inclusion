{{
    config(
        materialized="incremental",
        unique_key="adresse_id",
    )
}}

WITH adresses AS (
    SELECT * FROM {{ ref('int__union_adresses') }}
),

final AS (
    SELECT
        CAST('{{ run_started_at }}' AS TIMESTAMP) AS "geocoded_at",
        adresses._di_surrogate_id                             AS "adresse_id",
        adresses.adresse                                      AS "input_adresse",
        adresses.code_postal                                  AS "input_code_postal",
        adresses.code_insee                                   AS "input_code_insee",
        adresses.commune                                      AS "input_commune",
        geocodings.result_city                                AS "commune",
        geocodings.result_name                                AS "adresse",
        geocodings.result_postcode                            AS "code_postal",
        geocodings.result_citycode                            AS "code_insee",
        geocodings.result_score                               AS "score",
        geocodings.result_type                                AS "type",
        geocodings.longitude                                  AS "longitude",
        geocodings.latitude                                   AS "latitude"
    FROM
        adresses
    INNER JOIN processings.geocode(
        (
            SELECT
                JSONB_AGG(
                    JSONB_OBJECT(
                        ARRAY[
                            'id', adresses._di_surrogate_id,
                            'adresse', adresses.adresse,
                            'code_postal', adresses.code_postal,
                            'code_insee', adresses.code_insee,
                            'commune', adresses.commune
                        ]
                    )
                )
            FROM adresses
            {% if is_incremental() %}
                -- then only geocode new or changed rows
                LEFT JOIN {{ this }} ON adresses._di_surrogate_id = {{ this }}.adresse_id
                WHERE
                    -- new rows
                    {{ this }}.adresse_id IS NULL
                    -- previously failed rows
                    OR {{ this }}.score IS NULL
                    -- changed rows
                    OR {{ this }}.input_adresse != adresses.adresse
                    OR {{ this }}.input_code_postal != adresses.code_postal
                    OR {{ this }}.input_code_insee != adresses.code_insee
                    OR {{ this }}.input_commune != adresses.commune
            {% endif %}
        )
    ) AS geocodings ON adresses._di_surrogate_id = geocodings.id
)

SELECT * FROM final
