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
        CAST('{{ run_started_at }}' AS TIMESTAMP)           AS "geocoded_at",
        adresses._di_surrogate_id                                       AS "adresse_id",
        ARRAY[adresses.adresse, adresses.code_postal, adresses.commune] AS "input",
        geocodings.result_city                                          AS "commune",
        geocodings.result_name                                          AS "adresse",
        geocodings.result_postcode                                      AS "code_postal",
        geocodings.result_citycode                                      AS "code_insee",
        geocodings.result_score                                         AS "score",
        geocodings.result_type                                          AS "type",
        geocodings.longitude                                            AS "longitude",
        geocodings.latitude                                             AS "latitude"
    FROM
        adresses
    INNER JOIN processings.geocode(
        (
            SELECT JSONB_AGG(adresses)  -- noqa: references.qualification
            FROM adresses
            {% if is_incremental() %}
                -- then only geocode new or changed rows
                LEFT JOIN {{ this }} ON adresses._di_surrogate_id = {{ this }}.adresse_id
                WHERE
                    {{ this }}.input IS NULL
                    OR {{ this }}.input != ARRAY[adresses.adresse, adresses.code_postal, adresses.commune]
            {% endif %}
        )
    ) AS geocodings ON adresses._di_surrogate_id = geocodings.adresse_id
)

SELECT * FROM final
