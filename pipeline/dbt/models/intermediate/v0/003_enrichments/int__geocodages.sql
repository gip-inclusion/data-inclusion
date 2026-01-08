{{
    config(
        materialized="incremental",
        unique_key="adresse_id",
        post_hook=[
            "
            -- adresses can be deleted by the provider, or by us.
            -- in both cases, deleting the corresponding geocodages is required
            -- to ensure that the relationship between adresses and geocodages stands
            WITH deleted_adresses AS (
                SELECT geocodages.adresse_id
                FROM {{ this }} AS geocodages
                LEFT JOIN {{ ref('int__union_adresses') }} AS adresses ON geocodages.adresse_id = adresses._di_surrogate_id
                WHERE adresses._di_surrogate_id IS NULL
            )
            DELETE FROM {{ this }} WHERE {{ this }}.adresse_id IN (SELECT adresse_id FROM deleted_adresses)"
        ]
    )
}}

WITH adresses AS (
    SELECT * FROM {{ ref('int__union_adresses') }}
),

communes AS (
    SELECT * FROM {{ ref('stg_decoupage_administratif__communes') }}
),

codes_postaux AS (
    SELECT DISTINCT
        UNNEST(communes.codes_postaux)
            AS code_postal
    FROM communes
),

final AS (
    SELECT
        CAST('{{ run_started_at }}' AS TIMESTAMP) AS "geocoded_at",
        adresses._di_surrogate_id                 AS "adresse_id",
        adresses.adresse                          AS "input_adresse",
        adresses.code_postal                      AS "input_code_postal",
        adresses.code_insee                       AS "input_code_insee",
        adresses.commune                          AS "input_commune",
        geocodings.result_city                    AS "commune",
        geocodings.result_name                    AS "adresse",
        geocodings.result_postcode                AS "code_postal",
        -- ban api returns district codes for Paris, Lyon and Marseille
        -- replace them with actual city codes
        CASE
            WHEN LEFT(geocodings.result_citycode, 3) = '751' THEN '75056'  -- Paris
            WHEN LEFT(geocodings.result_citycode, 3) = '693' THEN '69123'  -- Lyon
            WHEN LEFT(geocodings.result_citycode, 3) = '132' THEN '13055'  -- Marseille
            ELSE geocodings.result_citycode
        END                                       AS "code_commune",
        CASE
            WHEN LEFT(geocodings.result_citycode, 3) = ANY(ARRAY['751', '693', '132'])
                THEN geocodings.result_citycode
        END                                       AS "code_arrondissement",
        geocodings.result_score                   AS "score",
        geocodings.result_type                    AS "type",
        geocodings.longitude                      AS "longitude",
        geocodings.latitude                       AS "latitude"
    FROM
        adresses
    INNER JOIN processings.geocode(
        (
            SELECT
                JSONB_AGG(
                    JSONB_OBJECT(
                        ARRAY[
                            'id', input_adresses._di_surrogate_id,
                            'adresse', input_adresses.adresse,
                            -- use the code postal if it exists
                            -- unfortunately, it's impossible to test in unit tests
                            'code_postal', COALESCE(codes_postaux.code_postal, ''),
                            'code_insee', input_adresses.code_insee,
                            'commune', input_adresses.commune
                        ]
                    )
                )
            FROM adresses AS input_adresses
            LEFT JOIN codes_postaux ON input_adresses.code_postal = codes_postaux.code_postal
            {% if is_incremental() %}
            -- then only geocode new or changed rows
                LEFT JOIN {{ this }} ON input_adresses._di_surrogate_id = {{ this }}.adresse_id
                WHERE
                    -- new rows
                    {{ this }}.adresse_id IS NULL
                    -- previously failed rows
                    OR {{ this }}.score IS NULL
                    -- score is low, and has not been checked recently
                    OR (
                        {{ this }}.score < 0.765
                        AND (
                            {{ this }}.geocoded_at IS NULL
                            OR {{ this }}.geocoded_at < (NOW() - INTERVAL '1 week')
                        )
                    )
                    -- changed rows
                    OR {{ this }}.input_adresse != input_adresses.adresse
                    OR {{ this }}.input_code_postal != input_adresses.code_postal
                    OR {{ this }}.input_code_insee != input_adresses.code_insee
                    OR {{ this }}.input_commune != input_adresses.commune
            {% endif %}
        )
    ) AS geocodings ON adresses._di_surrogate_id = geocodings.id
)

SELECT * FROM final
