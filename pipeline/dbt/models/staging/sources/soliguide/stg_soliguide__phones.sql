WITH source AS (
    {{ stg_source_header('soliguide', 'lieux') }}),

lieux AS (
    SELECT * FROM {{ ref('stg_soliguide__lieux') }}
),

phones AS (
    SELECT
        source.data ->> 'lieu_id'                 AS "lieu_id",
        NULLIF(inline_phone.data ->> 'label', '') AS "label",
        CASE
            WHEN inline_phone.data ->> 'phoneNumber' ~ '^[1-9]\d{8}$'
                THEN '0' || inline_phone.data ->> 'phoneNumber'
            ELSE inline_phone.data ->> 'phoneNumber'
        END                                       AS "phone_number",
        source.data -> 'sources'                  AS "sources"
    FROM
        source,
        LATERAL JSONB_PATH_QUERY(source.data, '$.entity.phones[*]') AS inline_phone (data)
),

final AS (
    SELECT phones.*
    FROM phones
    INNER JOIN lieux ON phones.lieu_id = lieux.lieu_id
)

SELECT * FROM final
