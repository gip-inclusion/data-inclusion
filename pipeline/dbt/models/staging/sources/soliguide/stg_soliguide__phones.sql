WITH source AS (
    {{ stg_source_header('soliguide', 'lieux') }}
),

phones AS (
    SELECT
        source._di_source_id                      AS "_di_source_id",
        source.data ->> 'lieu_id'                 AS "lieu_id",
        NULLIF(inline_phone.data ->> 'label', '') AS "label",
        inline_phone.data ->> 'phoneNumber'       AS "phone_number",
        source.data -> 'sources'                  AS "sources"
    FROM
        source,
        LATERAL (SELECT * FROM JSONB_PATH_QUERY(source.data, '$.entity.phones[*]')) AS inline_phone (data)
),

final AS (
    SELECT *
    FROM phones
    WHERE NOT sources @> '[{"name": "dora"}]'
)

SELECT * FROM final
