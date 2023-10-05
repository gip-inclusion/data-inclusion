WITH source AS (
    SELECT *
    FROM {{ source('soliguide', 'lieux') }}
),

final AS (
    SELECT
        source._di_source_id                AS "_di_source_id",
        source.data ->> 'lieu_id'           AS "lieu_id",
        NULLIF(phones.data ->> 'label', '') AS "label",
        phones.data ->> 'phoneNumber'       AS "phone_number"
    FROM
        source,
        LATERAL(SELECT * FROM JSONB_PATH_QUERY(source.data, '$.entity.phones[*]')) AS phones (data)
)

SELECT * FROM final
