{% set source_model = source('soliguide', 'lieux') %}

{% set table_exists = adapter.get_relation(database=source_model.database, schema=source_model.schema, identifier=source_model.name) is not none %}

{% if table_exists %}

    WITH source AS (
        SELECT * FROM {{ source_model }}
    ),

{% else %}

WITH source AS (
    SELECT
        NULL                AS "_di_source_id",
        CAST(NULL AS JSONB) AS "data"
    WHERE FALSE
),

{% endif %}

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
