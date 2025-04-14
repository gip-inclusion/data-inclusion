WITH source AS (
    {{ stg_source_header('fredo', 'structures') }}
),

final AS (
    SELECT
        data ->> 'id'                                             AS "structure_id",
        REPLACE(LOWER(TRIM(frais)), 'rendez vous', 'rendez-vous') AS "value"
    FROM
        source,
        LATERAL UNNEST(STRING_TO_ARRAY(TRIM(TRIM(data ->> 'frais'), '/'), '/')) AS frais
)

SELECT * FROM final
