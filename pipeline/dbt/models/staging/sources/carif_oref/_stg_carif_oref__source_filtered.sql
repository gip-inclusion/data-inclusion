WITH source AS (
    {{ stg_source_header('carif_oref', 'formations') }}),

domaines AS (
    SELECT * FROM {{ ref('stg_carif_oref__formacode_v14__domaines') }}
),

final AS (
    SELECT *
    FROM source
    WHERE
        -- keep only formations that have at least one of the following FORMACODE codes
        ARRAY(
            SELECT codes.data ->> '$'
            FROM JSONB_ARRAY_ELEMENTS(source.data -> 'domaine-formation' -> 'code-FORMACODE') AS codes (data)
        )
        &&
        ARRAY(
            SELECT domaines.code
            FROM domaines
            WHERE domaines.selected
        )
)

SELECT * FROM final
