WITH source AS (
    SELECT * FROM {{ ref('_stg_carif_oref__source_filtered') }}
),

final AS (
    SELECT DISTINCT ON (1, 2, 3)
        NULLIF(TRIM(source.data ->> '@numero'), '')                   AS "numero_formation",
        CAST(MD5(contacts_formations.data ->> 'coordonnees') AS TEXT) AS "hash_coordonnees",
        CAST(contacts_formations.data ->> 'type-contact' AS INTEGER)  AS "type_contact"
    FROM
        source,  -- noqa: structure.unused_join
        JSONB_PATH_QUERY(source.data, '$.contact\-formation[*]') AS contacts_formations (data)
    WHERE
        (contacts_formations.data ->> 'coordonnees') IS NOT NULL
)

SELECT * FROM final
