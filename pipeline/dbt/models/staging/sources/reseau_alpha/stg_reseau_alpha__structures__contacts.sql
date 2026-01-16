WITH source AS (
    {{ stg_source_header('reseau_alpha', 'structures') }}
),

final AS (
    SELECT
        NULLIF(TRIM(source.data ->> 'id'), '')           AS "structure_id",
        NULLIF(TRIM(contacts.data ->> 'nom'), '')        AS "nom",
        NULLIF(TRIM(contacts.data ->> 'email'), '')      AS "email",
        NULLIF(TRIM(contacts.data ->> 'prenom'), '')     AS "prenom",
        NULLIF(TRIM(contacts.data ->> 'civilite'), '')   AS "civilite",
        NULLIF(TRIM(contacts.data ->> 'telephone1'), '') AS "telephone1",
        NULLIF(TRIM(contacts.data ->> 'telephone2'), '') AS "telephone2"
    FROM
        source,
        JSONB_PATH_QUERY(source.data, '$.lieux.contacts[*]') AS contacts (data)
)

SELECT * FROM final
