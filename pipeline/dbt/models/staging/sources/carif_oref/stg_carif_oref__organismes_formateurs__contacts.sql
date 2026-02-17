WITH source AS (
    SELECT * FROM {{ ref('_stg_carif_oref__source_filtered') }}
),

final AS (
    SELECT DISTINCT ON (1, 2, 3, 4)
        NULLIF(TRIM(actions.data ->> '@numero'), '')                  AS "numero_action",
        NULLIF(TRIM(organismes_formateurs.data ->> '@numero'), '')    AS "numero_organisme_formateur",
        CAST(MD5(contacts_formateurs.data ->> 'coordonnees') AS TEXT) AS "hash_coordonnees",
        CAST(contacts_formateurs.data ->> 'type-contact' AS INTEGER)  AS "type_contact"
    FROM
        source,
        JSONB_PATH_QUERY(source.data, '$.action[*]') AS actions (data),
        JSONB_PATH_QUERY(actions.data, '$.lieu\-de\-formation[*]'),
        JSONB_PATH_QUERY(actions.data, '$.organisme\-formateur[*]') AS organismes_formateurs (data),
        JSONB_PATH_QUERY(organismes_formateurs.data, '$.contact\-formateur[*]') AS contacts_formateurs (data)
    WHERE
        (contacts_formateurs.data ->> 'coordonnees') IS NOT NULL
)

SELECT * FROM final
