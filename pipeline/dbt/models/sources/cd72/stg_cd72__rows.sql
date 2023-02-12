WITH source AS (
    SELECT *
    FROM {{ source('data_inclusion', 'datalake') }}
    WHERE
        logical_date = '{{ var('logical_date') }}'
        AND src_alias = 'cd72'
),

final AS (
    SELECT
        data ->> 'ID Structure'        AS "id",
        data ->> 'ID Structure'        AS "id_structure",
        data ->> 'SIRET'               AS "siret",
        data ->> 'Nom Structure'       AS "nom_structure",
        data ->> 'Ville'               AS "ville",
        data ->> 'Code postal'         AS "code_postal",
        data ->> 'Adresse'             AS "adresse",
        data ->> 'Typologie Structure' AS "typologie_structure",
        data ->> 'Téléphone accueil'   AS "telephone_accueil",
        data ->> 'Téléphone principal' AS "telephone_principal",
        data ->> 'E-mail accueil'      AS "email_accueil",
        data ->> 'Site Internet'       AS "site_internet",
        data ->> 'Description'         AS "description",
        data ->> 'Mis à jour le :'     AS "mise_a_jour_le",
        data ->> 'Horaires'            AS "horaires"
    FROM source
)

SELECT * FROM final
