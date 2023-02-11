WITH source AS (
    SELECT *
    FROM {{ source('data_inclusion', 'datalake') }}
    WHERE
        logical_date = '{{ var('logical_date') }}'
        AND src_alias = 'siao'
),

final AS (
    SELECT
        NULL                           AS "id",
        data ->> 'Code SIRET'          AS "code_siret",
        data ->> 'Nom de la structure' AS "nom_de_la_structure",
        data ->> 'Ville'               AS "ville",
        data ->> 'Code postal'         AS "code_postal",
        data ->> 'Adresse'             AS "adresse",
        data ->> 'Téléphone'           AS "telephone",
        data ->> 'Mail'                AS "mail"
    FROM source
)

SELECT * FROM final
