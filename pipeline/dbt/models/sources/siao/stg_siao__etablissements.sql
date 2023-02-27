WITH source AS (
    SELECT *
    FROM {{ source('data_inclusion', 'datalake') }}
    WHERE
        logical_date = '{{ var('logical_date') }}'
        AND src_alias = 'siao'
),

final AS (
    SELECT
        -- there is no proper index in the data, this is very problematic.
        -- for analytical use, annotate with the row number if the default ordering.
        ROW_NUMBER() OVER ()                                                                      AS "id",
        NULLIF(NULLIF(REGEXP_REPLACE(data ->> 'Code SIRET', '\D', '', 'g'), REPEAT('0', 14)), '') AS "code_siret",
        data ->> 'Nom de la structure'                                                            AS "nom_de_la_structure",
        data ->> 'Ville'                                                                          AS "ville",
        NULLIF(LPAD(REGEXP_REPLACE(data ->> 'Code postal', '\D', '', 'g'), 5, '0'), '')           AS "code_postal",
        data ->> 'Adresse'                                                                        AS "adresse",
        data ->> 'Téléphone'                                                                      AS "telephone",
        data ->> 'Mail'                                                                           AS "mail"
    FROM source
)

SELECT * FROM final
