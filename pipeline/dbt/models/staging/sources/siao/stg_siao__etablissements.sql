WITH source AS (
    {{ stg_source_header('siao', 'etablissements') }}
),

final AS (
    SELECT
        _di_source_id                                                                             AS "_di_source_id",
        -- there is no proper index in the data, this is very problematic.
        -- for analytical use, annotate with the row number if the default ordering.
        CAST(ROW_NUMBER() OVER () AS TEXT)                                                        AS "id",
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
