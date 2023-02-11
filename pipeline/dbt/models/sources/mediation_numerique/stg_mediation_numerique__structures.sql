WITH source AS (
    SELECT *
    FROM {{ source('data_inclusion', 'datalake') }}
    WHERE
        logical_date = '{{ var('logical_date') }}'
        AND src_alias ~ 'mednum'
        AND file ~ 'structures'
),

final AS (
    SELECT
        ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(NULLIF(data -> 'thematiques', 'null')))::TEXT[] AS "thematiques",
        (data ->> 'longitude')::FLOAT                                                                 AS "longitude",
        (data ->> 'latitude')::FLOAT                                                                  AS "latitude",
        (data ->> 'date_maj')::TIMESTAMP WITH TIME ZONE                                               AS "date_maj",
        data ->> 'id'                                                                                 AS "id",
        data ->> 'nom'                                                                                AS "nom",
        data ->> 'siret'                                                                              AS "siret",
        data ->> 'source'                                                                             AS "source",
        data ->> 'adresse'                                                                            AS "adresse",
        data ->> 'commune'                                                                            AS "commune",
        data ->> 'courriel'                                                                           AS "courriel",
        data ->> 'site_web'                                                                           AS "site_web",
        data ->> 'telephone'                                                                          AS "telephone",
        data ->> 'code_postal'                                                                        AS "code_postal",
        data ->> 'horaires_ouverture'                                                                 AS "horaires_ouverture"
    FROM source
)

SELECT * FROM final
