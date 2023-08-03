WITH source AS (
    SELECT * FROM {{ source('agefiph', 'services') }}
),

final AS (
    SELECT
        _di_source_id                                                                          AS "_di_source_id",
        data ->> 'structure_id'                                                                AS "structure_id",
        data ->> 'courriel'                                                                    AS "courriel",
        data ->> 'telephone'                                                                   AS "telephone",
        data ->> 'adresse'                                                                     AS "adresse",
        data ->> 'complement_adresse'                                                          AS "complement_adresse",
        data ->> 'commune'                                                                     AS "commune",
        data ->> 'code_postal'                                                                 AS "code_postal",
        data ->> 'code_insee'                                                                  AS "code_insee",
        data ->> 'id'                                                                          AS "id",
        CAST(data ->> 'date_creation' AS DATE)                                                 AS "date_creation",
        CAST(data ->> 'date_maj' AS DATE)                                                      AS "date_maj",
        data ->> 'nom'                                                                         AS "nom",
        data ->> 'presentation_resume'                                                         AS "presentation_resume",
        data ->> 'presentation_detail'                                                         AS "presentation_detail",
        CAST(data ->> 'contact_public' AS BOOLEAN)                                             AS "contact_public",
        CAST(ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(data -> 'thematiques')) AS TEXT []) AS "thematiques"
    FROM source
)

SELECT * FROM final
