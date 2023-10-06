{% set source_model = source('cd35', 'organisations') %}

{% set table_exists = adapter.get_relation(database=source_model.database, schema=source_model.schema, identifier=source_model.name) is not none %}

{% if table_exists %}

    WITH source AS (
        SELECT * FROM {{ source_model }}
    ),

{% else %}

WITH source AS (
    SELECT
        NULL                AS "_di_source_id",
        CAST(NULL AS JSONB) AS "data"
    WHERE FALSE
),

{% endif %}

final AS (
    SELECT
        _di_source_id                                                                                AS "_di_source_id",
        (data ->> 'LATITUDE')::FLOAT                                                                 AS "latitude",
        (data ->> 'LONGITUDE')::FLOAT                                                                AS "longitude",
        data ->> 'ADRESSE'                                                                           AS "adresse",
        data ->> 'CODE_INSEE'                                                                        AS "code_insee",
        data ->> 'CODE_POSTAL'                                                                       AS "code_postal",
        data ->> 'COMMUNE'                                                                           AS "commune",
        data ->> 'COMPLEMENT_ADRESSE'                                                                AS "complement_adresse",
        data ->> 'COURRIEL'                                                                          AS "courriel",
        TO_DATE(data ->> 'DATE_CREATION', 'DD-MM-YYYY')                                              AS "date_creation",
        TO_DATE(data ->> 'DATE_MAJ', 'DD-MM-YYYY')                                                   AS "date_maj",
        data ->> 'HORAIRES_OUVERTURES'                                                               AS "horaires_ouvertures",
        data ->> 'ID'                                                                                AS "id",
        data ->> 'LIEN_SOURCE'                                                                       AS "lien_source",
        data ->> 'NOM'                                                                               AS "nom",
        data ->> 'PRESENTATION_DETAIL'                                                               AS "presentation_detail",
        (SELECT ARRAY_AGG(TRIM(p)) FROM UNNEST(STRING_TO_ARRAY(data ->> 'PROFILS', ',')) AS "p")     AS "profils",
        data ->> 'SIGLE'                                                                             AS "sigle",
        data ->> 'SITE_WEB'                                                                          AS "site_web",
        data ->> 'TELEPHONE'                                                                         AS "telephone",
        (SELECT ARRAY_AGG(TRIM(t)) FROM UNNEST(STRING_TO_ARRAY(data ->> 'THEMATIQUES', ',')) AS "t") AS "thematiques"
    FROM source
)

SELECT * FROM final
