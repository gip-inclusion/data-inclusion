{% set source_model = source('cd72', 'structures') %}

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
        _di_source_id                     AS "_di_source_id",
        data ->> 'id'                     AS "id",
        data ->> 'nom'                    AS "nom",
        data ->> 'siret'                  AS "siret",
        data ->> 'adresse'                AS "adresse",
        data ->> 'commune'                AS "commune",
        data ->> 'courriel'               AS "courriel",
        CAST(data ->> 'date_maj' AS DATE) AS "date_maj",
        data ->> 'site_web'               AS "site_web",
        data ->> 'telephone'              AS "telephone",
        data ->> 'typologie'              AS "typologie",
        data ->> 'code_postal'            AS "code_postal",
        data ->> 'horaires_ouverture'     AS "horaires_ouverture",
        data ->> 'presentation_detail'    AS "presentation_detail"
    FROM source
)

SELECT * FROM final
