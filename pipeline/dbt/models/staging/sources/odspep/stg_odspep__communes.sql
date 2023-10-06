{% set source_model = source('odspep', 'DD009_COMMUNE_RESSOURCE') %}

{% set table_exists = adapter.get_relation(database=source_model.database, schema=source_model.schema, identifier=source_model.name) is not none %}

{% if table_exists %}

    WITH source AS (
        SELECT * FROM {{ source_model }}
    ),

    communes AS (
        SELECT * FROM {{ source('insee', 'communes') }}
    ),

    -- exclude communes déléguées (duplicated codes)
    filtered_communes AS (
        SELECT *
        FROM communes
        WHERE "TYPECOM" != 'COMD'
    ),

    final AS (
        SELECT
            source."ID_COM"             AS "id",
            source."ID_COM"             AS "id_com",
            source."ID_RES"             AS "id_res",
            source."CODE_COMMUNE_COM"   AS "code_commune_com",
            filtered_communes."LIBELLE" AS "libelle"
        FROM source
        LEFT JOIN filtered_communes ON source."CODE_COMMUNE_COM" = filtered_communes."COM"
    )

    SELECT * FROM final

{% else %}

SELECT
    NULL AS "id",
    NULL AS "id_com",
    NULL AS "id_res",
    NULL AS "code_commune_com",
    NULL AS "libelle"
WHERE FALSE

{% endif %}
