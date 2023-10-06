{% set source_model = source('odspep', 'DD009_BASSIN_RESSOURCE') %}

{% set table_exists = adapter.get_relation(database=source_model.database, schema=source_model.schema, identifier=source_model.name) is not none %}

{% if table_exists %}

    WITH source AS (
        SELECT * FROM {{ source_model }}
    ),

    final AS (
        SELECT
            "ID_BAS"          AS "id",
            "ID_BAS"          AS "id_bas",
            "ID_RES"          AS "id_res",
            "CODE_BASSIN_BAS" AS "code_bassin_bas"
        FROM source
    )

    SELECT * FROM final

{% else %}

SELECT
    NULL AS "id",
    NULL AS "id_bas",
    NULL AS "id_res",
    NULL AS "code_bassin_bas"
WHERE FALSE

{% endif %}
