{% set source_model = source('odspep', 'DD009_DIR_TERRITORIALE_OFII') %}

{% set table_exists = adapter.get_relation(database=source_model.database, schema=source_model.schema, identifier=source_model.name) is not none %}

{% if table_exists %}

    WITH source AS (
        SELECT * FROM {{ source_model }}
    ),

    final AS (
        SELECT
            "ID_DIT"   AS "id",
            "ID_DIT"   AS "id_dit",
            "ID_RES"   AS "id_res",
            "CODE_DIT" AS "code_dit"
        FROM source
    )

    SELECT * FROM final

{% else %}

SELECT
    NULL AS "id",
    NULL AS "id_dit",
    NULL AS "id_res",
    NULL AS "code_dit"
WHERE FALSE

{% endif %}
