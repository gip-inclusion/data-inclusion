{% set source_model = source('odspep', 'DD009_HORAIRE') %}

{% set table_exists = adapter.get_relation(database=source_model.database, schema=source_model.schema, identifier=source_model.name) is not none %}

{% if table_exists %}

    WITH source AS (
        SELECT * FROM {{ source_model }}
    ),

    final AS (
        SELECT
            "ID_HOR"        AS "id",
            "ID_HOR"        AS "id_hor",
            "ID_RES"        AS "id_res",
            "JOUR_HOR"::INT AS "jour_hor",
            "HD_AM_HOR"     AS "hd_am_hor",
            "HF_AM_HOR"     AS "hf_am_hor",
            "HD_PM_HOR"     AS "hd_pm_hor",
            "HF_PM_HOR"     AS "hf_pm_hor"
        FROM source
    )

    SELECT * FROM final

{% else %}

SELECT
    NULL      AS "id",
    NULL      AS "id_hor",
    NULL      AS "id_res",
    NULL::INT AS "jour_hor",
    NULL      AS "hd_am_hor",
    NULL      AS "hf_am_hor",
    NULL      AS "hd_pm_hor",
    NULL      AS "hf_pm_hor"
WHERE FALSE

{% endif %}
