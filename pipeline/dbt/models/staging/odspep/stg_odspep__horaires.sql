WITH source AS (
    SELECT *
    FROM {{ source('odspep', 'DD009_HORAIRE') }}
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
