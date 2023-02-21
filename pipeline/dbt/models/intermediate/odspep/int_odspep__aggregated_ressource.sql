WITH regions AS (
    SELECT
        id,
        id_res,
        code_region_reg AS code,
        'Région'        AS type_code
    FROM {{ ref('stg_odspep__regions') }}
),

departements AS (
    SELECT
        id,
        id_res,
        code_departement_dpt AS code,
        'Département'        AS type_code
    FROM {{ ref('stg_odspep__departements') }}
),

final AS (

    SELECT * FROM regions
    UNION
    SELECT * FROM departements
)

SELECT * FROM final
