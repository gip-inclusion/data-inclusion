-- This model aggregates all the *_RESSOURCE ODSPEP tables in the same table, aligned on a common set of columns

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

communes AS (
    SELECT
        id,
        id_res,
        code_commune_com AS code,
        'Commune'        AS type_code
    FROM {{ ref('stg_odspep__communes') }}
),

bassins AS (
    SELECT
        id,
        id_res,
        code_bassin_bas AS code,
        'Bassin'        AS type_code
    FROM {{ ref('stg_odspep__bassins') }}
),

final AS (

    SELECT * FROM regions
    UNION
    SELECT * FROM departements
    UNION
    SELECT * FROM communes
    UNION
    SELECT * FROM bassins
)

SELECT * FROM final
