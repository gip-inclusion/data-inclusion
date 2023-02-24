-- This model concatenates the ODSPEP tables related to the geographic scopes of ressources/prestations and using the DN001 referential

WITH regions AS (
    SELECT
        id_res,
        code_region_reg                   AS "code",
        'Région'                          AS "type_code",
        CONCAT('region', code_region_reg) AS "unique_code"
    FROM {{ ref('stg_odspep__regions') }}
),

departements AS (
    SELECT
        id_res,
        code_departement_dpt                         AS "code",
        'Département'                                AS "type_code",
        CONCAT('departement_', code_departement_dpt) AS "unique_code"

    FROM {{ ref('stg_odspep__departements') }}
),

communes AS (
    SELECT
        id_res,
        code_commune_com                     AS "code",
        'Commune'                            AS "type_code",
        CONCAT('commune_', code_commune_com) AS "unique_code"
    FROM {{ ref('stg_odspep__communes') }}
),

bassins AS (
    SELECT
        id_res,
        code_bassin_bas                    AS "code",
        'Bassin'                           AS "type_code",
        CONCAT('bassin_', code_bassin_bas) AS "unique_code"
    FROM {{ ref('stg_odspep__bassins') }}
),

dir_territoriale_ofii AS (
    SELECT
        id_res,
        code_dit                  AS "code",
        'DT OFII'                 AS "type_code",
        CONCAT('ofii_', code_dit) AS "unique_code"
    FROM {{ ref('stg_odspep__dir_territoriale_ofii') }}
),

final AS (

    SELECT * FROM regions
    UNION
    SELECT * FROM departements
    UNION
    SELECT * FROM communes
    UNION
    SELECT * FROM bassins
    UNION
    SELECT * FROM dir_territoriale_ofii
)

SELECT * FROM final
