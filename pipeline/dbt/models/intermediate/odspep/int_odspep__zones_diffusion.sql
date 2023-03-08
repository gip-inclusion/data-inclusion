-- This model concatenates the ODSPEP tables related to the geographic scopes of ressources/prestations and using the DN001 referential

WITH regions AS (
    SELECT
        id_res,
        code_region_reg                   AS "code",
        'region'                          AS "type",
        libelle                           AS "libelle",
        CONCAT('region', code_region_reg) AS "unique_code"
    FROM {{ ref('stg_odspep__regions') }}
),

departements AS (
    SELECT
        id_res,
        code_departement_dpt                         AS "code",
        'departement'                                AS "type",
        libelle                                      AS "libelle",
        CONCAT('departement_', code_departement_dpt) AS "unique_code"
    FROM {{ ref('stg_odspep__departements') }}
),

communes AS (
    SELECT
        id_res,
        code_commune_com                     AS "code",
        'commune'                            AS "type",
        libelle                              AS "libelle",
        CONCAT('commune_', code_commune_com) AS "unique_code"
    FROM {{ ref('stg_odspep__communes') }}
),

bassins AS (
    SELECT
        id_res,
        code_bassin_bas                    AS "code",
        'bassin'                           AS "type",
        NULL                               AS "libelle",
        CONCAT('bassin_', code_bassin_bas) AS "unique_code"
    FROM {{ ref('stg_odspep__bassins') }}
),

dir_territoriale_ofii AS (
    SELECT
        id_res,
        code_dit                  AS "code",
        'dt_ofii'                 AS "type",
        NULL                      AS "libelle",
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
