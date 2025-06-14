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

final AS (

    SELECT * FROM regions
    UNION
    SELECT * FROM departements
    UNION
    SELECT * FROM communes
)

SELECT * FROM final
