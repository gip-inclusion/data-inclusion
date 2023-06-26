WITH ressources_partenariales AS (
    SELECT * FROM {{ ref('int_odspep__enhanced_res_partenariales') }}
),

final AS (
    -- remove duplication introduced by us
    -- in the int_odspep__enhanced_res_partenariales model
    SELECT DISTINCT ON (1)
        id_res                 AS "id",
        longitude_adr          AS "longitude",
        latitude_adr           AS "latitude",
        'odspep'               AS "source",
        l3_complement_adr      AS "complement_adresse",
        libelle_commune_adr    AS "commune",
        l4_numero_lib_voie_adr AS "adresse",
        code_postal_adr        AS "code_postal",
        code_commune_adr       AS "code_insee"
    FROM ressources_partenariales
    ORDER BY 1
)

SELECT * FROM final
