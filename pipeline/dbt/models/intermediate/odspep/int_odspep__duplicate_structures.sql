WITH ressources_partenariales AS (
    SELECT * FROM {{ ref('int_odspep__enhanced_res_partenariales') }}
),



select_duplicates AS (
    SELECT
        id_res,
        nom_structure,
        l1_identification_dest_adr,
        l3_complement_adr,
        l4_numero_lib_voie_adr,
        code_commune_adr,
        '1'::BOOLEAN                                                                                                                                                     AS "duplicate",
        COUNT(id_res) OVER(PARTITION BY UNACCENT(LOWER(CONCAT(nom_structure, l1_identification_dest_adr, l3_complement_adr, l4_numero_lib_voie_adr, code_commune_adr)))) AS "count_duplicates"

    FROM ressources_partenariales
),

final AS (
    SELECT * FROM select_duplicates
    WHERE "count_duplicates" > 1

)

SELECT * FROM final
