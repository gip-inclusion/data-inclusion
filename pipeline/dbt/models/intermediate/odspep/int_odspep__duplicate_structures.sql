-- remove duplication introduced by us, when we joined the perimetre/zone_de_diffusion with 
-- the services (1 service, n zone_de_diffusion) the int_odspep__enhanced_res_partenariales model

WITH ressources_partenariales AS (
    SELECT * FROM {{ ref('int_odspep__enhanced_res_partenariales') }}
),

unique_id_res AS (
    SELECT DISTINCT ON (id_res)
        id_res,
        nom_structure,
        l1_identification_dest_adr,
        l3_complement_adr,
        l4_numero_lib_voie_adr,
        code_commune_adr
    FROM ressources_partenariales
),

select_duplicates AS (
    SELECT *,
        '1'::BOOLEAN                                                                                                                                                     AS "duplicate",
        COUNT(id_res) OVER(PARTITION BY UNACCENT(LOWER(CONCAT(nom_structure, l1_identification_dest_adr, l3_complement_adr, l4_numero_lib_voie_adr, code_commune_adr)))) AS "count_duplicates"
 
    FROM unique_id_res
),

final AS (
    SELECT * FROM select_duplicates
    WHERE "count_duplicates" > 1
    ORDER BY UNACCENT(LOWER(CONCAT(count_duplicates, nom_structure, l1_identification_dest_adr, l3_complement_adr, l4_numero_lib_voie_adr, code_commune_adr))) DESC

)
SELECT * FROM final
