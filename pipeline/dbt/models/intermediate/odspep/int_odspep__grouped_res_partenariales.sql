WITH duplicated_ressources_partenariales AS (
    SELECT * FROM {{ ref('int_odspep__enhanced_res_partenariales') }}
),

ressources_partenariales AS (
    -- remove duplication introduced by us, when we joined the perimetre/zone_de_diffusion with
    -- the services (1 service, n zone_de_diffusion) the int_odspep__enhanced_res_partenariales model
    SELECT DISTINCT ON (id_res) *
    FROM duplicated_ressources_partenariales
),

ressources_partenariales_with_group_key AS (
    SELECT
        *,
        UNACCENT(LOWER(CONCAT(nom_structure,
            l1_identification_dest_adr,
            l3_complement_adr,
            l4_numero_lib_voie_adr,
            code_commune_adr))) AS "group_key"
    FROM ressources_partenariales
),

final AS (
    SELECT
        {{ dbt_utils.star(from=ref('int_odspep__enhanced_res_partenariales')) }},
        group_key                             AS "group_key",
        DENSE_RANK() OVER(ORDER BY group_key) AS "group_number",
        COUNT(*) OVER(PARTITION BY group_key) AS "group_size"
    FROM ressources_partenariales_with_group_key
)

SELECT * FROM final
