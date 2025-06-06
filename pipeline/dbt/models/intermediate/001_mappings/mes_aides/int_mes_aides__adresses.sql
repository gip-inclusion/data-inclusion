{{ dbt_utils.union_relations(
    relations=[ref('int_mes_aides__garages__adresses'), ref('int_mes_aides__permis_velo__adresses')]
) }}
