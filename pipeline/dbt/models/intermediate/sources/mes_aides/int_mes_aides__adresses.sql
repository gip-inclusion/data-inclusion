{{ dbt_utils.union_relations(
    relations=[ref('int_mes_aides_garages__adresses'), ref('int_mes_aides_permis_velo__adresses')]
) }}
