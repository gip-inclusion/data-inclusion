{{ dbt_utils.union_relations(
    relations=[ref('int_mes_aides_garages__structures'), ref('int_mes_aides_permis_velo__structures')]
) }}
