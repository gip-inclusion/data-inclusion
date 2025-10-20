{{
    dbt_utils.union_relations(
        relations=[
            ref('int_mes_aides__garages__adresses_v1'),
        ],
        source_column_name=None
    )
}}
