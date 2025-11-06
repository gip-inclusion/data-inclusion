{{
    dbt_utils.union_relations(
        relations=[
            ref('int_mes_aides__aides__structures_v1'),
            ref('int_mes_aides__garages__structures_v1'),
        ],
        column_override={
            "reseaux_porteurs": "TEXT[]",
        },
        source_column_name=None
    )
}}
