{{
    dbt_utils.union_relations(
        relations=[
            ref('int_soliguide__structures_v1'),
        ],
        column_override={
            "reseaux_porteurs": "TEXT[]",
        },
        source_column_name=None
    )
}}
