{{
    dbt_utils.union_relations(
        relations=[
            ref('int_soliguide__adresses_v1'),
        ],
        source_column_name=None
    )
}}
