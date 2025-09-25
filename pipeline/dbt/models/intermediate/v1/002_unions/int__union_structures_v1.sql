{{
    dbt_utils.union_relations(
        relations=[
            ref('int_action_logement__structures_v1'),
        ],
        source_column_name=None
    )
}}
