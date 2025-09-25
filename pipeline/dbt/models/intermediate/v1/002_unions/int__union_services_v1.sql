{{
    dbt_utils.union_relations(
        relations=[
            ref('int_action_logement__services_v1'),
        ],
        source_column_name=None
    )
}}
