{{
    dbt_utils.union_relations(
        relations=[
            ref('int_mes_aides__permis_velo__structures'),
            ref('int_mes_aides__garages__structures'),
        ],
        column_override={
            "labels_nationaux": "TEXT[]",
            "labels_autres": "TEXT[]",
            "thematiques": "TEXT[]",
        },
        source_column_name=None
    )
}}
