{{
    dbt_utils.union_relations(
        relations=[
            ref('int_mes_aides__garages__services_v1'),
        ],
        column_override={
            "thematiques": "TEXT[]",
            "publics": "TEXT[]",
            "modes_accueil": "TEXT[]",
            "zone_eligibilite": "TEXT[]",
            "modes_mobilisation": "TEXT[]",
            "mobilisable_par": "TEXT[]",
        },
        source_column_name=None
    )
}}
