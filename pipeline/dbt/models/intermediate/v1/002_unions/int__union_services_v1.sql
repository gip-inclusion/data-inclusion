{{
    dbt_utils.union_relations(
        relations=[
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
