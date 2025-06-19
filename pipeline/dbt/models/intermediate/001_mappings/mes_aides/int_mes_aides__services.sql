{{
    dbt_utils.union_relations(
        relations=[
            ref('int_mes_aides__permis_velo__services'),
            ref('int_mes_aides__garages__services'),
        ],
        column_override={
            "frais": "TEXT[]",
            "justificatifs": "TEXT[]",
            "modes_accueil": "TEXT[]",
            "modes_orientation_accompagnateur": "TEXT[]",
            "modes_orientation_beneficiaire": "TEXT[]",
            "modes_mobilisation": "TEXT[]",
            "mobilisable_par": "TEXT[]",
            "pre_requis": "TEXT[]",
            "profils": "TEXT[]",
            "thematiques": "TEXT[]",
            "types": "TEXT[]",
        },
        source_column_name=None
    )
}}
