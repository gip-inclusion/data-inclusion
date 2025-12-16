{{
    dbt_utils.union_relations(
        relations=[
            ref('int_action_logement__services_v1'),
            ref('int_agefiph__services_v1'),
            ref('int_carif_oref__services_v1'),
            ref('int_dora__services_v1'),
            ref('int_france_travail__services_v1'),
            ref('int_soliguide__services_v1'),
            ref('int_ma_boussole_aidants__services_v1'),
            ref('int_monenfant__services_v1'),
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
