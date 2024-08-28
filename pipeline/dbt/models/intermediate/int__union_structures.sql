WITH structures AS (
    {{
        dbt_utils.union_relations(
            relations=[
                ref('int_action_logement__structures'),
                ref('int_agefiph__structures'),
                ref('int_data_inclusion__structures'),
                ref('int_dora__structures'),
                ref('int_cd35__structures'),
                ref('int_emplois_de_linclusion__structures'),
                ref('int_finess__structures'),
                ref('int_france_travail__structures'),
                ref('int_fredo__structures'),
                ref('int_mediation_numerique__structures'),
                ref('int_mes_aides__structures'),
                ref('int_monenfant__structures'),
                ref('int_odspep__structures'),
                ref('int_reseau_alpha__structures'),
                ref('int_siao__structures'),
                ref('int_soliguide__structures'),
            ],
            column_override={
                "thematiques": "TEXT[]",
                "labels_nationaux": "TEXT[]",
                "labels_autres": "TEXT[]",
            },
            source_column_name=None
        )
    }}
),

final AS (
    SELECT
        *,
        source || '-' || id         AS "_di_surrogate_id",
        source || '-' || adresse_id AS "_di_adresse_surrogate_id"
    FROM structures
)

SELECT * FROM final
