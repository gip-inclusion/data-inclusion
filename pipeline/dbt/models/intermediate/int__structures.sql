WITH structures AS (
    {{ 
        dbt_utils.union_relations(
            relations=[
                ref('int_dora__structures'),
                ref('int_cd35__structures'),
                ref('int_cd72__structures'),
                ref('int_emplois_de_linclusion__structures'),
                ref('int_mes_aides__structures'),
                ref('int_mediation_numerique__structures'),
                ref('int_odspep__deduplicated_structures'),
                ref('int_soliguide__structures'),
                ref('int_siao__structures'),
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
        source || '-' || id AS "_di_surrogate_id"
    FROM structures
)

SELECT * FROM final
