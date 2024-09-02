WITH final AS (
    {{
        dbt_utils.union_relations(
            relations=[
                ref('int_dora__contacts'),
                ref('int_mediation_numerique__contacts'),
                ref('int_mes_aides__contacts'),
            ],
            source_column_name=None
        )
    }}
)

SELECT * FROM final
