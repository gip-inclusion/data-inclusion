WITH services AS (
    {{
        dbt_utils.union_relations(
            relations=[
                ref('int_dora__services'),
                ref('int_mediation_numerique__services')
            ],
            column_override={
                "types": "TEXT[]",
                "frais": "TEXT[]",
                "profils": "TEXT[]",
                "thematiques": "TEXT[]",
            },
            source_column_name=None
        )
    }}
),

final AS (
    SELECT
        *,
        source || '-' || id           AS "_di_surrogate_id",
        source || '-' || structure_id AS "_di_structure_surrogate_id"
    FROM services
)

SELECT * FROM final
