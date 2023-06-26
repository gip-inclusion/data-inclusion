WITH structures AS (
    {{
        dbt_utils.union_relations(
            relations=[
                ref('stg_mediation_numerique_angers__structures'),
                ref('stg_mediation_numerique_assembleurs__structures'),
                ref('stg_mediation_numerique_cd17__structures'),
            ],
            column_override={
                "thematiques": "TEXT[]",
                "labels_nationaux": "TEXT[]",
            },
            source_column_name=None
        )
    }}
),

final AS (
    SELECT
        id            AS "id",
        commune       AS "commune",
        code_postal   AS "code_postal",
        NULL          AS "code_insee",
        adresse       AS "adresse",
        NULL          AS "complement_adresse",
        longitude     AS "longitude",
        latitude      AS "latitude",
        _di_source_id AS "source"
    FROM structures
)

SELECT * FROM final
