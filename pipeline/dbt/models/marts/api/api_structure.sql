{{
    config(
        pre_hook=[
            "DROP INDEX IF EXISTS structure_source_idx",
        ],
        post_hook=[
            "CREATE INDEX IF NOT EXISTS structure_source_idx ON {{ this }} (source);",
        ]
    )
}}

WITH structures AS (
    SELECT * FROM {{ ref('int__union_structures__enhanced') }}
),

final AS (
    SELECT
        {{
            dbt_utils.star(
                relation_alias='structures',
                from=ref('int__union_structures__enhanced'),
                except=[
                    '_di_sirene_date_fermeture',
                    '_di_sirene_etab_successeur',
                    '_di_adresse_surrogate_id',
                    '_di_annotated_antenne',
                    '_di_annotated_siret',
                    '_di_email_is_pii',
                    'adresse_id',
                ]
            )
        }}
    FROM structures
    WHERE structures.source NOT IN ('siao', 'finess')
)

SELECT * FROM final
