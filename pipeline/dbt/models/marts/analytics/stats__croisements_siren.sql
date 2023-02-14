WITH structures AS (
    SELECT * FROM {{ ref('int__enhanced_structures') }}
),

sirens AS (
    SELECT
        source                                        AS "source",
        LEFT(COALESCE(siret, _di_annotated_siret), 9) AS "siren"
    FROM
        structures
),

croisements AS (
    SELECT
        sirens.source       AS "source",
        other_sirens.source AS "other_source"
    FROM sirens
    LEFT JOIN sirens AS other_sirens ON sirens.siren = other_sirens.siren
    WHERE sirens.source != other_sirens.source
),

final AS (
    SELECT
        source,
        {{
            dbt_utils.pivot(
                column='other_source',
                values=dbt_utils.get_column_values(
                    table=ref('int__enhanced_structures'),
                    column="source",
                    order_by="source"
                )
            )
        }}
    FROM croisements
    GROUP BY 1
    ORDER BY 1
)

SELECT * FROM final
