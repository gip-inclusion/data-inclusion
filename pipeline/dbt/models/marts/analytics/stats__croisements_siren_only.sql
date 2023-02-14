WITH structures AS (
    SELECT * FROM {{ ref('int__enhanced_structures') }}
),

sirets AS (
    SELECT
        source                                        AS "source",
        COALESCE(siret, _di_annotated_siret)          AS "siret",
        LEFT(COALESCE(siret, _di_annotated_siret), 9) AS "siren"
    FROM
        structures
),

croisements AS (
    SELECT
        sirets.source       AS "source",
        other_sirets.source AS "other_source"
    FROM sirets
    LEFT JOIN sirets AS other_sirets ON sirets.siren = other_sirets.siren
    WHERE sirets.siret != other_sirets.siret
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
