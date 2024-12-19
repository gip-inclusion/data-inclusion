WITH structures AS (
    SELECT * FROM {{ ref('int__union_structures__enhanced') }}
),

final AS (
    SELECT * FROM processings.deduplicate(TO_JSONB(
        (SELECT JSONB_AGG(ROW_TO_JSON(structures)) FROM structures)  -- noqa: references.qualification
    ))
)

SELECT * FROM final
