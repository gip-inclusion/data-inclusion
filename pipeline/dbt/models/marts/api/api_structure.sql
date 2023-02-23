{{
    config(
        post_hook="ALTER TABLE {{ this }} ADD PRIMARY KEY (_di_surrogate_id)",
    )
}}

WITH structures AS (
    SELECT * FROM {{ ref('int__validated_structures') }}
),

final AS (
    SELECT *
    FROM structures
    WHERE source NOT IN ('soliguide', 'siao', 'finess')
)

SELECT * FROM final
