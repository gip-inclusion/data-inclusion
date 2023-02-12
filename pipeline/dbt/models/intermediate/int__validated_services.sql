WITH services AS (
    SELECT * FROM {{ ref('int__services') }}
),

thematiques AS (
    SELECT * FROM {{ ref('thematiques') }}
),

typologies_de_services AS (
    SELECT * FROM {{ ref('typologies_de_services') }}
),

frais AS (
    SELECT * FROM {{ ref('frais') }}
),

profils AS (
    SELECT * FROM {{ ref('profils') }}
),

validated_structures AS (
    SELECT * FROM {{ ref('int__validated_structures') }}
),

filtered_services AS (
    SELECT services.*
    FROM services
    INNER JOIN validated_structures ON services._di_structure_surrogate_id = validated_structures._di_surrogate_id
),

final AS (
    SELECT *
    FROM filtered_services
    WHERE
        id IS NOT NULL
        AND structure_id IS NOT NULL
        AND source IS NOT NULL
        AND nom IS NOT NULL
        AND (thematiques IS NULL OR thematiques <@ ARRAY(SELECT value FROM thematiques))
        AND (types IS NULL OR types <@ ARRAY(SELECT value FROM typologies_de_services))
        AND (frais IS NULL OR frais <@ ARRAY(SELECT value FROM frais))
        AND (profils IS NULL OR profils <@ ARRAY(SELECT value FROM profils))
)

SELECT * FROM final
