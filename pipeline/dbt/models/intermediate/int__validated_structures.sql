WITH structures AS (
    SELECT * FROM {{ ref('int__enhanced_structures') }}
),

typologies_de_structures AS (
    SELECT * FROM {{ ref('typologies_de_structures') }}
),

labels_nationaux AS (
    SELECT * FROM {{ ref('labels_nationaux') }}
),

thematiques AS (
    SELECT * FROM {{ ref('thematiques') }}
),

final AS (
    SELECT *
    FROM structures
    WHERE
        id IS NOT NULL
        AND siret ~ '^\d{14}$'
        AND rna ~ '^W\d{9}$'
        AND nom IS NOT NULL
        AND commune IS NOT NULL
        AND code_postal ~ '^\d{5}$'
        AND code_insee ~ '^.{5}$'
        AND adresse IS NOT NULL
        AND date_maj IS NOT NULL
        AND (typologie IS NULL OR typologie IN (SELECT value FROM typologies_de_structures))
        AND (labels_nationaux IS NULL OR labels_nationaux <@ ARRAY(SELECT value FROM labels_nationaux))
        AND (thematiques IS NULL OR thematiques <@ ARRAY(SELECT value FROM thematiques))
        AND LENGTH(presentation_resume) <= 280
)

SELECT * FROM final
