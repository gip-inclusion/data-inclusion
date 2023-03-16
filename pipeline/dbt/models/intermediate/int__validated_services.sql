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

modes_accueil AS (
    SELECT * FROM {{ ref('modes_accueil') }}
),

types_cog AS (
    SELECT * FROM {{ ref('types_cog') }}
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
        AND (code_postal IS NULL OR code_postal ~ '^\d{5}$')
        AND (code_insee IS NULL OR code_insee ~ '^.{5}$')
        -- RFC 5322
        AND (courriel IS NULL OR courriel ~ '^[a-zA-Z0-9.!#$%&''*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$')
        AND (modes_accueil IS NULL OR modes_accueil <@ ARRAY(SELECT value FROM modes_accueil))
        AND (zone_diffusion_type IS NULL OR zone_diffusion_type IN (SELECT value FROM types_cog))
        AND (zone_diffusion_code IS NULL OR zone_diffusion_code ~ '^(\w{5}|\w{2,3}|\d{2})$')
)

SELECT * FROM final
