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

null_id AS (
    SELECT
        _di_structure_surrogate_id AS "_di_structure_surrogate_id",
        _di_surrogate_id           AS "_di_surrogate_id",
        source                     AS "_di_source_id",
        id                         AS "id",
        'id'                       AS "field",
        id::TEXT                   AS "value",
        'id_est_nul'               AS "type"
    FROM services
    WHERE id IS NULL
),

null_nom AS (
    SELECT
        _di_structure_surrogate_id AS "_di_structure_surrogate_id",
        _di_surrogate_id           AS "_di_surrogate_id",
        source                     AS "_di_source_id",
        id                         AS "id",
        'nom'                      AS "field",
        nom::TEXT                  AS "value",
        'nom_est_nul'              AS "type"
    FROM services
    WHERE nom IS NULL
),

null_source AS (
    SELECT
        _di_structure_surrogate_id AS "_di_structure_surrogate_id",
        _di_surrogate_id           AS "_di_surrogate_id",
        source                     AS "_di_source_id",
        id                         AS "id",
        'source'                   AS "field",
        source::TEXT               AS "value",
        'source_est_nulle'         AS "type"
    FROM services
    WHERE source IS NULL
),

null_structure_id AS (
    SELECT
        _di_structure_surrogate_id AS "_di_structure_surrogate_id",
        _di_surrogate_id           AS "_di_surrogate_id",
        source                     AS "_di_source_id",
        id                         AS "id",
        'structure_id'             AS "field",
        structure_id::TEXT         AS "value",
        'structure_id_est_nul'     AS "type"
    FROM services
    WHERE structure_id IS NULL
),

invalid_thematiques AS (
    SELECT
        _di_structure_surrogate_id AS "_di_structure_surrogate_id",
        _di_surrogate_id           AS "_di_surrogate_id",
        source                     AS "_di_source_id",
        id                         AS "id",
        'thematiques'              AS "field",
        thematiques::TEXT          AS "value",
        'thematiques_invalides'    AS "type"
    FROM services
    WHERE thematiques IS NOT NULL AND NOT thematiques <@ ARRAY(SELECT value FROM thematiques)
),

invalid_types AS (
    SELECT
        _di_structure_surrogate_id AS "_di_structure_surrogate_id",
        _di_surrogate_id           AS "_di_surrogate_id",
        source                     AS "_di_source_id",
        id                         AS "id",
        'types'                    AS "field",
        types::TEXT                AS "value",
        'types_invalide'           AS "type"
    FROM services
    WHERE types IS NOT NULL AND NOT types <@ ARRAY(SELECT value FROM typologies_de_services)
),

invalid_frais AS (
    SELECT
        _di_structure_surrogate_id AS "_di_structure_surrogate_id",
        _di_surrogate_id           AS "_di_surrogate_id",
        source                     AS "_di_source_id",
        id                         AS "id",
        'frais'                    AS "field",
        frais::TEXT                AS "value",
        'frais_invalides'          AS "type"
    FROM services
    WHERE frais IS NOT NULL AND NOT frais <@ ARRAY(SELECT value FROM frais)
),

invalid_profils AS (
    SELECT
        _di_structure_surrogate_id AS "_di_structure_surrogate_id",
        _di_surrogate_id           AS "_di_surrogate_id",
        source                     AS "_di_source_id",
        id                         AS "id",
        'profils'                  AS "field",
        profils::TEXT              AS "value",
        'profils_invalides'        AS "type"
    FROM services
    WHERE profils IS NOT NULL AND NOT profils <@ ARRAY(SELECT value FROM profils)
),

malformed_code_postal AS (
    SELECT
        _di_structure_surrogate_id AS "_di_structure_surrogate_id",
        _di_surrogate_id           AS "_di_surrogate_id",
        source                     AS "_di_source_id",
        id                         AS "id",
        'code_postal'              AS "field",
        code_postal::TEXT          AS "value",
        'code_postal_malformé'     AS "type"
    FROM services
    WHERE code_postal IS NOT NULL AND NOT code_postal ~ '^\d{5}$'
),

malformed_code_insee AS (
    SELECT
        _di_structure_surrogate_id AS "_di_structure_surrogate_id",
        _di_surrogate_id           AS "_di_surrogate_id",
        source                     AS "_di_source_id",
        id                         AS "id",
        'code_insee'               AS "field",
        code_insee::TEXT           AS "value",
        'code_insee_malformé'      AS "type"
    FROM services
    WHERE code_insee IS NOT NULL AND NOT code_insee ~ '^.{5}$'
),

invalid_courriel AS (
    SELECT
        _di_structure_surrogate_id AS "_di_structure_surrogate_id",
        _di_surrogate_id           AS "_di_surrogate_id",
        source                     AS "_di_source_id",
        id                         AS "id",
        'courriel'                 AS "field",
        courriel::TEXT             AS "value",
        'courriel_invalide'        AS "type"
    FROM services
    WHERE courriel IS NOT NULL AND NOT courriel ~ '^[a-zA-Z0-9.!#$%&''*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$'
),

invalid_modes_accueil AS (
    SELECT
        _di_structure_surrogate_id AS "_di_structure_surrogate_id",
        _di_surrogate_id           AS "_di_surrogate_id",
        source                     AS "_di_source_id",
        id                         AS "id",
        'modes_accueil'            AS "field",
        modes_accueil::TEXT        AS "value",
        'modes_accueil_invalides'  AS "type"
    FROM services
    WHERE modes_accueil IS NOT NULL AND NOT modes_accueil <@ ARRAY(SELECT value FROM modes_accueil)
),

invalid_zone_diffusion_type AS (
    SELECT
        _di_structure_surrogate_id     AS "_di_structure_surrogate_id",
        _di_surrogate_id               AS "_di_surrogate_id",
        source                         AS "_di_source_id",
        id                             AS "id",
        'zone_diffusion_type'          AS "field",
        zone_diffusion_type::TEXT      AS "value",
        'zone_diffusion_type_invalide' AS "type"
    FROM services
    WHERE
        zone_diffusion_type IS NOT NULL
        AND NOT zone_diffusion_type IN (SELECT value FROM types_cog)
),

malformed_zone_diffusion_code AS (
    SELECT
        _di_structure_surrogate_id     AS "_di_structure_surrogate_id",
        _di_surrogate_id               AS "_di_surrogate_id",
        source                         AS "_di_source_id",
        id                             AS "id",
        'zone_diffusion_code'          AS "field",
        zone_diffusion_code::TEXT      AS "value",
        'zone_diffusion_code_malformé' AS "type"
    FROM services
    WHERE zone_diffusion_code IS NOT NULL AND NOT zone_diffusion_code ~ '^(\w{5}|\w{2,3}|\d{2})$'
),

final AS (
    SELECT * FROM null_id
    UNION ALL
    SELECT * FROM null_nom
    UNION ALL
    SELECT * FROM null_source
    UNION ALL
    SELECT * FROM null_structure_id
    UNION ALL
    SELECT * FROM invalid_thematiques
    UNION ALL
    SELECT * FROM invalid_types
    UNION ALL
    SELECT * FROM invalid_frais
    UNION ALL
    SELECT * FROM invalid_profils
    UNION ALL
    SELECT * FROM malformed_code_postal
    UNION ALL
    SELECT * FROM malformed_code_insee
    UNION ALL
    SELECT * FROM invalid_courriel
    UNION ALL
    SELECT * FROM invalid_modes_accueil
    UNION ALL
    SELECT * FROM invalid_zone_diffusion_type
    UNION ALL
    SELECT * FROM malformed_zone_diffusion_code
)

SELECT * FROM final
