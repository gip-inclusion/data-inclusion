WITH source AS (
    SELECT * FROM {{ ref('stg_agefiph__services') }}
),

final AS (
    SELECT
        _di_source_id       AS "source",
        structure_id        AS "structure_id",
        courriel            AS "courriel",
        telephone           AS "telephone",
        adresse             AS "adresse",
        commune             AS "commune",
        code_postal         AS "code_postal",
        code_insee          AS "code_insee",
        zone_diffusion_type AS "zone_diffusion_type",
        zone_diffusion_code AS "zone_diffusion_code",
        id                  AS "id",
        date_creation       AS "date_creation",
        date_maj            AS "date_maj",
        nom                 AS "nom",
        presentation_resume AS "presentation_resume",
        presentation_detail AS "presentation_detail",
        contact_public      AS "contact_public",
        --id_thematiques                                                                 AS "id_thematiques",
        thematiques         AS "thematiques"


    FROM source
)

SELECT * FROM final
