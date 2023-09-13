WITH services AS (
    SELECT * FROM {{ ref('stg_cd72__services') }}
),

final AS (
    SELECT
        id                                    AS "adresse_id",
        TRUE                                  AS "contact_public",
        contact_nom_prenom                    AS "contact_nom_prenom", -- ignored for now
        courriel                              AS "courriel", -- ignored for now
        date_creation                         AS "date_creation",
        date_maj                              AS "date_maj",
        date_suspension                       AS "date_suspension",
        NULL                                  AS "formulaire_en_ligne",
        frais_autres                          AS "frais_autres",
        id                                    AS "id",
        NULL                                  AS "lien_source",
        NULL                                  AS "modes_orientation_accompagnateur_autres",
        modes_orientation_beneficiaire_autres AS "modes_orientation_beneficiaire_autres",
        nom                                   AS "nom",
        presentation_resume                   AS "presentation_resume",
        presentation_detail                   AS "presentation_detail",
        NULL                                  AS "prise_rdv",
        profils                               AS "profils",
        recurrence                            AS "recurrence",
        _di_source_id                         AS "source",
        structure_id                          AS "structure_id",
        telephone                             AS "telephone",
        thematiques                           AS "thematiques",
        zone_diffusion_code                   AS "zone_diffusion_code",
        NULL                                  AS "zone_diffusion_nom",
        zone_diffusion_type                   AS "zone_diffusion_type",
        CAST(NULL AS BOOLEAN)                 AS "cumulable",
        CAST(NULL AS TEXT [])                 AS "justificatifs",
        CAST(NULL AS TEXT [])                 AS "modes_accueil",
        CAST(NULL AS TEXT [])                 AS "modes_orientation_accompagnateur",
        CAST(NULL AS TEXT [])                 AS "modes_orientation_beneficiaire",
        CAST(NULL AS TEXT [])                 AS "types",
        ARRAY[pre_requis]                     AS "pre_requis",
        CAST(NULL AS TEXT [])                 AS "frais"
    FROM services
)

SELECT * FROM final
