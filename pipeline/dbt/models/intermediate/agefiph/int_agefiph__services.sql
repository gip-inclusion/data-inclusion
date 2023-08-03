WITH services AS (
    SELECT * FROM {{ ref('stg_agefiph__services') }}
),

final AS (
    SELECT
        id                    AS "adresse_id",
        contact_public        AS "contact_public",
        NULL                  AS "contact_nom_prenom",
        courriel              AS "courriel",
        NULL                  AS "cumulable",
        date_creation         AS "date_creation",
        date_maj              AS "date_maj",
        NULL                  AS "date_suspension",
        NULL                  AS "formulaire_en_ligne",
        NULL                  AS "frais_autres",
        id                    AS "id",
        NULL                  AS "justificatifs",
        NULL                  AS "lien_source",
        nom                   AS "nom",
        presentation_resume   AS "presentation_resume",
        presentation_detail   AS "presentation_detail",
        NULL                  AS "prise_rdv",
        NULL                  AS "recurrence",
        _di_source_id         AS "source",
        structure_id          AS "structure_id",
        telephone             AS "telephone",
        thematiques           AS "thematiques",
        NULL                  AS "zone_diffusion_code",
        NULL                  AS "zone_diffusion_nom",
        NULL                  AS "zone_diffusion_type",
        NULL                  AS "pre_requis",
        CAST(NULL AS TEXT []) AS "modes_accueil",
        CAST(NULL AS TEXT []) AS "modes_orientation_accompagnateur",
        CAST(NULL AS TEXT []) AS "modes_orientation_beneficiaire",
        CAST(NULL AS TEXT []) AS "profils",
        CAST(NULL AS TEXT []) AS "types",
        CAST(NULL AS TEXT []) AS "frais"
    FROM services
)

SELECT * FROM final
