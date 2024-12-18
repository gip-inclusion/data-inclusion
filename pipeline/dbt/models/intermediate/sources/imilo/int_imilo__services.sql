WITH services AS (
    SELECT * FROM {{ ref('stg_imilo__offres') }}
),

final AS (
    SELECT
        _di_source_id                         AS "source",
        structure_id                          AS "structure_id",
        NULL                                  AS "courriel",
        CAST(NULL AS BOOLEAN)                 AS "cumulable",
        CAST(NULL AS BOOLEAN)                 AS "contact_public",
        NULL                                  AS "contact_nom_prenom",
        CAST(date_maj AS DATE)                AS "date_maj",
        CAST(date_creation AS DATE)           AS "date_creation",
        NULL                                  AS "formulaire_en_ligne",
        NULL                                  AS "frais_autres",
        CAST(NULL AS TEXT [])                 AS "justificatifs",
        NULL                                  AS "lien_source",
        CAST(NULL AS TEXT [])                 AS "modes_accueil",
        CAST(NULL AS TEXT [])                 AS "modes_orientation_accompagnateur",
        NULL                                  AS "modes_orientation_accompagnateur_autres",
        ARRAY[modes_orientation_beneficiaire] AS "modes_orientation_beneficiaire",
        NULL                                  AS "modes_orientation_beneficiaire_autres",
        nom                                   AS "nom",
        NULL                                  AS "page_web",
        NULL                                  AS "presentation_detail",
        presentation_resume                   AS "presentation_resume",
        NULL                                  AS "prise_rdv",
        ARRAY[profils]                        AS "profils",
        CAST(NULL AS TEXT [])                 AS "pre_requis",
        NULL                                  AS "recurrence",
        ARRAY[thematiques]                    AS "thematiques",
        CAST(NULL AS TEXT [])                 AS "types",
        NULL                                  AS "telephone",
        CAST(NULL AS TEXT [])                 AS "frais",
        NULL                                  AS "zone_diffusion_type",
        NULL                                  AS "zone_diffusion_code",
        NULL                                  AS "zone_diffusion_nom",
        CAST(NULL AS DATE)                    AS "date_suspension",
        id                                    AS "id"
    FROM services
)

SELECT * FROM final
