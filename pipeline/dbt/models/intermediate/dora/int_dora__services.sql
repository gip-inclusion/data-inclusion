WITH services AS (
    SELECT * FROM {{ ref('stg_dora__services') }}
),

final AS (
    SELECT
        id                               AS "adresse_id",
        contact_nom                      AS "contact_nom",
        contact_prenom                   AS "contact_prenom",
        contact_public                   AS "contact_public",
        courriel                         AS "courriel",
        cumulable                        AS "cumulable",
        date_creation                    AS "date_creation",
        date_maj                         AS "date_maj",
        date_suspension                  AS "date_suspension",
        formulaire_en_ligne              AS "formulaire_en_ligne",
        frais_autres                     AS "frais_autres",
        frais                            AS "frais",
        id                               AS "id",
        justificatifs                    AS "justificatifs",
        lien_source                      AS "lien_source",
        modes_accueil                    AS "modes_accueil",
        nom                              AS "nom",
        presentation_resume              AS "presentation_resume",
        presentation_detail              AS "presentation_detail",
        prise_rdv                        AS "prise_rdv",
        profils                          AS "profils",
        recurrence                       AS "recurrence",
        _di_source_id                    AS "source",
        structure_id                     AS "structure_id",
        telephone                        AS "telephone",
        thematiques                      AS "thematiques",
        types                            AS "types",
        zone_diffusion_code              AS "zone_diffusion_code",
        zone_diffusion_nom               AS "zone_diffusion_nom",
        zone_diffusion_type              AS "zone_diffusion_type",
        ARRAY_TO_STRING(pre_requis, ',') AS "pre_requis"
    FROM services
)

SELECT * FROM final
