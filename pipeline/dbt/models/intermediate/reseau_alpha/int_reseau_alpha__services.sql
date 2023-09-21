WITH formations AS (
    SELECT * FROM {{ ref('stg_reseau_alpha__formations') }}
),

final AS (
    SELECT
        TRUE                        AS "contact_public",
        content__contact_prenom_nom AS "contact_nom_prenom",
        content__courriel           AS "courriel",
        NULL                        AS "formulaire_en_ligne",
        NULL                        AS "frais_autres",
        nom                         AS "nom",
        NULL                        AS "presentation_resume",
        NULL                        AS "prise_rdv",
        NULL                        AS "recurrence",
        _di_source_id               AS "source",
        structure_id                AS "structure_id",
        content__telephone          AS "telephone",
        'departement'               AS "zone_diffusion_code",
        NULL                        AS "zone_diffusion_nom",
        NULL                        AS "zone_diffusion_type",
        TRUE                        AS "cumulable",  -- TODO
        url                         AS "lien_source",  -- TODO
        id                          AS "id",
        NULL                        AS "presentation_detail",
        'service--' || id           AS "adresse_id",
        CAST(NULL AS TEXT [])       AS "justificatifs",
        CAST(NULL AS TEXT [])       AS "pre_requis",
        CAST(NULL AS DATE)          AS "date_suspension",  -- TODO
        CAST(NULL AS DATE)          AS "date_creation",
        CAST(NULL AS DATE)          AS "date_maj",
        CAST(NULL AS TEXT [])       AS "thematiques",  -- TODO
        ARRAY['en-presentiel']      AS "modes_accueil",
        CAST(NULL AS TEXT [])       AS "modes_orientation_accompagnateur",
        CAST(NULL AS TEXT [])       AS "modes_orientation_beneficiaire",
        CAST(NULL AS TEXT [])       AS "profils",
        ARRAY['formation']          AS "types",
        CAST(NULL AS TEXT [])       AS "frais"  -- TODO
    FROM
        formations
)

SELECT * FROM final
