WITH formations AS (
    SELECT * FROM {{ ref('stg_reseau_alpha__formations') }}
),

structures AS (
    SELECT * FROM {{ ref('stg_reseau_alpha__structures') }}
),

final AS (
    SELECT
        TRUE                                   AS "contact_public",
        formations.content__contact_prenom_nom AS "contact_nom_prenom",
        formations.content__courriel           AS "courriel",
        NULL                                   AS "formulaire_en_ligne",
        NULL                                   AS "frais_autres",
        formations.nom                         AS "nom",
        NULL                                   AS "presentation_resume",
        NULL                                   AS "prise_rdv",
        formations.content__horaires           AS "recurrence",
        formations._di_source_id               AS "source",
        formations.structure_id                AS "structure_id",
        formations.content__telephone          AS "telephone",
        NULL                                   AS "zone_diffusion_code",  -- FIXME
        NULL                                   AS "zone_diffusion_nom",
        'departement'                          AS "zone_diffusion_type",
        TRUE                                   AS "cumulable",
        formations.url                         AS "lien_source",
        formations.id                          AS "id",
        formations.content__date_maj           AS "date_maj",
        NULL                                   AS "modes_orientation_accompagnateur_autres",
        NULL                                   AS "modes_orientation_beneficiaire_autres",
        ARRAY_TO_STRING(
            ARRAY[
                formations.content__contenu_et_objectifs,
                formations.content__public_attendu,
                formations.content__inscription,
                formations.content__informations_pratiques
            ],
            E'\n\n'
        )                                      AS "presentation_detail",
        'service--' || formations.id           AS "adresse_id",
        CAST(NULL AS TEXT [])                  AS "justificatifs",
        CAST(NULL AS TEXT [])                  AS "pre_requis",
        CAST(NULL AS DATE)                     AS "date_suspension",
        CAST(NULL AS DATE)                     AS "date_creation",
        ARRAY_REMOVE(
            ARRAY[
                'apprendre-francais--suivre-formation',
                CASE WHEN formations.activite = 'Français à visée professionnelle' THEN 'apprendre-francais--accompagnement-insertion-pro' END,
                CASE WHEN formations.activite = 'Français à visée sociale et communicative' THEN 'apprendre-francais--communiquer-vie-tous-les-jours' END
            ],
            NULL
        )                                      AS "thematiques",
        ARRAY['en-presentiel']                 AS "modes_accueil",
        CAST(NULL AS TEXT [])                  AS "modes_orientation_accompagnateur",
        CAST(NULL AS TEXT [])                  AS "modes_orientation_beneficiaire",
        CAST(NULL AS TEXT [])                  AS "profils",
        ARRAY['formation']                     AS "types",
        CAST(NULL AS TEXT [])                  AS "frais"
    FROM formations
    LEFT JOIN structures ON formations.structure_id = structures.id
)

SELECT * FROM final
