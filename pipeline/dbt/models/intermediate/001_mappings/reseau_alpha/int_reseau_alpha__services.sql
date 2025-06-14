WITH formations AS (
    SELECT * FROM {{ ref('stg_reseau_alpha__formations') }}
),

final AS (
    SELECT
        formations.content__contact_inscription__contact                       AS "contact_nom_prenom",
        formations.content__contact_inscription__courriel                      AS "courriel",
        formations.content__inscription__informations_en_ligne                 AS "formulaire_en_ligne",
        NULL                                                                   AS "frais_autres",
        formations.nom                                                         AS "nom",
        NULL                                                                   AS "prise_rdv",
        formations.content__lieux_et_horaires_formation__horaires              AS "recurrence",
        formations._di_source_id                                               AS "source",
        formations.structure_id                                                AS "structure_id",
        SPLIT_PART(formations.content__contact_inscription__telephone, '/', 1) AS "telephone",
        NULL                                                                   AS "zone_diffusion_code",
        NULL                                                                   AS "zone_diffusion_nom",  -- FIXME
        'departement'                                                          AS "zone_diffusion_type",
        formations.url                                                         AS "lien_source",
        formations.id                                                          AS "id",
        formations.content__date_maj                                           AS "date_maj",
        NULL                                                                   AS "modes_orientation_accompagnateur_autres",
        NULL                                                                   AS "modes_orientation_beneficiaire_autres",
        NULL                                                                   AS "page_web",
        CASE
            WHEN LENGTH(formations.content__contenu_et_objectifs__titre) <= 280
                THEN formations.content__contenu_et_objectifs__titre
            ELSE LEFT(formations.content__contenu_et_objectifs__titre, 279) || '…'
        END                                                                    AS "presentation_resume",
        ARRAY_TO_STRING(
            ARRAY[
                '# Contenu et objectifs de la formation',
                formations.content__contenu_et_objectifs__titre,
                formations.content__contenu_et_objectifs__objectifs,
                formations.content__contenu_et_objectifs__niveau,
                '# Public attendu',
                formations.content__public_attendu__niveau,
                formations.content__public_attendu__competences,
                formations.content__public_attendu__type_de_public,
                '# Inscription',
                formations.content__inscription__places,
                formations.content__inscription__entree_sortie,
                '# Informations pratiques',
                formations.content__informations_pratiques__etendue,
                formations.content__informations_pratiques__volume,
                formations.content__informations_pratiques__cout,
                formations.content__informations_pratiques__prise_en_charge,
                formations.content__informations_pratiques__remuneration,
                formations.content__informations_pratiques__garde
            ],
            E'\n\n'
        )                                                                      AS "presentation_detail",
        'service--' || formations.id                                           AS "adresse_id",
        CAST(NULL AS TEXT [])                                                  AS "justificatifs",
        CAST(NULL AS TEXT [])                                                  AS "pre_requis",
        ARRAY_REMOVE(
            ARRAY[
                'apprendre-francais--suivre-formation',
                CASE WHEN formations.activite = 'Français à visée professionnelle' THEN 'apprendre-francais--accompagnement-insertion-pro' END,
                CASE WHEN formations.activite = 'Français à visée sociale et communicative' THEN 'apprendre-francais--communiquer-vie-tous-les-jours' END
            ],
            NULL
        )                                                                      AS "thematiques",
        ARRAY['en-presentiel']                                                 AS "modes_accueil",
        ARRAY_REMOVE(
            ARRAY[
                CASE WHEN formations.content__contact_inscription__courriel IS NOT NULL THEN 'envoyer-un-mail' END,
                CASE WHEN formations.content__contact_inscription__telephone IS NOT NULL THEN 'telephoner' END
            ],
            NULL
        )                                                                      AS "modes_orientation_accompagnateur",
        ARRAY_REMOVE(
            ARRAY[
                CASE WHEN formations.content__contact_inscription__courriel IS NOT NULL THEN 'envoyer-un-mail' END,
                CASE WHEN formations.content__contact_inscription__telephone IS NOT NULL THEN 'telephoner' END
            ],
            NULL
        )                                                                      AS "modes_orientation_beneficiaire",
        ARRAY['public-langues-etrangeres']                                     AS "profils",
        NULL                                                                   AS "profils_precisions",
        ARRAY['formation']                                                     AS "types",
        CAST(NULL AS TEXT [])                                                  AS "frais"
    FROM formations
)

SELECT * FROM final
