WITH offres AS (
    SELECT * FROM {{ ref('stg_mission_locale__offres') }}
),

structures_offres AS (
    SELECT * FROM {{ ref('stg_mission_locale__structures_offres') }}
),

structures AS (
    SELECT * FROM {{ ref('stg_mission_locale__structures') }}
),

offres__liste_des_profils AS (
    SELECT * FROM {{ ref('stg_mission_locale__offres__liste_des_profils') }}
),

mapping_profils AS (
    SELECT x.*
    FROM (
        VALUES
        ('jeunes de 16 à 25 ans', 'jeunes-16-26'),
        ('rqth moins de 30 ans', 'personnes-handicapees')
    ) AS x (profil_mission_locale, profil_di)
),

final AS (
    SELECT
        offres._di_source_id                                                      AS "source",
        structures_offres.missionlocale_id || '-' || structures_offres.offre_id   AS "id",
        structures_offres.missionlocale_id                                        AS "adresse_id",
        structures_offres.missionlocale_id                                        AS "structure_id",
        structures.email                                                          AS "courriel",
        NULL                                                                      AS "contact_nom_prenom",
        CAST(offres.date_maj AS DATE)                                             AS "date_maj",
        NULL                                                                      AS "formulaire_en_ligne",
        NULL                                                                      AS "frais_autres",
        CAST(NULL AS TEXT [])                                                     AS "justificatifs",
        NULL                                                                      AS "lien_source",
        ARRAY[offres.modes_accueil]                                               AS "modes_accueil",
        CAST(NULL AS TEXT [])                                                     AS "modes_orientation_accompagnateur",
        NULL                                                                      AS "modes_orientation_accompagnateur_autres",
        ARRAY[offres.modes_orientation_beneficiaire]                              AS "modes_orientation_beneficiaire",
        CAST(NULL AS TEXT [])                                                     AS "modes_mobilisation",
        CAST(NULL AS TEXT [])                                                     AS "mobilisable_par",
        NULL                                                                      AS "mobilisation_precisions",
        NULL                                                                      AS "modes_orientation_beneficiaire_autres",
        offres.nom_dora                                                           AS "nom",
        NULL                                                                      AS "page_web",
        offres.presentation                                                       AS "presentation_detail",
        offres.presentation                                                       AS "presentation_resume",
        NULL                                                                      AS "prise_rdv",
        ARRAY(
            SELECT mapping_profils.profil_di
            FROM offres__liste_des_profils
            INNER JOIN mapping_profils ON offres__liste_des_profils.value = mapping_profils.profil_mission_locale
            WHERE offres.id_offre = offres__liste_des_profils.id_offre
        )                                                                         AS "profils",
        NULL                                                                      AS "profils_precisions",
        CAST(NULL AS TEXT [])                                                     AS "pre_requis",
        NULL                                                                      AS "recurrence",
        CASE WHEN offres.thematique IS NOT NULL THEN ARRAY[offres.thematique] END AS "thematiques",
        CASE WHEN offres.type_offre IS NOT NULL THEN ARRAY[offres.type_offre] END AS "types",
        structures.telephone                                                      AS "telephone",
        CASE WHEN offres.frais IS NOT NULL THEN ARRAY[offres.frais] END           AS "frais",
        offres.perimetre_offre                                                    AS "zone_diffusion_type",
        NULL                                                                      AS "zone_diffusion_code",
        NULL                                                                      AS "zone_diffusion_nom",
        CAST(NULL AS FLOAT)                                                       AS "volume_horaire_hebdomadaire",
        CAST(NULL AS INT)                                                         AS "nombre_semaines"
    FROM structures_offres
    INNER JOIN offres
        ON structures_offres.offre_id = offres.id_offre
    LEFT JOIN structures
        ON structures_offres.missionlocale_id = structures.id_structure
)

SELECT * FROM final
