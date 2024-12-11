WITH offres AS (
    SELECT * FROM {{ ref('stg_imilo__offres') }}
),

structures_offres AS (
    SELECT * FROM {{ ref('stg_imilo__structures_offres') }}
),

offres__liste_des_profils AS (
    SELECT * FROM {{ ref('stg_imilo__offres__liste_des_profils') }}
),

mapping_profils AS (
    SELECT x.*
    FROM (
        VALUES
        ('jeunes de 16 à 25 ans', 'jeunes-16-26'),
        ('rqth moins de 30 ans', 'personnes-handicapees')
    ) AS x (profil_imilo, profil_di)
),

final AS (
    SELECT
        offres._di_source_id                                                     AS "source",
        structures_offres.missionlocale_id || '--' || structures_offres.offre_id AS "id",
        structures_offres.missionlocale_id                                       AS "adresse_id",
        structures_offres.missionlocale_id                                       AS "structure_id",
        NULL                                                                     AS "courriel",
        CAST(NULL AS BOOLEAN)                                                    AS "cumulable",
        CAST(NULL AS BOOLEAN)                                                    AS "contact_public",
        NULL                                                                     AS "contact_nom_prenom",
        CAST(offres.date_maj AS DATE)                                            AS "date_maj",
        CAST(offres.date_import AS DATE)                                         AS "date_creation",
        NULL                                                                     AS "formulaire_en_ligne",
        NULL                                                                     AS "frais_autres",
        CAST(NULL AS TEXT [])                                                    AS "justificatifs",
        NULL                                                                     AS "lien_source",
        CAST(NULL AS TEXT [])                                                    AS "modes_accueil",
        CAST(NULL AS TEXT [])                                                    AS "modes_orientation_accompagnateur",
        NULL                                                                     AS "modes_orientation_accompagnateur_autres",
        ARRAY[offres.modes_orientation_beneficiaire]                             AS "modes_orientation_beneficiaire",
        NULL                                                                     AS "modes_orientation_beneficiaire_autres",
        offres.nom_dora                                                          AS "nom",
        NULL                                                                     AS "page_web",
        NULL                                                                     AS "presentation_detail",
        offres.presentation                                                      AS "presentation_resume",
        NULL                                                                     AS "prise_rdv",
        ARRAY(
            SELECT mapping_profils.profil_di
            FROM offres__liste_des_profils
            INNER JOIN mapping_profils ON offres__liste_des_profils.value = mapping_profils.profil_imilo
            WHERE offres.id_offre = offres__liste_des_profils.id_offre
        )                                                                        AS "profils",
        NULL                                                                     AS "profils_precisions",
        CAST(NULL AS TEXT [])                                                    AS "pre_requis",
        NULL                                                                     AS "recurrence",
        ARRAY[offres.thematique]                                                 AS "thematiques",
        CAST(NULL AS TEXT [])                                                    AS "types",
        NULL                                                                     AS "telephone",
        ARRAY[offres.frais]                                                      AS "frais",
        offres.perimetre_offre                                                   AS "zone_diffusion_type",
        NULL                                                                     AS "zone_diffusion_code",
        NULL                                                                     AS "zone_diffusion_nom",
        CAST(NULL AS DATE)                                                       AS "date_suspension"
    FROM structures_offres
    INNER JOIN offres
        ON structures_offres.offre_id = offres.id_offre
)

SELECT * FROM final
