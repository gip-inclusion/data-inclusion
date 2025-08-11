WITH permis_velo AS (
    SELECT * FROM {{ ref('stg_mes_aides__permis_velo') }}
),

liaisons_besoins AS (
    SELECT * FROM {{ ref('stg_mes_aides__permis_velo__liaisons_besoins') }}
),

mapping_thematiques AS (
    SELECT * FROM {{ ref('_map_permis_velo_thematiques') }}
),

thematiques AS (
    SELECT
        permis_velo.id                                            AS service_id,
        ARRAY_AGG(DISTINCT mapping_thematiques.correspondance_di) AS thematiques
    FROM permis_velo
    INNER JOIN liaisons_besoins ON permis_velo.id = liaisons_besoins.service_id
    INNER JOIN mapping_thematiques ON liaisons_besoins.value = mapping_thematiques.besoins
    GROUP BY permis_velo.id
),

final AS (
    SELECT
        permis_velo.id                                                     AS "id",
        permis_velo.id                                                     AS "adresse_id",
        permis_velo.siret                                                  AS "siret",
        NULL                                                               AS "rna",
        permis_velo.nom_organisme                                          AS "nom",
        SUBSTRING(permis_velo.contact_telephone FROM '\+?\d[\d\.\-\s]*\d') AS "telephone",
        permis_velo.contact_email                                          AS "courriel",
        permis_velo.site                                                   AS "site_web",
        'mes-aides'                                                        AS "source",
        permis_velo.url_mes_aides                                          AS "lien_source",
        NULL                                                               AS "horaires_ouverture",
        NULL                                                               AS "accessibilite",
        CASE
            WHEN permis_velo.organisme_type ~* 'mission locale' THEN ARRAY['mission-locale']
            ELSE CAST(NULL AS TEXT [])
        END                                                                AS "labels_nationaux",
        CAST(NULL AS TEXT [])                                              AS "labels_autres",
        NULL                                                               AS "typologie",
        NULL                                                               AS "presentation_resume",
        NULL                                                               AS "presentation_detail",
        permis_velo.modifie_le                                             AS "date_maj",
        thematiques.thematiques                                            AS "thematiques"
    FROM permis_velo
    LEFT JOIN thematiques ON permis_velo.id = thematiques.service_id
    WHERE permis_velo.slug_organisme NOT IN ('action-logement', 'france-travail')
)

SELECT * FROM final
