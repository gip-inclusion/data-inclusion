WITH permis_velo AS (
    SELECT * FROM {{ ref('stg_mes_aides__permis_velo') }}
),

mapping_thematiques AS (
    SELECT * FROM {{ ref('_map_permis_velo_thematiques') }}
),

thematiques AS (
    SELECT
        ARRAY_AGG(mapping_thematiques.correspondance_di) AS thematiques,
        permis_velo.id                                   AS service_id
    FROM permis_velo
    INNER JOIN mapping_thematiques ON mapping_thematiques.besoins = ANY(permis_velo.liaisons_besoins_mes_aides)
    WHERE permis_velo.slug_thematique_mes_aides = mapping_thematiques.thematiques
    GROUP BY permis_velo.id
),

final AS (
    SELECT
        permis_velo.id                        AS "id",
        permis_velo.id                        AS "adresse_id",
        permis_velo.siret_structure           AS "siret",
        CAST(NULL AS BOOLEAN)                 AS "antenne",
        NULL                                  AS "rna",
        permis_velo.nom_organisme_structure   AS "nom",
        permis_velo.contact_telephone         AS "telephone",
        permis_velo.contact_email             AS "courriel",
        permis_velo.site                      AS "site_web",
        permis_velo._di_source_id             AS "source",
        permis_velo.url_mes_aides             AS "lien_source",
        NULL                                  AS "horaires_ouverture",
        NULL                                  AS "accessibilite",
        CASE
            WHEN permis_velo.typologie_structure LIKE '%Mission locale%' THEN ARRAY['mission-locale']
            ELSE CAST(NULL AS TEXT [])
        END                                   AS "labels_nationaux",
        CAST(NULL AS TEXT [])                 AS "labels_autres",
        NULL                                  AS "typologie",
        NULL                                  AS "presentation_resume",
        NULL                                  AS "presentation_detail",
        CAST(permis_velo.modifiee_le AS DATE) AS "date_maj",
        thematiques.thematiques               AS "thematiques"
    FROM permis_velo
    LEFT JOIN thematiques ON permis_velo.id = thematiques.service_id
    WHERE permis_velo.slug_organisme_structure NOT IN ('action-logement', 'france-travail')

)

SELECT * FROM final
