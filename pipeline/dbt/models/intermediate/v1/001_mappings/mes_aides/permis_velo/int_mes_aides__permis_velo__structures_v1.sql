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
        'mes-aides'                                                        AS "source",
        'mes-aides--' || permis_velo.id                                    AS "id",
        'mes-aides--' || permis_velo.id                                    AS "adresse_id",
        permis_velo.siret                                                  AS "siret",
        permis_velo.nom                                                    AS "nom",
        permis_velo.description                                            AS "description",
        SUBSTRING(permis_velo.contact_telephone FROM '\+?\d[\d\.\-\s]*\d') AS "telephone",
        permis_velo.contact_email                                          AS "courriel",
        permis_velo.site                                                   AS "site_web",
        permis_velo.url_mes_aides                                          AS "lien_source",
        NULL                                                               AS "horaires_accueil",
        NULL                                                               AS "accessibilite_lieu",
        NULLIF(ARRAY[CASE
            WHEN permis_velo.organisme_type IN ('Communauté de Communes', 'Intercommunalité', 'Commune')
                THEN 'communes'
            WHEN permis_velo.organisme_type = 'Département'
                THEN 'departements'
            WHEN permis_velo.organisme_type IN ('Garage', 'Réseau de garages')
                THEN 'garages-solidaires'
            WHEN permis_velo.organisme_type = 'Mission locale'
                THEN 'mission-locale'
            WHEN permis_velo.organisme_type = 'Régime de protection sociale'
                THEN 'cpam'
        END], '{}')                                                        AS "reseaux_porteurs",
        permis_velo.modifie_le                                             AS "date_maj"
    FROM permis_velo
    LEFT JOIN thematiques ON permis_velo.id = thematiques.service_id
    WHERE
        permis_velo.slug_organisme NOT IN ('action-logement', 'france-travail')
        AND permis_velo.en_ligne
)

SELECT * FROM final
