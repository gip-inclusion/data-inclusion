WITH structures AS (
    SELECT * FROM {{ ref('stg_reseau_alpha__structures') }}
),

structures_adresses AS (
    SELECT DISTINCT ON (structure_id) *
    FROM {{ ref('stg_reseau_alpha__structures__adresses') }}
    ORDER BY structure_id, voie IS NOT NULL DESC
),

pages AS (
    SELECT * FROM {{ ref('stg_reseau_alpha__pages') }}
),

structures__contacts AS (
    SELECT DISTINCT ON (structure_id) *
    FROM {{ ref('stg_reseau_alpha__structures__contacts') }}
    ORDER BY structure_id, telephone1 IS NOT NULL DESC, email IS NOT NULL DESC
),

final AS (
    SELECT
        'reseau-alpha'                                                             AS "source",
        'reseau-alpha--' || structures.id                                          AS "id",
        'reseau-alpha--' || 'structure-' || structures_adresses.structure_id       AS "adresse_id",
        structures.nom                                                             AS "nom",
        pages.date_derniere_modification                                           AS "date_maj",
        structures.url                                                             AS "lien_source",
        NULL                                                                       AS "siret",
        COALESCE(structures__contacts.telephone1, structures__contacts.telephone2) AS "telephone",
        structures__contacts.email                                                 AS "courriel",
        pages.url                                                                  AS "site_web",
        structures.description                                                     AS "description",
        NULL                                                                       AS "horaires_accueil",
        NULL                                                                       AS "accessibilite_lieu",
        NULL                                                                       AS "reseaux_porteurs"
    FROM structures
    LEFT JOIN pages ON structures.id = pages.structure_id
    LEFT JOIN structures__contacts ON structures.id = structures__contacts.structure_id
    LEFT JOIN structures_adresses ON structures.id = structures_adresses.structure_id
)

SELECT * FROM final
