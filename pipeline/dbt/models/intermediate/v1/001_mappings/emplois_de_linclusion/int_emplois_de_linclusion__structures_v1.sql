WITH structures AS (
    SELECT * FROM {{ ref('stg_emplois_de_linclusion__siaes') }}
    UNION
    SELECT * FROM {{ ref('stg_emplois_de_linclusion__organisations') }}
),

reseaux_porteurs_mapping AS (
    SELECT * FROM {{ ref('_map_emplois__reseaux_porteurs_v1') }}
),

final AS (
    SELECT
        'emplois-de-linclusion--' || structures.id AS "id",
        'emplois-de-linclusion--' || structures.id AS "adresse_id",
        'emplois-de-linclusion'                    AS "source",
        structures.nom                             AS "nom",
        structures.date_maj                        AS "date_maj",
        structures.kind                            AS "kind",
        structures.telephone                       AS "telephone",
        structures.courriel                        AS "courriel",
        structures.site_web                        AS "site_web",
        structures.siret                           AS "siret",
        structures.lien_source                     AS "lien_source",
        structures.description                     AS "description",
        reseaux_porteurs_mapping.reseaux_porteurs  AS "reseaux_porteurs",
        NULL                                       AS "horaires_accueil",
        NULL                                       AS "accessibilite_lieu",
        NULL                                       AS "contact_nom_prenom"
    FROM structures
    LEFT JOIN reseaux_porteurs_mapping
        ON structures.kind = reseaux_porteurs_mapping.kind
)

SELECT * FROM final
