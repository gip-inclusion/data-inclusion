WITH structures AS (
    SELECT * FROM {{ ref('int__union_structures') }}
),

map_reseaux_labels AS (SELECT * FROM {{ ref('_map_reseaux_labels') }}),

map_reseaux_typologie AS (SELECT * FROM {{ ref('_map_reseaux_typologie') }}),

reseaux_porteurs AS (
    SELECT
        structures._di_surrogate_id,
        ARRAY_REMOVE(ARRAY(
            SELECT DISTINCT map_reseaux_labels.reseau_porteur
            FROM UNNEST(structures.labels_nationaux) AS x (label_national)
            LEFT JOIN map_reseaux_labels ON x.label_national = map_reseaux_labels.label_national
            UNION
            SELECT map_reseaux_typologie.reseau_porteur
        ), NULL) AS reseaux_porteurs
    FROM structures
    LEFT JOIN map_reseaux_typologie ON structures.typologie = map_reseaux_typologie.typologie
)

SELECT
    structures._di_surrogate_id                           AS "_di_surrogate_id",
    structures.courriel                                   AS "courriel",
    structures.horaires_ouverture                         AS "horaires_accueil",
    structures.horaires_ouverture                         AS "horaires_ouverture",
    structures.id                                         AS "id",
    structures.labels_autres                              AS "labels_autres",
    structures.labels_nationaux                           AS "labels_nationaux",
    structures.lien_source                                AS "lien_source",
    CASE
        WHEN LENGTH(structures.presentation_detail) >= 2000
            THEN LEFT(structures.presentation_detail, 1999) || '…'
        ELSE COALESCE(structures.presentation_detail, structures.presentation_resume)
    END                                                   AS "description",
    structures.presentation_detail                        AS "presentation_detail",
    structures.presentation_resume                        AS "presentation_resume",
    structures.rna                                        AS "rna",
    structures.siret                                      AS "siret",
    structures.source                                     AS "source",
    structures.typologie                                  AS "typologie",
    structures.date_maj                                   AS "date_maj",
    structures.thematiques                                AS "thematiques",
    reseaux_porteurs.reseaux_porteurs                     AS "reseaux_porteurs",
    processings.format_phone_number(structures.telephone) AS "telephone",
    structures.site_web                                   AS "site_web",
    structures.accessibilite                              AS "accessibilite",
    structures.nom                                        AS "nom",
    structures.longitude                                  AS "longitude",
    structures.latitude                                   AS "latitude",
    structures.complement_adresse                         AS "complement_adresse",
    structures.commune                                    AS "commune",
    structures.adresse                                    AS "adresse",
    structures.code_postal                                AS "code_postal",
    structures.code_insee                                 AS "code_insee"
FROM structures
LEFT JOIN reseaux_porteurs ON structures._di_surrogate_id = reseaux_porteurs._di_surrogate_id
