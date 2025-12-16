WITH structures AS (
    SELECT * FROM {{ ref('int_dora__structures_v0') }}
),

map_reseaux_labels AS (SELECT * FROM {{ ref('_map_reseaux_labels') }}),

map_reseaux_typologie AS (SELECT * FROM {{ ref('_map_reseaux_typologie') }}),

reseaux_porteurs AS (
    SELECT
        structures.id AS "structure_id",
        ARRAY_REMOVE(ARRAY(
            SELECT DISTINCT map_reseaux_labels.reseau_porteur
            FROM UNNEST(structures.labels_nationaux) AS x (label_national)
            LEFT JOIN map_reseaux_labels ON x.label_national = map_reseaux_labels.label_national
            UNION
            SELECT map_reseaux_typologie.reseau_porteur
        ), NULL)      AS "reseaux_porteurs"
    FROM structures
    LEFT JOIN map_reseaux_typologie ON structures.typologie = map_reseaux_typologie.typologie
),

final AS (
    SELECT
        'dora'                            AS "source",
        'dora--' || structures.id         AS "id",
        'dora--' || structures.id         AS "adresse_id",
        structures.courriel               AS "courriel",
        structures.horaires_ouverture     AS "horaires_accueil",
        structures.lien_source            AS "lien_source",
        CASE
            WHEN LENGTH(structures.presentation_detail) >= 10000
                THEN LEFT(structures.presentation_detail, 9999) || '…'
            ELSE COALESCE(structures.presentation_detail, structures.presentation_resume)
        END                               AS "description",
        structures.siret                  AS "siret",
        CAST(structures.date_maj AS DATE) AS "date_maj",
        reseaux_porteurs.reseaux_porteurs AS "reseaux_porteurs",
        structures.telephone              AS "telephone",
        structures.site_web               AS "site_web",
        structures.accessibilite          AS "accessibilite_lieu",
        structures.nom                    AS "nom"
    FROM structures
    LEFT JOIN reseaux_porteurs ON structures.id = reseaux_porteurs.structure_id
)

SELECT * FROM final
