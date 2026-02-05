WITH structures AS (
    SELECT * FROM {{ ref('stg_dora__structures') }}
),

adresses AS (
    SELECT * FROM {{ ref('int_dora__adresses_v1') }}
),

reseaux_from_labels AS (
    SELECT DISTINCT
        labels.structure_id,
        map.reseau_porteur
    FROM {{ ref('stg_dora__structures__labels_nationaux') }} AS labels
    INNER JOIN {{ ref('_map_dora__reseaux_porteurs__labels_nationaux') }} AS map
        ON labels.item = map.label_national
),

reseaux_from_typologie AS (
    SELECT
        structures.id AS "structure_id",
        map.reseau_porteur
    FROM structures
    INNER JOIN {{ ref('_map_dora__reseaux_porteurs_typologie') }} AS map
        ON structures.typologie = map.typologie
),

reseaux_porteurs AS (
    SELECT
        structure_id,
        ARRAY_AGG(DISTINCT reseau_porteur ORDER BY reseau_porteur) AS "reseaux_porteurs"
    FROM (
        SELECT * FROM reseaux_from_labels
        UNION
        SELECT * FROM reseaux_from_typologie
    )
    GROUP BY structure_id
),

final AS (
    SELECT
        'dora'                            AS "source",
        'dora--' || structures.id         AS "id",
        adresses.id                       AS "adresse_id",
        structures.nom                    AS "nom",
        CASE
            WHEN LENGTH(structures.presentation_detail) >= 10000
                THEN LEFT(structures.presentation_detail, 9999) || '…'
            ELSE COALESCE(structures.presentation_detail, structures.presentation_resume)
        END                               AS "description",
        structures.siret                  AS "siret",
        CAST(structures.date_maj AS DATE) AS "date_maj",
        structures.lien_source            AS "lien_source",
        structures.telephone              AS "telephone",
        structures.courriel               AS "courriel",
        structures.site_web               AS "site_web",
        structures.horaires_ouverture     AS "horaires_accueil",
        structures.accessibilite          AS "accessibilite_lieu",
        reseaux_porteurs.reseaux_porteurs AS "reseaux_porteurs"
    FROM structures
    LEFT JOIN adresses ON ('dora--' || structures.id) = adresses.id
    LEFT JOIN reseaux_porteurs ON structures.id = reseaux_porteurs.structure_id
)

SELECT * FROM final
