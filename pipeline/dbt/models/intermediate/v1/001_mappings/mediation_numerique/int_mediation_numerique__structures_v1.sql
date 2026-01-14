WITH structures AS (
    SELECT * FROM {{ ref('stg_mediation_numerique__structures') }}
),

reseaux_porteurs AS (
    SELECT
        structures.id,
        ARRAY_AGG(DISTINCT map.reseau_porteur ORDER BY map.reseau_porteur) AS "reseaux_porteurs"
    FROM structures
    CROSS JOIN LATERAL UNNEST(structures.dispositif_programmes_nationaux) AS d (dispositif)
    INNER JOIN {{ ref('_map_mediation_numerique__reseaux_porteurs') }} AS map
        ON d.dispositif = map.dispositif
    WHERE map.reseau_porteur IS NOT NULL
    GROUP BY structures.id
),

final AS (
    SELECT
        'mediation-numerique'                                                                        AS "source",
        'mediation-numerique--' || structures.id                                                     AS "id",
        'mediation-numerique--' || structures.id                                                     AS "adresse_id",
        structures.nom                                                                               AS "nom",
        CAST(structures.date_maj AS DATE)                                                            AS "date_maj",
        'https://cartographie.societenumerique.gouv.fr/cartographie/' || structures.id || '/details' AS "lien_source",
        structures.siret                                                                             AS "siret",
        structures.telephone                                                                         AS "telephone",
        structures.courriel                                                                          AS "courriel",
        structures.site_web                                                                          AS "site_web",
        COALESCE(structures.presentation_detail, structures.presentation_resume)                     AS "description",
        structures.horaires                                                                          AS "horaires_accueil",
        NULL                                                                                         AS "accessibilite_lieu",
        reseaux_porteurs.reseaux_porteurs                                                            AS "reseaux_porteurs"
    FROM structures
    LEFT JOIN reseaux_porteurs ON structures.id = reseaux_porteurs.id
)

SELECT * FROM final
