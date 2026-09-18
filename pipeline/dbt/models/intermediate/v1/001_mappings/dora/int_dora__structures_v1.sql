WITH structures AS (
    SELECT * FROM {{ ref('stg_dora__structures') }}
),

adresses AS (
    SELECT * FROM {{ ref('int_dora__adresses_v1') }}
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
        structures.reseaux_porteurs       AS "reseaux_porteurs"
    FROM structures
    LEFT JOIN adresses ON ('dora--' || structures.id) = adresses.id
)

SELECT * FROM final
