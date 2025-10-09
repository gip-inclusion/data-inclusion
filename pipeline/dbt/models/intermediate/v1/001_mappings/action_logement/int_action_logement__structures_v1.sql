WITH structures AS (
    SELECT * FROM {{ ref('stg_action_logement__structures') }}
),

final AS (
    SELECT
        'action-logement'         AS "source",
        'action-logement--' || id AS "id",
        'action-logement--' || id AS "adresse_id",
        NULL                      AS "nom",
        CAST(NULL AS DATE)        AS "date_maj",
        NULL                      AS "description",
        NULL                      AS "lien_source",
        NULL                      AS "siret",
        NULL                      AS "telephone",
        NULL                      AS "courriel",
        NULL                      AS "site_web",
        NULL                      AS "horaires_accueil",
        NULL                      AS "accessibilite_lieu",
        CAST(NULL AS TEXT [])     AS "reseaux_porteurs"
    FROM structures
)

SELECT * FROM final
