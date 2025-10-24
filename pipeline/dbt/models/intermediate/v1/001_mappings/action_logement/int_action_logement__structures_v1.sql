WITH structures AS (
    SELECT * FROM {{ ref('stg_action_logement__structures') }}
),

final AS (
    SELECT
        'action-logement'         AS "source",
        'action-logement--' || id AS "id",
        'action-logement--' || id AS "adresse_id",
        nom                       AS "nom",
        date_maj                  AS "date_maj",
        lien_source               AS "lien_source",
        siret                     AS "siret",
        telephone                 AS "telephone",
        NULL                      AS "courriel",
        site_web                  AS "site_web",
        description               AS "description",
        horaires_accueil          AS "horaires_accueil",
        NULL                      AS "accessibilite_lieu",
        reseaux_porteurs          AS "reseaux_porteurs"
    FROM structures
)

SELECT * FROM final
