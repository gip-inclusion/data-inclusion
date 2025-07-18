WITH structures AS (
    SELECT * FROM {{ ref('stg_action_logement__structures') }}
),

final AS (
    SELECT
        accessibilite          AS "accessibilite",
        id                     AS "adresse_id",
        courriel               AS "courriel",
        horaires_ouverture     AS "horaires_ouverture",
        id                     AS "id",
        labels_autres          AS "labels_autres",
        labels_nationaux       AS "labels_nationaux",
        NULL                   AS "lien_source",
        nom                    AS "nom",
        presentation_detail    AS "presentation_detail",
        presentation_resume    AS "presentation_resume",
        rna                    AS "rna",
        siret                  AS "siret",
        site_web               AS "site_web",
        _di_source_id          AS "source",
        telephone              AS "telephone",
        typologie              AS "typologie",
        CAST(date_maj AS DATE) AS "date_maj",
        CAST(NULL AS TEXT [])  AS "thematiques"
    FROM structures
)

SELECT * FROM final
