WITH structures AS (
    SELECT * FROM {{ ref('stg_mediation_numerique__structures') }}
),

final AS (
    SELECT
        id                                                                                                           AS "id",
        id                                                                                                           AS "adresse_id",
        siret                                                                                                        AS "siret",
        NULL                                                                                                         AS "rna",
        nom                                                                                                          AS "nom",
        telephone                                                                                                    AS "telephone",
        courriel                                                                                                     AS "courriel",
        site_web                                                                                                     AS "site_web",
        NULL                                                                                                         AS "lien_source",
        horaires_ouverture                                                                                           AS "horaires_ouverture",
        accessibilite                                                                                                AS "accessibilite",
        labels_nationaux                                                                                             AS "labels_nationaux",
        thematiques                                                                                                  AS "thematiques",
        typologie                                                                                                    AS "typologie",
        presentation_resume                                                                                          AS "presentation_resume",
        {{ truncate_text("presentation_detail") }} AS "presentation_detail",
        CAST(date_maj AS DATE)                                                                                       AS "date_maj",
        _di_source_id                                                                                                AS "source",
        labels_autres                                                                                                AS "labels_autres",
        CAST(NULL AS BOOLEAN)                                                                                        AS "antenne"
    FROM structures
)

SELECT * FROM final
