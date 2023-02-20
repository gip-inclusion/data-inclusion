WITH structures AS (
    SELECT * FROM {{ ref('stg_mediation_numerique__structures') }}
),

final AS (
    SELECT
        id                 AS "id",
        siret              AS "siret",
        NULL::BOOLEAN      AS "antenne",
        NULL               AS "rna",
        nom                AS "nom",
        commune            AS "commune",
        code_postal        AS "code_postal",
        NULL               AS "code_insee",
        adresse            AS "adresse",
        NULL               AS "complement_adresse",
        longitude          AS "longitude",
        latitude           AS "latitude",
        telephone          AS "telephone",
        courriel           AS "courriel",
        site_web           AS "site_web",
        NULL               AS "lien_source",
        horaires_ouverture AS "horaires_ouverture",
        NULL               AS "accessibilite",
        labels_nationaux       AS "labels_nationaux",
        NULL::TEXT[]       AS "labels_autres",
        thematiques        AS "thematiques",
        NULL               AS "typologie",
        NULL               AS "presentation_resume",
        NULL               AS "presentation_detail",
        date_maj           AS "date_maj",
        CASE
            WHEN source ~* 'assembleurs' THEN 'mediation-numerique-assembleurs'
            WHEN source ~* 'maine-et-loire' THEN 'mediation-numerique-cd49'
            WHEN source ~* 'hinaura' THEN 'mediation-numerique-hinaura'
            WHEN source ~* 'francil-in' THEN 'mediation-numerique-francilin'
            WHEN source ~* 'france tiers-lieux' THEN 'mediation-numerique-france-tiers-lieux'
            WHEN source ~* 'angers' THEN 'mediation-numerique-angers'
            WHEN source ~* 'france services' THEN 'mediation-numerique-france-services'
            WHEN source ~* 'conseiller numerique' THEN 'mediation-numerique-conseiller-numerique'
        END                AS "source"
    FROM structures
)

SELECT * FROM final
