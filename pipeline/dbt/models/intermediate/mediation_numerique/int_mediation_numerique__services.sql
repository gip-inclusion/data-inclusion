WITH services AS (
    SELECT * FROM {{ ref('stg_mediation_numerique__services') }}
),

final AS (
    SELECT
        id           AS "id",
        nom          AS "nom",
        NULL         AS "presentation_resume",
        types        AS "types",
        NULL         AS "prise_rdv",
        frais        AS "frais",
        NULL::TEXT   AS "frais_autres",
        profils      AS "profils",
        structure_id AS "structure_id",
        thematiques  AS "thematiques",
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
    FROM services
)

SELECT * FROM final
