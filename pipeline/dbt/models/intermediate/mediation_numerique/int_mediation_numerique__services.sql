WITH services AS (
    SELECT * FROM {{ ref('stg_mediation_numerique__services') }}
),

final AS (
    SELECT
        id            AS "id",
        nom           AS "nom",
        NULL          AS "presentation_resume",
        types         AS "types",
        NULL          AS "prise_rdv",
        frais         AS "frais",
        NULL::TEXT    AS "frais_autres",
        profils       AS "profils",
        structure_id  AS "structure_id",
        thematiques   AS "thematiques",
        _di_source_id AS "source"
    FROM services
)

SELECT * FROM final
