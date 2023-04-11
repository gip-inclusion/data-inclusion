WITH services AS (
    SELECT * FROM {{ ref('stg_mediation_numerique__services') }}
),

final AS (
    SELECT
        id            AS "id",
        nom           AS "nom",
        NULL          AS "presentation_resume",
        NULL          AS "presentation_detail",
        types         AS "types",
        prise_rdv     AS "prise_rdv",
        frais         AS "frais",
        NULL::TEXT    AS "frais_autres",
        profils       AS "profils",
        structure_id  AS "structure_id",
        thematiques   AS "thematiques",
        _di_source_id AS "source",
        NULL          AS "pre_requis",
        NULL          AS "cumulable",
        NULL          AS "justificatifs",
        NULL          AS "formulaire_en_ligne",
        NULL          AS "commune",
        NULL          AS "code_postal",
        NULL          AS "code_insee",
        NULL          AS "adresse",
        NULL          AS "complement_adresse",
        NULL          AS "longitude",
        NULL          AS "latitude",
        NULL          AS "recurrence",
        NULL          AS "date_creation",
        NULL          AS "date_suspension",
        NULL          AS "lien_source",
        NULL          AS "telephone",
        NULL          AS "courriel",
        NULL          AS "contact_public",
        NULL          AS "date_maj",
        NULL::TEXT[]  AS "modes_accueil",
        NULL          AS "zone_diffusion_type",
        NULL          AS "zone_diffusion_code",
        NULL          AS "zone_diffusion_nom"
    FROM services
)

SELECT * FROM final
