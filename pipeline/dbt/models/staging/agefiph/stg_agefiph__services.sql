WITH source AS (
    SELECT * FROM {{ source('agefiph', 'services') }}
),

final AS (
    SELECT
        _di_source_id                  AS "_di_source_id",
        data ->> 'structure_id'        AS "structure_id",
        data ->> 'courriel'            AS "courriel",
        data ->> 'telephone'           AS "telephone",
        data ->> 'adresse'             AS "adresse",
        data ->> 'commune'             AS "commune",
        data ->> 'code_postal'         AS "code_postal",
        data ->> 'code_insee'          AS "code_insee",
        data ->> 'zone_diffusion_type' AS "zone_diffusion_type",
        data ->> 'zone_diffusion_code' AS "zone_diffusion_code",
        data ->> 'id'                  AS "id",
        data ->> 'date_creation'       AS "date_creation",
        data ->> 'date_maj'            AS "date_maj",
        data ->> 'nom'                 AS "nom",
        data ->> 'presentation_resume' AS "presentation_resume",
        data ->> 'presentation_detail' AS "presentation_detail",
        data ->> 'contact_public'      AS "contact_public",
        data ->> 'id_thematiques'      AS "id_thematiques",
        data ->> 'thematiques'         AS "thematiques"


    FROM source
)

SELECT * FROM final
