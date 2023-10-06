{% set source_model = source('cd72', 'services') %}

{% set table_exists = adapter.get_relation(database=source_model.database, schema=source_model.schema, identifier=source_model.name) is not none %}

{% if table_exists %}

WITH source AS (
    SELECT * FROM {{ source_model }}
),

{% else %}

    WITH source AS (
        SELECT
            NULL                AS "_di_source_id",
            CAST(NULL AS JSONB) AS "data"
        WHERE FALSE
    ),

{% endif %}

structures AS (
    SELECT * FROM {{ ref('stg_cd72__structures') }}
),

final AS (
    SELECT
        _di_source_id                                                                                AS "_di_source_id",
        data ->> 'id'                                                                                AS "id",
        data ->> 'nom'                                                                               AS "nom",
        data ->> 'lieu'                                                                              AS "lieu",
        data ->> 'siret'                                                                             AS "siret",
        -- TODO: frais, change column type from bool to ref list on grist
        data ->> 'adresse'                                                                           AS "adresse",
        data ->> 'commune'                                                                           AS "commune",
        (SELECT ARRAY_AGG(TRIM(p)) FROM UNNEST(STRING_TO_ARRAY(data ->> 'profils', ',')) AS "p")     AS "profils",
        data ->> 'courriel'                                                                          AS "courriel",
        TO_DATE(data ->> 'date_maj', 'YYYY-MM-DD')                                                   AS "date_maj",
        data ->> 'telephone'                                                                         AS "telephone",
        data ->> 'pre_requis'                                                                        AS "pre_requis",
        data ->> 'recurrence'                                                                        AS "recurrence",
        data ->> 'code_postal'                                                                       AS "code_postal",
        data ->> 'contact_nom_prenom'                                                                AS "contact_nom_prenom",
        data ->> 'frais_autres'                                                                      AS "frais_autres",
        (SELECT ARRAY_AGG(TRIM(t)) FROM UNNEST(STRING_TO_ARRAY(data ->> 'thematiques', ',')) AS "t") AS "thematiques",
        data ->> 'structure_id'                                                                      AS "structure_id",
        TO_DATE(data ->> 'date_creation', 'YYYY-MM-DD')                                              AS "date_creation",
        TO_DATE(data ->> 'date_suspension', 'YYYY-MM-DD')                                            AS "date_suspension",
        data ->> 'zone_diffusion_nom'                                                                AS "zone_diffusion_nom",
        data ->> 'presentation_detail'                                                               AS "presentation_detail",
        data ->> 'presentation_resume'                                                               AS "presentation_resume",
        data ->> 'zone_diffusion_code'                                                               AS "zone_diffusion_code",
        data ->> 'zone_diffusion_type'                                                               AS "zone_diffusion_type",
        data ->> 'modes_orientation_beneficiaire_autres'                                             AS "modes_orientation_beneficiaire_autres"
    FROM source
    WHERE
        data ->> 'structure_id' IS NOT NULL
        AND data ->> 'structure_id' IN (SELECT id FROM structures)
)

SELECT * FROM final
