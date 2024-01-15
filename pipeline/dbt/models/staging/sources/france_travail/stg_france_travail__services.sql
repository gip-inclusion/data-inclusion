WITH source AS (
    {{ stg_source_header('france_travail', 'services') }}
),

final AS (
    SELECT
        _di_source_id                                                                                                          AS "_di_source_id",
        TRUE                                                                                                                   AS "cumulable",
        data ->> 'id'                                                                                                          AS "id",
        CURRENT_DATE AT TIME ZONE 'Europe/Paris'                                                                               AS "date_maj",
        ARRAY_REMOVE(ARRAY(SELECT value FROM JSONB_EACH_TEXT(data) WHERE key ~* 'modes_accueil.\d+'), NULL)                    AS "modes_accueil",
        ARRAY_REMOVE(ARRAY(SELECT value FROM JSONB_EACH_TEXT(data) WHERE key ~* 'profils.\d+'), NULL)                          AS "profils",
        ARRAY_REMOVE(ARRAY(SELECT value FROM JSONB_EACH_TEXT(data) WHERE key ~* 'thematiques.\d+'), NULL)                      AS "thematiques",
        ARRAY_REMOVE(ARRAY(SELECT value FROM JSONB_EACH_TEXT(data) WHERE key ~* 'types.\d+'), NULL)                            AS "types",
        ARRAY_REMOVE(ARRAY(SELECT value FROM JSONB_EACH_TEXT(data) WHERE key ~* 'justificatifs.\d+'), NULL)                    AS "justificatifs",
        ARRAY_REMOVE(ARRAY(SELECT value FROM JSONB_EACH_TEXT(data) WHERE key ~* 'pre_requis.\d+'), NULL)                       AS "pre_requis",
        ARRAY_REMOVE(ARRAY(SELECT value FROM JSONB_EACH_TEXT(data) WHERE key ~* 'modes_orientation_accompagnateur.\d+'), NULL) AS "modes_orientation_accompagnateur",
        ARRAY_REMOVE(ARRAY(SELECT value FROM JSONB_EACH_TEXT(data) WHERE key ~* 'modes_orientation_beneficiaire.\d+'), NULL)   AS "modes_orientation_beneficiaire",
        ARRAY_REMOVE(ARRAY(SELECT value FROM JSONB_EACH_TEXT(data) WHERE key ~* 'frais.\d+'), NULL)                            AS "frais",
        data ->> 'modes_orientation_accompagnateur_autres'                                                                     AS "modes_orientation_accompagnateur_autres",
        data ->> 'modes_orientation_beneficiaire_autres'                                                                       AS "modes_orientation_beneficiaire_autres",
        data ->> 'formulaire_en_ligne'                                                                                         AS "formulaire_en_ligne",
        data ->> 'frais_autres'                                                                                                AS "frais_autres",
        data ->> 'nom'                                                                                                         AS "nom",
        data ->> 'presentation_resume'                                                                                         AS "presentation_resume",
        data ->> 'presentation_detail'                                                                                         AS "presentation_detail",
        data ->> 'prise_rdv'                                                                                                   AS "prise_rdv",
        data ->> 'recurrence'                                                                                                  AS "recurrence",
        NULLIF(TRIM(data ->> 'zone_diffusion_code'), '')                                                                       AS "zone_diffusion_code",
        NULLIF(TRIM(data ->> 'zone_diffusion_nom'), '')                                                                        AS "zone_diffusion_nom",
        data ->> 'zone_diffusion_type'                                                                                         AS "zone_diffusion_type"
    FROM source
    WHERE
        NOT COALESCE(CAST(data ->> '__ignore__' AS BOOLEAN), FALSE)
)

SELECT * FROM final
