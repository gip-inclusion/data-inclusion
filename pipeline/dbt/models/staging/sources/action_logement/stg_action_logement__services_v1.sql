WITH source AS (
    {{ stg_source_header('action_logement_v1', 'services') }}),

final AS (
    SELECT
        source.data ->> 'id'                                        AS "id",
        CURRENT_DATE AT TIME ZONE 'Europe/Paris'                    AS "date_maj",
        source.data ->> 'nom'                                       AS "nom",
        source.data ->> 'description'                               AS "description",
        source.data ->> 'lien_source'                               AS "lien_source",
        source.data ->> 'type'                                      AS "type",
        STRING_TO_ARRAY(source.data ->> 'thematiques', ', ')        AS "thematiques",
        source.data ->> 'frais'                                     AS "frais",
        STRING_TO_ARRAY(source.data ->> 'publics', ', ')            AS "publics",
        source.data ->> 'publics_precisions'                        AS "publics_precisions",
        source.data ->> 'conditions_acces'                          AS "conditions_acces",
        source.data ->> 'telephone'                                 AS "telephone",
        STRING_TO_ARRAY(source.data ->> 'modes_accueil', ', ')      AS "modes_accueil",
        STRING_TO_ARRAY(source.data ->> 'modes_mobilisation', ', ') AS "modes_mobilisation",
        source.data ->> 'lien_mobilisation'                         AS "lien_mobilisation",
        STRING_TO_ARRAY(source.data ->> 'mobilisable_par', ', ')    AS "mobilisable_par"
    FROM source
    WHERE
        NOT COALESCE(CAST(source.data ->> '__ignore__' AS BOOLEAN), FALSE)
)

SELECT * FROM final
