WITH source AS (
    {{ stg_source_header('action_logement', 'services') }}),

final AS (
    SELECT
        source.data ->> 'id'                                         AS "id",
        CAST(source.data ->> 'date_maj' AS DATE)                     AS "date_maj",
        source.data ->> 'nom'                                        AS "nom",
        source.data ->> 'description'                                AS "description",
        source.data ->> 'lien_source'                                AS "lien_source",
        source.data ->> 'type'                                       AS "type",
        STRING_TO_ARRAY(source.data ->> 'thematiques', ', ')         AS "thematiques",
        source.data ->> 'frais'                                      AS "frais",
        source.data ->> 'frais_precisions'                           AS "frais_precisions",
        STRING_TO_ARRAY(source.data ->> 'publics', ', ')             AS "publics",
        source.data ->> 'publics_precisions'                         AS "publics_precisions",
        source.data ->> 'conditions_acces'                           AS "conditions_acces",
        source.data ->> 'telephone'                                  AS "telephone",
        source.data ->> 'courriel'                                   AS "courriel",
        NULLIF(TRIM(source.data ->> 'contact_nom_prenom'), '')       AS "contact_nom_prenom",
        STRING_TO_ARRAY(source.data ->> 'modes_accueil', ', ')       AS "modes_accueil",
        STRING_TO_ARRAY(source.data ->> 'modes_mobilisation', ', ')  AS "modes_mobilisation",
        source.data ->> 'lien_mobilisation'                          AS "lien_mobilisation",
        source.data ->> 'mobilisation_precisions'                    AS "mobilisation_precisions",
        STRING_TO_ARRAY(source.data ->> 'mobilisable_par', ', ')     AS "mobilisable_par",
        STRING_TO_ARRAY(source.data ->> 'zone_eligibilite', ', ')    AS "zone_eligibilite",
        CAST(source.data ->> 'volume_horaire_hebdomadaire' AS FLOAT) AS "volume_horaire_hebdomadaire",
        CAST(source.data ->> 'nombre_semaines' AS INT)               AS "nombre_semaines",
        source.data ->> 'horaires_accueil'                           AS "horaires_accueil"
    FROM source
    WHERE
        NOT COALESCE(CAST(source.data ->> '__ignore__' AS BOOLEAN), FALSE)
)

SELECT * FROM final
