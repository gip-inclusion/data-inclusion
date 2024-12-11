WITH source AS (
    {{ stg_source_header('mission_locale', 'offres') }}
),

final AS (
    SELECT
        source._di_source_id                                                           AS "_di_source_id",
        NULLIF(TRIM(source.data -> 'offres' ->> 'id_offre'), '')                       AS "id_offre",
        CAST((source.data -> 'offres' ->> 'date_maj') AS TIMESTAMP WITH TIME ZONE)     AS "date_maj",
        NULLIF(TRIM(source.data -> 'offres' ->> 'nom_dora'), '')                       AS "nom_dora",
        NULLIF(TRIM(source.data -> 'offres' ->> 'thematique'), '')                     AS "thematique",
        CAST((source.data -> 'offres' ->> 'date_import') AS TIMESTAMP WITH TIME ZONE)  AS "date_import",
        NULLIF(TRIM(source.data -> 'offres' ->> 'presentation'), '')                   AS "presentation",
        NULLIF(TRIM(source.data -> 'offres' ->> 'modes_accueil'), '')                  AS "modes_accueil",
        NULLIF(TRIM(source.data -> 'offres' ->> 'modes_orientation_beneficiaire'), '') AS "modes_orientation_beneficiaire",
        NULLIF(TRIM(source.data -> 'offres' ->> 'frais'), '')                          AS "frais",
        NULLIF(TRIM(source.data -> 'offres' ->> 'perimetre_offre'), '')                AS "perimetre_offre",
        NULLIF(TRIM(source.data -> 'offres' ->> 'type_offre'), '')                     AS "type_offre"
    FROM source
)

SELECT * FROM final
