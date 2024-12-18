WITH source AS (
    {{ stg_source_header('imilo', 'offres') }}
),

structures_offres AS (
    SELECT
        CAST((data -> 'structures_offres' ->> 'offre_id') AS INTEGER)         AS "id_offre",
        CAST((data -> 'structures_offres' ->> 'missionlocale_id') AS INTEGER) AS "id_structure",
        CONCAT(
            (data -> 'structures_offres' ->> 'offre_id'),
            '_',
            (data -> 'structures_offres' ->> 'missionlocale_id')
        )                                                                     AS "offre_structure_id"
    FROM {{ source('imilo', 'structures_offres') }}
),

final AS (
    SELECT
        source._di_source_id                                                           AS "_di_source_id",
        structures_offres."offre_structure_id"                                         AS "id",
        CAST((source.data -> 'offres' ->> 'date_maj') AS TIMESTAMP WITH TIME ZONE)     AS "date_maj",
        NULLIF(TRIM(source.data -> 'offres' ->> 'nom_dora'), '')                       AS "nom",
        NULLIF(TRIM(source.data -> 'offres' ->> 'thematique'), '')                     AS "thematiques",
        CAST((source.data -> 'offres' ->> 'date_import') AS TIMESTAMP WITH TIME ZONE)  AS "date_creation",
        NULLIF(TRIM(source.data -> 'offres' ->> 'description'), '')                    AS "presentation_resume",
        NULLIF(TRIM(source.data -> 'offres' ->> 'modes_accueil'), '')                  AS "modes_accueil",
        NULLIF(TRIM(source.data -> 'offres' ->> 'liste_des_profils'), '')              AS "profils",
        NULLIF(TRIM(source.data -> 'offres' ->> 'modes_orientation_beneficiaire'), '') AS "modes_orientation_beneficiaire",
        CAST(structures_offres."id_structure" AS TEXT)                                 AS "structure_id"
    FROM source
    LEFT JOIN structures_offres
        ON CAST((source.data -> 'offres' ->> 'id_offre') AS INTEGER) = structures_offres.id_offre
)

SELECT * FROM final
