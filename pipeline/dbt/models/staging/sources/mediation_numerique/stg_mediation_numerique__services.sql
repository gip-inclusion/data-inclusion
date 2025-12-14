-- mediation-numerique source filtering (Dec 2025)
-- See stg_mediation_numerique__structures.sql for details on source selection.

WITH source AS (
    {{ stg_source_header('mediation_numerique', 'services') }}),

final AS (
    SELECT
        CAST(ARRAY(SELECT d.* FROM JSONB_ARRAY_ELEMENTS_TEXT(NULLIF(source.data -> 'types', 'null')) AS d) AS TEXT [])       AS "types",
        CAST(ARRAY(SELECT d.* FROM JSONB_ARRAY_ELEMENTS_TEXT(NULLIF(source.data -> 'profils', 'null')) AS d) AS TEXT [])     AS "profils",
        CAST(ARRAY(SELECT d.* FROM JSONB_ARRAY_ELEMENTS_TEXT(NULLIF(source.data -> 'thematiques', 'null')) AS d) AS TEXT []) AS "thematiques",
        CAST(ARRAY(SELECT d.* FROM JSONB_ARRAY_ELEMENTS_TEXT(NULLIF(source.data -> 'frais', 'null')) AS d) AS TEXT [])       AS "frais",
        CAST((source.data ->> 'longitude') AS FLOAT)                                                                         AS "longitude",
        CAST((source.data ->> 'latitude') AS FLOAT)                                                                          AS "latitude",
        source.data ->> 'id'                                                                                                 AS "id",
        source.data ->> 'structure_id'                                                                                       AS "structure_id",
        source.data ->> 'nom'                                                                                                AS "nom",
        source.data ->> 'source'                                                                                             AS "source",
        source.data ->> 'prise_rdv'                                                                                          AS "prise_rdv"
    FROM source
    WHERE
        source.data ->> 'source' IN (
            'France Services',
            'Coop numérique',
            'Hinaura',
            'SIILAB',
            'Hub Bretagne',
            'Conseiller Numerique'
        )
)

SELECT * FROM final
