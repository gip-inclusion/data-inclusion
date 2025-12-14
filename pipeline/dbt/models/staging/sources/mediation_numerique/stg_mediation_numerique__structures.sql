-- mediation-numerique source filtering (Dec 2025)
--
-- keep 6 original sources:
--   - Coop numérique (10K structures, date_maj > 2024, avg score 0.66)
--   - France Services (2.3K structures, avg score 0.80)
--   - SIILAB (1.9K structures, 40% in 2024+, score 0.74)
--   - Hub Bretagne (970 structures, 50% in 2024+)
--   - Hinaura (400 structures, date_maj > 2024, avg score 0.88)
--   - Conseiller Numerique (2.5K, 46% date_maj > 2024)
--
-- discard:
--   - dora, fredo — already ingested separately
--   - Aidants Connect (3.8K) — every date_maj 1970
--   - Paca, RhinOcc, Loire Atlantique, Haute-Vienne, etc. — all stale (1970 dates)
--   - Francil-in, Gironde, Hub-lo, Mednum BFC — no recent updates

WITH source AS (
    {{ stg_source_header('mediation_numerique', 'structures') }}),

final AS (
    SELECT
        CAST(
            ARRAY(
                SELECT thematiques.*
                FROM JSONB_ARRAY_ELEMENTS_TEXT(NULLIF(source.data -> 'thematiques', 'null')) AS thematiques
            ) AS TEXT []
        )                                                       AS "thematiques",
        CAST((data ->> 'longitude') AS FLOAT)                   AS "longitude",
        CAST((data ->> 'latitude') AS FLOAT)                    AS "latitude",
        CAST((data ->> 'date_maj') AS TIMESTAMP WITH TIME ZONE) AS "date_maj",
        CAST(
            ARRAY(
                SELECT labels_nationaux.*
                FROM JSONB_ARRAY_ELEMENTS_TEXT(NULLIF(source.data -> 'labels_nationaux', 'null')) AS labels_nationaux
            ) AS TEXT []
        )                                                       AS "labels_nationaux",
        CAST(
            ARRAY(
                SELECT labels_autres.*
                FROM JSONB_ARRAY_ELEMENTS_TEXT(NULLIF(source.data -> 'labels_autres', 'null')) AS labels_autres
            ) AS TEXT []
        )                                                       AS "labels_autres",
        data ->> 'id'                                           AS "id",
        data ->> 'nom'                                          AS "nom",
        NULLIF(data ->> 'siret', REPEAT('0', 14))               AS "siret",
        data ->> 'source'                                       AS "source",
        NULLIF(TRIM(data ->> 'adresse'), '')                    AS "adresse",
        NULLIF(TRIM(data ->> 'commune'), '')                    AS "commune",
        NULLIF(TRIM(data ->> 'courriel'), '')                   AS "courriel",
        data ->> 'site_web'                                     AS "site_web",
        NULLIF(TRIM(data ->> 'telephone'), '')                  AS "telephone",
        data ->> 'code_postal'                                  AS "code_postal",
        data ->> 'code_insee'                                   AS "code_insee",
        data ->> 'horaires_ouverture'                           AS "horaires_ouverture",
        data ->> 'typologie'                                    AS "typologie",
        data ->> 'presentation_resume'                          AS "presentation_resume",
        data ->> 'accessibilite'                                AS "accessibilite",
        data ->> 'presentation_detail'                          AS "presentation_detail"
    FROM source
    WHERE
        data ->> 'source' IN (
            'France Services',
            'Coop numérique',
            'Hinaura',
            'SIILAB',
            'Hub Bretagne',
            'Conseiller Numerique'
        )
)

SELECT * FROM final
