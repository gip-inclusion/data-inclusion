WITH source AS (
    {{ stg_source_header('mediation_numerique', 'structures') }}
),

final AS (
    SELECT
        _di_source_id                                           AS "_di_source_id",
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
        data ->> 'source' != 'dora'
)


SELECT * FROM final
