WITH source AS (
    {{ stg_source_header('cd72', 'structures') }}
),

final AS (
    SELECT
        _di_source_id                                  AS "_di_source_id",
        data ->> 'id'                                  AS "id",
        data ->> 'nom'                                 AS "nom",
        data ->> 'siret'                               AS "siret",
        TRIM(SPLIT_PART(data ->> 'adresse', E'\n', 1)) AS "adresse",
        data ->> 'commune'                             AS "commune",
        data ->> 'courriel'                            AS "courriel",
        CAST(data ->> 'date_maj' AS DATE)              AS "date_maj",
        data ->> 'site_web'                            AS "site_web",
        data ->> 'telephone'                           AS "telephone",
        data ->> 'typologie'                           AS "typologie",
        data ->> 'code_postal'                         AS "code_postal",
        data ->> 'horaires_ouverture'                  AS "horaires_ouverture",
        data ->> 'presentation_detail'                 AS "presentation_detail"
    FROM source
)

SELECT * FROM final
