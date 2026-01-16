WITH source AS (
    {{ stg_source_header('reseau_alpha', 'pages') }}
),

final AS (
    SELECT
        NULLIF(TRIM(data ->> 'structure_id'), '') AS "structure_id",
        TO_DATE(
            SPLIT_PART(data ->> 'date_derniere_modification', ' : ', 2),
            'DD TMmonth YYYY'
        )                                         AS "date_derniere_modification",
        NULLIF(TRIM(data ->> 'url'), '')          AS "url"
    FROM source
)

SELECT * FROM final
