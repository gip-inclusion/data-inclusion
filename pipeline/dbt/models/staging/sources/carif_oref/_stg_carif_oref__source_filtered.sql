WITH source AS (
    {{ stg_source_header('carif_oref', 'formations') }}
),

final AS (
    SELECT *
    FROM source
    WHERE
        -- keep only formations that have at least one of the following FORMACODE codes
        ARRAY(
            SELECT codes.data ->> '$'
            FROM JSONB_ARRAY_ELEMENTS(source.data -> 'domaine-formation' -> 'code-FORMACODE') AS codes (data)
        )
        &&
        ARRAY[
            -- français mise à niveau
            -- https://formacode.centre-inffo.fr/spip.php?page=thesaurus&recherche_libre=15040
            '15040',
            -- alphabétisation
            -- https://formacode.centre-inffo.fr/spip.php?page=thesaurus&recherche_libre=15043
            '15043',
            -- français langue étrangère
            -- https://formacode.centre-inffo.fr/spip.php?page=thesaurus&recherche_libre=15235
            '15235'
        ]
)

SELECT * FROM final
