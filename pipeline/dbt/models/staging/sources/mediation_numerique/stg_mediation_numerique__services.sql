WITH source AS (
    {{ stg_source_header('mediation_numerique', 'structures') }}
),

final AS (
    SELECT
        source.data ->> 'id'                                                     AS "id",
        source.data ->> 'id'                                                     AS "structure_id",
        NULLIF(TRIM(source.data ->> 'prise_rdv'), '')                            AS "prise_rdv",
        CAST(NULLIF(source.data ->> 'date_maj', '') AS TIMESTAMP WITH TIME ZONE) AS "date_maj",
        CAST(
            ARRAY(
                SELECT d.*
                FROM JSONB_ARRAY_ELEMENTS_TEXT(NULLIF(source.data -> 'services', 'null')) AS d
            ) AS TEXT []
        )                                                                        AS "services",
        CAST(
            ARRAY(
                SELECT d.*
                FROM JSONB_ARRAY_ELEMENTS_TEXT(NULLIF(source.data -> 'frais_a_charge', 'null')) AS d
            ) AS TEXT []
        )                                                                        AS "frais_a_charge",
        CAST(
            ARRAY(
                SELECT d.*
                FROM JSONB_ARRAY_ELEMENTS_TEXT(NULLIF(source.data -> 'modalites_acces', 'null')) AS d
            ) AS TEXT []
        )                                                                        AS "modalites_acces",
        CAST(
            ARRAY(
                SELECT d.*
                FROM JSONB_ARRAY_ELEMENTS_TEXT(NULLIF(source.data -> 'modalites_accompagnement', 'null')) AS d
            ) AS TEXT []
        )                                                                        AS "modalites_accompagnement",
        CAST(
            ARRAY(
                SELECT d.*
                FROM JSONB_ARRAY_ELEMENTS_TEXT(NULLIF(source.data -> 'publics_specifiquement_adresses', 'null')) AS d
            ) AS TEXT []
        )                                                                        AS "publics_specifiquement_adresses",
        CAST(
            ARRAY(
                SELECT d.*
                FROM JSONB_ARRAY_ELEMENTS_TEXT(NULLIF(source.data -> 'prise_en_charge_specifique', 'null')) AS d
            ) AS TEXT []
        )                                                                        AS "prise_en_charge_specifique"
    FROM source
    WHERE source.data ->> 'source' NOT IN ('dora', 'fredo', 'soliguide')
)

SELECT * FROM final
