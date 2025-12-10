WITH source AS (
    {{ stg_source_header('mes_aides', 'garages') }}),

besoins AS (
    SELECT
        data ->> 'ID'                                  AS "garage_id",
        TRIM(STRING_TO_TABLE(data ->> 'Besoins', ',')) AS "item"
    FROM source
),

final AS (
    SELECT
        garage_id,
        item,
        CASE item
            -- noqa: disable=layout.spacing
            WHEN 'Accompagnement personnalisé à la mobilité' THEN 'Mobilité'
            WHEN 'Acheter ou louer un véhicule'              THEN 'Transport'
            WHEN 'Atelier pédagogique'                       THEN 'Mobilité'
            WHEN 'Covoiturage & autopartage'                 THEN 'Transport'
            WHEN 'Réparer votre véhicule'                    THEN 'Transport'
            WHEN 'Transport à la demande'                    THEN 'Transport'
            -- noqa: enable=layout.spacing
        END AS "categorie"
    FROM besoins
)

SELECT * FROM final
