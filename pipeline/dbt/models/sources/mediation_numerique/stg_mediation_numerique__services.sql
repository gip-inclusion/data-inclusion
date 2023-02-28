WITH source AS (
    {{
        dbt_utils.union_relations(
            relations=[
                source('mediation_numerique_angers', 'services'),
                source('mediation_numerique_assembleurs', 'services'),
                source('mediation_numerique_cd49', 'services'),
                source('mediation_numerique_conseiller_numerique', 'services'),
                source('mediation_numerique_france_services', 'services'),
                source('mediation_numerique_france_tiers_lieux', 'services'),
                source('mediation_numerique_francilin', 'services'),
                source('mediation_numerique_hinaura', 'services'),
            ],
            source_column_name=None
        )
    }}
),

final AS (
    SELECT
        ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(NULLIF(data -> 'types', 'null')))::TEXT[]       AS "types",
        ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(NULLIF(data -> 'profils', 'null')))::TEXT[]     AS "profils",
        ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(NULLIF(data -> 'thematiques', 'null')))::TEXT[] AS "thematiques",
        ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(NULLIF(data -> 'frais', 'null')))::TEXT[]       AS "frais",
        (data ->> 'longitude')::FLOAT                                                                 AS "longitude",
        (data ->> 'latitude')::FLOAT                                                                  AS "latitude",
        data ->> 'id'                                                                                 AS "id",
        data ->> 'structure_id'                                                                       AS "structure_id",
        data ->> 'nom'                                                                                AS "nom",
        data ->> 'source'                                                                             AS "source"
    FROM source
)

SELECT * FROM final
