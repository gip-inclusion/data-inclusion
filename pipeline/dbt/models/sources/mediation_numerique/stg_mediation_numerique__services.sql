WITH source AS (
    {{
        dbt_utils.union_relations(
            relations=[
                source('mediation_numerique_angers', 'services'),
                source('mediation_numerique_assembleurs', 'services'),
                source('mediation_numerique_cd49', 'services'),
                source('mediation_numerique_cd87', 'services'),
                source('mediation_numerique_conseiller_numerique', 'services'),
                source('mediation_numerique_france_services', 'services'),
                source('mediation_numerique_france_tiers_lieux', 'services'),
                source('mediation_numerique_francilin', 'services'),
                source('mediation_numerique_hinaura', 'services'),
                source('mediation_numerique_hub_antilles', 'services'),
            ],
            source_column_name=None
        )
    }}
),

final AS (
    SELECT
        _di_source_id                                                                                          AS "_di_source_id",
        CAST(ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(NULLIF(data -> 'types', 'null'))) AS TEXT [])       AS "types",
        CAST(ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(NULLIF(data -> 'profils', 'null'))) AS TEXT [])     AS "profils",
        CAST(ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(NULLIF(data -> 'thematiques', 'null'))) AS TEXT []) AS "thematiques",
        CAST(ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(NULLIF(data -> 'frais', 'null'))) AS TEXT [])       AS "frais",
        CAST((data ->> 'longitude') AS FLOAT)                                                                  AS "longitude",
        CAST((data ->> 'latitude') AS FLOAT)                                                                   AS "latitude",
        data ->> 'id'                                                                                          AS "id",
        data ->> 'structure_id'                                                                                AS "structure_id",
        data ->> 'nom'                                                                                         AS "nom",
        data ->> 'source'                                                                                      AS "source",
        data ->> 'prise_rdv'                                                                                   AS "prise_rdv"
    FROM source
)

SELECT * FROM final
