WITH source AS (
    {{
        dbt_utils.union_relations(
            relations=[
                source('mediation_numerique_angers', 'structures'),
                source('mediation_numerique_assembleurs', 'structures'),
                source('mediation_numerique_cd49', 'structures'),
                source('mediation_numerique_conseiller_numerique', 'structures'),
                source('mediation_numerique_france_services', 'structures'),
                source('mediation_numerique_france_tiers_lieux', 'structures'),
                source('mediation_numerique_francilin', 'structures'),
                source('mediation_numerique_hinaura', 'structures'),
            ],
            source_column_name=None
        )
    }}
),

final AS (
    SELECT
        _di_source_id                                                                                      AS "_di_source_id",
        ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(NULLIF(data -> 'thematiques', 'null')))::TEXT[]      AS "thematiques",
        (data ->> 'longitude')::FLOAT                                                                      AS "longitude",
        (data ->> 'latitude')::FLOAT                                                                       AS "latitude",
        (data ->> 'date_maj')::TIMESTAMP WITH TIME ZONE                                                    AS "date_maj",
        ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(NULLIF(data -> 'labels_nationaux', 'null')))::TEXT[] AS "labels_nationaux",
        data ->> 'id'                                                                                      AS "id",
        data ->> 'nom'                                                                                     AS "nom",
        NULLIF(data ->> 'siret', REPEAT('0', 14))                                                          AS "siret",
        data ->> 'source'                                                                                  AS "source",
        data ->> 'adresse'                                                                                 AS "adresse",
        data ->> 'commune'                                                                                 AS "commune",
        data ->> 'courriel'                                                                                AS "courriel",
        data ->> 'site_web'                                                                                AS "site_web",
        data ->> 'telephone'                                                                               AS "telephone",
        data ->> 'code_postal'                                                                             AS "code_postal",
        data ->> 'horaires_ouverture'                                                                      AS "horaires_ouverture"
    FROM source
)

SELECT * FROM final
