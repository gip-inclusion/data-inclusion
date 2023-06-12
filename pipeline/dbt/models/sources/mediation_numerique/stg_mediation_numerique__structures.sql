WITH source AS (
    {{
        dbt_utils.union_relations(
            relations=[
                source('mediation_numerique_angers', 'structures'),
                source('mediation_numerique_assembleurs', 'structures'),
                source('mediation_numerique_cd23', 'structures'),
                source('mediation_numerique_cd33', 'structures'),
                source('mediation_numerique_cd40', 'structures'),
                source('mediation_numerique_cd44', 'structures'),
                source('mediation_numerique_cd49', 'structures'),
                source('mediation_numerique_cd87', 'structures'),
                source('mediation_numerique_conseiller_numerique', 'structures'),
                source('mediation_numerique_conumm', 'structures'),
                source('mediation_numerique_cr93', 'structures'),
                source('mediation_numerique_fibre_64', 'structures'),
                source('mediation_numerique_france_services', 'structures'),
                source('mediation_numerique_france_tiers_lieux', 'structures'),
                source('mediation_numerique_francilin', 'structures'),
                source('mediation_numerique_hinaura', 'structures'),
                source('mediation_numerique_hub_antilles', 'structures'),
                source('mediation_numerique_hub_lo', 'structures'),
                source('mediation_numerique_mulhouse', 'structures'),
                source('mediation_numerique_res_in', 'structures'),
                source('mediation_numerique_rhinocc', 'structures'),
                source('mediation_numerique_ultra_numerique', 'structures'),
            ],
            source_column_name=None
        )
    }}
),

final AS (
    SELECT
        _di_source_id                                                                                               AS "_di_source_id",
        CAST(ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(NULLIF(data -> 'thematiques', 'null'))) AS TEXT [])      AS "thematiques",
        CAST((data ->> 'longitude') AS FLOAT)                                                                       AS "longitude",
        CAST((data ->> 'latitude') AS FLOAT)                                                                        AS "latitude",
        CAST((data ->> 'date_maj') AS TIMESTAMP WITH TIME ZONE)                                                     AS "date_maj",
        CAST(ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(NULLIF(data -> 'labels_nationaux', 'null'))) AS TEXT []) AS "labels_nationaux",
        data ->> 'id'                                                                                               AS "id",
        data ->> 'nom'                                                                                              AS "nom",
        NULLIF(data ->> 'siret', REPEAT('0', 14))                                                                   AS "siret",
        data ->> 'source'                                                                                           AS "source",
        data ->> 'adresse'                                                                                          AS "adresse",
        data ->> 'commune'                                                                                          AS "commune",
        data ->> 'courriel'                                                                                         AS "courriel",
        data ->> 'site_web'                                                                                         AS "site_web",
        data ->> 'telephone'                                                                                        AS "telephone",
        data ->> 'code_postal'                                                                                      AS "code_postal",
        data ->> 'horaires_ouverture'                                                                               AS "horaires_ouverture"
    FROM source
)

SELECT * FROM final
