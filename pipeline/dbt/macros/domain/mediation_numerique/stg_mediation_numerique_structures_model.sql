{% macro stg_mediation_numerique_structures_model() %}

WITH source AS (
    SELECT * FROM {{ source('mediation_numerique_' ~ model.fqn[-2], 'structures') }}
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

{% endmacro %}