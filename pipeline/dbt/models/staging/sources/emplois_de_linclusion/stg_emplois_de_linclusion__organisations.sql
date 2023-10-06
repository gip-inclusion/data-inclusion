{% set source_model = source('emplois_de_linclusion', 'organisations') %}

{% set table_exists = adapter.get_relation(database=source_model.database, schema=source_model.schema, identifier=source_model.name) is not none %}

{% if table_exists %}

    WITH source AS (
        SELECT * FROM {{ source_model }}
    ),

{% else %}

WITH source AS (
    SELECT
        NULL                AS "_di_source_id",
        CAST(NULL AS JSONB) AS "data"
    WHERE FALSE
),

{% endif %}

final AS (
    SELECT
        _di_source_id                                                                       AS "_di_source_id",
        (data ->> 'antenne')::BOOLEAN                                                       AS "antenne",
        ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(data -> 'thematiques'))::TEXT []      AS "thematiques",
        ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(data -> 'labels_autres'))::TEXT []    AS "labels_autres",
        ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(data -> 'labels_nationaux'))::TEXT [] AS "labels_nationaux",
        (data ->> 'longitude')::FLOAT                                                       AS "longitude",
        (data ->> 'latitude')::FLOAT                                                        AS "latitude",
        (data ->> 'date_maj')::DATE                                                         AS "date_maj",
        data ->> 'id'                                                                       AS "id",
        data ->> 'nom'                                                                      AS "nom",
        data ->> 'rna'                                                                      AS "rna",
        data ->> 'siret'                                                                    AS "siret",
        data ->> 'source'                                                                   AS "source",
        data ->> 'adresse'                                                                  AS "adresse",
        data ->> 'commune'                                                                  AS "commune",
        data ->> 'courriel'                                                                 AS "courriel",
        data ->> 'site_web'                                                                 AS "site_web",
        data ->> 'telephone'                                                                AS "telephone",
        data ->> 'typologie'                                                                AS "typologie",
        data ->> 'code_insee'                                                               AS "code_insee",
        data ->> 'code_postal'                                                              AS "code_postal",
        data ->> 'lien_source'                                                              AS "lien_source",
        data ->> 'accessibilite'                                                            AS "accessibilite",
        data ->> 'complement_adresse'                                                       AS "complement_adresse",
        data ->> 'horaires_ouverture'                                                       AS "horaires_ouverture",
        data ->> 'presentation_detail'                                                      AS "presentation_detail",
        data ->> 'presentation_resume'                                                      AS "presentation_resume"
    FROM source
)

SELECT * FROM final
