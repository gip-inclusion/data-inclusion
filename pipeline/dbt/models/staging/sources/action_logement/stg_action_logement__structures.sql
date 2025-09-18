WITH source AS (
    {{ stg_source_header('action_logement', 'structures') }}),

final AS (
    SELECT
        CURRENT_DATE AT TIME ZONE 'Europe/Paris'   AS "date_maj",
        ARRAY_REMOVE(ARRAY(
            SELECT x.value FROM JSONB_EACH_TEXT(source.data) AS x
            WHERE x.key ~* 'labels_autres.\d+'
        ), NULL)                                   AS "labels_autres",
        ARRAY_REMOVE(ARRAY(
            SELECT x.value FROM JSONB_EACH_TEXT(source.data) AS x
            WHERE x.key ~* 'labels_nationaux.\d+'
        ), NULL)                                   AS "labels_nationaux",
        CAST(source.data ->> 'latitude' AS FLOAT)  AS "latitude",
        CAST(source.data ->> 'longitude' AS FLOAT) AS "longitude",
        ARRAY_REMOVE(ARRAY(
            SELECT x.value FROM JSONB_EACH_TEXT(source.data) AS x
            WHERE x.key ~* 'thematiques.\d+'
        ), NULL)                                   AS "thematiques",
        source.data ->> 'accessibilite'            AS "accessibilite",
        source.data ->> 'adresse'                  AS "adresse",
        source.data ->> 'code_insee'               AS "code_insee",
        source.data ->> 'code_postal'              AS "code_postal",
        source.data ->> 'commune'                  AS "commune",
        source.data ->> 'complement_adresse'       AS "complement_adresse",
        source.data ->> 'courriel'                 AS "courriel",
        source.data ->> 'horaires_ouverture'       AS "horaires_ouverture",
        source.data ->> 'id'                       AS "id",
        source.data ->> 'nom'                      AS "nom",
        source.data ->> 'presentation_resume'      AS "presentation_resume",
        source.data ->> 'presentation_detail'      AS "presentation_detail",
        source.data ->> 'rna'                      AS "rna",
        source.data ->> 'siret'                    AS "siret",
        source.data ->> 'site_web'                 AS "site_web",
        source.data ->> 'source'                   AS "source",
        source.data ->> 'telephone'                AS "telephone",
        source.data ->> 'typologie'                AS "typologie"
    FROM source
    WHERE
        NOT COALESCE(CAST(source.data ->> '__ignore__' AS BOOLEAN), FALSE)
)

SELECT * FROM final
