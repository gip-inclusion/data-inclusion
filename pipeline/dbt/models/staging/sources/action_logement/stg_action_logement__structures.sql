WITH source AS (
    {{ stg_source_header('action_logement', 'structures') }}),

final AS (
    SELECT
        CAST(source.data ->> 'date_maj' AS DATE)                  AS "date_maj",
        source.data ->> 'id'                                      AS "id",
        source.data ->> 'nom'                                     AS "nom",
        source.data ->> 'siret'                                   AS "siret",
        source.data ->> 'description'                             AS "description",
        source.data ->> 'adresse'                                 AS "adresse",
        source.data ->> 'complement_adresse'                      AS "complement_adresse",
        source.data ->> 'code_postal'                             AS "code_postal",
        source.data ->> 'commune'                                 AS "commune",
        source.data ->> 'telephone'                               AS "telephone",
        source.data ->> 'lien_source'                             AS "lien_source",
        source.data ->> 'site_web'                                AS "site_web",
        source.data ->> 'horaires_accueil'                        AS "horaires_accueil",
        STRING_TO_ARRAY(source.data ->> 'reseaux_porteurs', ', ') AS "reseaux_porteurs"
    FROM source
    WHERE
        NOT COALESCE(CAST(source.data ->> '__ignore__' AS BOOLEAN), FALSE)
)

SELECT * FROM final
