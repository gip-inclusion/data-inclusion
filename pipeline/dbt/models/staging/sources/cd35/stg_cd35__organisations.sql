WITH source AS (
    {{ stg_source_header('cd35', 'organisations') }}),

final AS (
    SELECT
        CAST((data ->> 'LATITUDE') AS FLOAT)             AS "latitude",
        CAST((data ->> 'LONGITUDE') AS FLOAT)            AS "longitude",
        data ->> 'ADRESSE'                               AS "adresse",
        data ->> 'CODE_INSEE'                            AS "code_insee",
        data ->> 'CODE_POSTAL'                           AS "code_postal",
        data ->> 'COMMUNE'                               AS "commune",
        data ->> 'COMPLEMENT_ADRESSE'                    AS "complement_adresse",
        data ->> 'COURRIEL'                              AS "courriel",
        CAST(data ->> 'DATE_MAJ' AS DATE)                AS "date_maj",
        data ->> 'HORAIRES_OUVERTURE'                    AS "horaires_ouvertures",
        data ->> 'ID'                                    AS "id",
        data ->> 'LIEN_SOURCE'                           AS "lien_source",
        NULLIF(TRIM(data ->> 'NOM'), '')                 AS "nom",
        NULLIF(TRIM(data ->> 'PRESENTATION_DETAIL'), '') AS "presentation_detail",
        data ->> 'SIGLE'                                 AS "sigle",
        data ->> 'SITE_WEB'                              AS "site_web",
        data ->> 'TELEPHONE'                             AS "telephone"
    FROM source
)

SELECT * FROM final
