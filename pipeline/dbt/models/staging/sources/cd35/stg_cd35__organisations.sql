WITH source AS (
    {{ stg_source_header('cd35', 'organisations') }}
),

final AS (
    SELECT
        CAST((data ->> 'LATITUDE') AS FLOAT)                                                        AS "latitude",
        CAST((data ->> 'LONGITUDE') AS FLOAT)                                                       AS "longitude",
        data ->> 'ADRESSE'                                                                          AS "adresse",
        data ->> 'CODE_INSEE'                                                                       AS "code_insee",
        data ->> 'CODE_POSTAL'                                                                      AS "code_postal",
        data ->> 'COMMUNE'                                                                          AS "commune",
        data ->> 'COMPLEMENT_ADRESSE'                                                               AS "complement_adresse",
        data ->> 'COURRIEL'                                                                         AS "courriel",
        TO_DATE(data ->> 'DATE_MAJ', 'YYYYMMDD')                                                    AS "date_maj",
        data ->> 'HORAIRES_OUVERTURE'                                                               AS "horaires_ouvertures",
        data ->> 'ID'                                                                               AS "id",
        data ->> 'LIEN_SOURCE'                                                                      AS "lien_source",
        data ->> 'NOM'                                                                              AS "nom",
        data ->> 'PRESENTATION_DETAIL'                                                              AS "presentation_detail",
        (SELECT ARRAY_AGG(TRIM(p)) FROM UNNEST(STRING_TO_ARRAY(data ->> 'PROFIL', ',')) AS "p")     AS "profils",
        data ->> 'SIGLE'                                                                            AS "sigle",
        data ->> 'SITE_WEB'                                                                         AS "site_web",
        data ->> 'TELEPHONE'                                                                        AS "telephone",
        (SELECT ARRAY_AGG(TRIM(t)) FROM UNNEST(STRING_TO_ARRAY(data ->> 'THEMATIQUE', ',')) AS "t") AS "thematiques"
    FROM source
)

SELECT * FROM final
