WITH source AS (
    SELECT * FROM {{ source('cd35', 'organisations') }}
),


final AS (
    SELECT
        _di_source_id                                    AS "_di_source_id",
        (TRIM('|' FROM data ->> 'ORG_LONGITUDE'))::FLOAT AS "org_longitude",
        (TRIM('|' FROM data ->> 'ORG_LATITUDE'))::FLOAT  AS "org_latitude",
        TO_DATE(data ->> 'ORG_DATEMAJ', 'DD-MM-YYYY')    AS "org_datemaj",
        TO_DATE(data ->> 'ORG_DATECREA', 'DD-MM-YYYY')   AS "org_datecrea",
        data ->> 'ORG_ID'                                AS "id",
        data ->> 'ORG_ID'                                AS "org_id",
        data ->> 'ORG_NOM'                               AS "org_nom",
        data ->> 'ORG_VILLE'                             AS "org_ville",
        data ->> 'ORG_CP'                                AS "org_cp",
        data ->> 'ORG_ADRES'                             AS "org_adres",
        data ->> 'ORG_SIGLE'                             AS "org_sigle",
        data ->> 'ORG_TEL'                               AS "org_tel",
        data ->> 'ORG_MAIL'                              AS "org_mail",
        data ->> 'ORG_WEB'                               AS "org_web",
        data ->> 'ORG_DESC'                              AS "org_desc",
        data ->> 'URL'                                   AS "url",
        data ->> 'ORG_HORAIRE'                           AS "org_horaire"
    FROM source
)

SELECT * FROM final
