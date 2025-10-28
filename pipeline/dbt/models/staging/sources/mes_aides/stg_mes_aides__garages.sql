WITH source AS (
    {{ stg_source_header('mes_aides', 'garages') }}),

final AS (
    SELECT
        NULLIF(TRIM(fields ->> 'Adresse'), '')                                        AS "adresse",
        NULLIF(TRIM(fields ->> 'Code INSEE'), '')                                     AS "code_insee",
        NULLIF(TRIM(fields ->> 'Code Postal'), '')                                    AS "code_postal",
        CAST(fields ->> 'Créé le' AS DATE)                                            AS "cree_le",
        NULLIF(TRIM(fields ->> 'Critères d''éligibilité'), '')                        AS "criteres_eligibilite",
        NULLIF(TRIM(fields ->> 'Département Nom'), '')                                AS "departement_nom",
        NULLIF(TRIM(fields ->> 'Email'), '')                                          AS "email",
        COALESCE(CAST(fields ->> 'En Ligne' AS BOOLEAN), FALSE)                       AS "en_ligne",
        fields ->> 'ID'                                                               AS "id",
        -- some rows are formatted as `LAT, LAT`... use first value
        CAST(SPLIT_PART(fields ->> 'Latitude', ',', 1) AS FLOAT)                      AS "latitude",
        CAST(SPLIT_PART(fields ->> 'Longitude', ',', 1) AS FLOAT)                     AS "longitude",
        CAST(fields ->> 'Modifié le' AS DATE)                                         AS "modifie_le",
        NULLIF(TRIM(fields ->> 'Nom'), '')                                            AS "nom",
        NULLIF(TRIM(fields ->> 'Partenaire Nom'), '')                                 AS "partenaire_nom",
        NULLIF(TRIM(fields ->> 'Région Nom'), '')                                     AS "region_nom",
        REPLACE(NULLIF(TRIM(fields ->> 'SIRET'), ''), ' ', '')                        AS "siret",
        SUBSTRING(NULLIF(TRIM(fields ->> 'Téléphone'), '') FROM '\+?\d[\d\.\-\s]*\d') AS "telephone",
        NULLIF(TRIM(fields ->> 'Type'), '')                                           AS "type",
        NULLIF(TRIM(fields ->> 'Url'), '')                                            AS "url",
        NULLIF(TRIM(fields ->> 'Ville Nom'), '')                                      AS "ville_nom"
    FROM (
        SELECT data -> 'fields' AS "fields"
        FROM source
    )
)

SELECT * FROM final
