WITH source AS (
    {{ stg_source_header('ma_boussole_aidants', 'structures') }}),

/*
Ignored fields:
- `source` since it is always set to "Ma Boussole Aidants" and we have our own slug for this source
- `departement` -> `nomDepartement` since the code is present and we can derive the name from it
- `ville` -> `nomVille` for the same reason as above
- `typeStructure` -> `nomTypeStructure` since it contains generic descriptions about the structure type, that we maintain in our reseaux porteurs table
*/

final AS (
    SELECT
        NULLIF(TRIM(data -> 'adresse' ->> 'codePostal'), '')                              AS "adresse__code_postal",
        CAST(data -> 'adresse' -> 'latitude' AS FLOAT)                                    AS "adresse__latitude",
        CAST(data -> 'adresse' -> 'longitude' AS FLOAT)                                   AS "adresse__longitude",
        NULLIF(TRIM(data -> 'adresse' ->> 'ligneAdresse'), '')                            AS "adresse__ligne_adresse",
        CAST(data ->> 'ageMax' AS INT)                                                    AS "age_max",
        CAST(data ->> 'ageMin' AS INT)                                                    AS "age_min",
        SUBSTRING((data -> 'departement' ->> 'codeDepartement') FROM '(?i)^\d[\dab]\d?$') AS "departement__code_departement",
        NULLIF(TRIM(data ->> 'email'), '')                                                AS "email",
        NULLIF(TRIM(data ->> 'idStructure'), '')                                          AS "id_structure",
        CAST(data ->> 'lastModifiedDate' AS DATE)                                         AS "last_modified_date",
        NULLIF(TRIM(data ->> 'nomStructure'), '')                                         AS "nom_structure",
        LOWER(NULLIF(TRIM(data -> 'rayonAction' ->> 'nomRayonAction'), ''))               AS "rayon_action__nom_rayon_action",
        NULLIF(TRIM(data ->> 'siteWeb'), '')                                              AS "site_web",
        NULLIF(TRIM(data -> 'structureMere' ->> 'siret'), '')                             AS "structure_mere__siret",
        NULLIF(TRIM(data ->> 'telephone1'), '')                                           AS "telephone_1",
        NULLIF(TRIM(data -> 'typeStructure' ->> 'idTypeStructure'), '')                   AS "id_type_structure",
        NULLIF(TRIM(data -> 'typeStructure' ->> 'descriptionTypeStructure'), '')          AS "description_structure",
        SUBSTRING(data -> 'ville' ->> 'codeCommune' FROM '\d[\w\d]\d{3}')                 AS "ville__code_commune",
        NULLIF(TRIM(data -> 'ville' ->> 'nomVille'), '')                                  AS "ville__nom_ville"
    FROM source
)

SELECT * FROM final
