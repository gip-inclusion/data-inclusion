WITH source AS (
    {{ stg_source_header('pole_emploi', 'services') }}
),

structures AS (
    SELECT * FROM {{ ref('stg_pole_emploi__structures') }}
),

services AS (
    SELECT
        _di_source_id                                                                                               AS "_di_source_id",
        CAST((data ->> 'contact_public') AS BOOLEAN)                                                                AS "contact_public",
        CAST((data ->> 'cumulable') AS BOOLEAN)                                                                     AS "cumulable",
        CAST((data ->> 'date_creation') AS TIMESTAMP WITH TIME ZONE)                                                AS "date_creation",
        CAST((data ->> 'date_maj') AS TIMESTAMP WITH TIME ZONE)                                                     AS "date_maj",
        CAST((data ->> 'date_suspension') AS TIMESTAMP WITH TIME ZONE)                                              AS "date_suspension",
        CAST((data ->> 'latitude') AS FLOAT)                                                                        AS "latitude",
        CAST((data ->> 'longitude') AS FLOAT)                                                                       AS "longitude",
        CAST(ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(data -> 'modes_accueil')) AS TEXT [])                    AS "modes_accueil",
        CAST(ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(data -> 'profils')) AS TEXT [])                          AS "profils",
        CAST(ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(data -> 'thematiques')) AS TEXT [])                      AS "thematiques",
        CAST(ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(data -> 'types')) AS TEXT [])                            AS "types",
        CAST(ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(data -> 'justificatifs')) AS TEXT [])                    AS "justificatifs",
        CAST(ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(data -> 'pre_requis')) AS TEXT [])                       AS "pre_requis",
        CAST(ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(data -> 'modes_orientation_accompagnateur')) AS TEXT []) AS "modes_orientation_accompagnateur",
        CAST(ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(data -> 'modes_orientation_beneficiaire')) AS TEXT [])   AS "modes_orientation_beneficiaire",
        data ->> 'modes_orientation_accompagnateur_autres'                                                          AS "modes_orientation_accompagnateur_autres",
        data ->> 'modes_orientation_beneficiaire_autres'                                                            AS "modes_orientation_beneficiaire_autres",
        data ->> 'adresse'                                                                                          AS "adresse",
        data ->> 'code_insee'                                                                                       AS "code_insee",
        data ->> 'code_postal'                                                                                      AS "code_postal",
        data ->> 'commune'                                                                                          AS "commune",
        data ->> 'complement_adresse'                                                                               AS "complement_adresse",
        NULLIF(TRIM(data ->> 'contact_nom'), '')                                                                    AS "contact_nom",
        NULLIF(TRIM(data ->> 'contact_prenom'), '')                                                                 AS "contact_prenom",
        NULLIF(TRIM(data ->> 'courriel'), '')                                                                       AS "courriel",
        data ->> 'formulaire_en_ligne'                                                                              AS "formulaire_en_ligne",
        data ->> 'frais_autres'                                                                                     AS "frais_autres",
        data ->> 'frais'                                                                                            AS "frais",
        data ->> 'id'                                                                                               AS "id",
        data ->> 'lien_source'                                                                                      AS "lien_source",
        data ->> 'nom'                                                                                              AS "nom",
        data ->> 'presentation_resume'                                                                              AS "presentation_resume",
        data ->> 'presentation_detail'                                                                              AS "presentation_detail",
        data ->> 'prise_rdv'                                                                                        AS "prise_rdv",
        data ->> 'recurrence'                                                                                       AS "recurrence",
        data ->> 'source'                                                                                           AS "source",
        data ->> 'structure_id'                                                                                     AS "structure_id",
        NULLIF(TRIM(data ->> 'telephone'), '')                                                                      AS "telephone",
        NULLIF(TRIM(data ->> 'zone_diffusion_code'), '')                                                            AS "zone_diffusion_code",
        NULLIF(TRIM(data ->> 'zone_diffusion_nom'), '')                                                             AS "zone_diffusion_nom",
        data ->> 'zone_diffusion_type'                                                                              AS "zone_diffusion_type"
    FROM source
    WHERE data ->> 'structure_id' = 'f26d4cc9-6ca8-4864-ad1c-013c38ab7cfb'
),

-- select services associated to PE structure(s)
final AS (
    SELECT services.*
    FROM services INNER JOIN structures ON services.structure_id = structures.id
)

SELECT * FROM final
