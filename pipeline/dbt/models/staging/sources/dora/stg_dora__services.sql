WITH source AS (
    {{ stg_source_header('dora', 'services') }}),

structures AS (
    SELECT * FROM {{ ref('stg_dora__structures') }}
),

services AS (
    SELECT
        source._di_source_id                                                                                         AS "_di_source_id",
        CAST((source.data ->> 'contact_public') AS BOOLEAN)                                                          AS "contact_public",
        CAST((source.data ->> 'cumulable') AS BOOLEAN)                                                               AS "cumulable",
        CAST((source.data ->> 'date_creation') AS TIMESTAMP WITH TIME ZONE)                                          AS "date_creation",
        CAST((source.data ->> 'date_maj') AS TIMESTAMP WITH TIME ZONE)                                               AS "date_maj",
        CAST((source.data ->> 'date_suspension') AS TIMESTAMP WITH TIME ZONE)                                        AS "date_suspension",
        CAST((source.data ->> 'latitude') AS FLOAT)                                                                  AS "latitude",
        CAST((source.data ->> 'longitude') AS FLOAT)                                                                 AS "longitude",
        CAST(CAST((source.data ->> 'temps_passe_semaines') AS FLOAT) AS INT)                                         AS "temps_passe_semaines",
        CAST(CAST((source.data ->> 'temps_passe_duree_hebdomadaire') AS FLOAT) AS INT)                               AS "temps_passe_duree_hebdomadaire",
        ARRAY(SELECT s.* FROM JSONB_ARRAY_ELEMENTS_TEXT(source.data -> 'modes_accueil') AS s)                        AS "modes_accueil",
        ARRAY(SELECT s.* FROM JSONB_ARRAY_ELEMENTS_TEXT(source.data -> 'profils') AS s)                              AS "profils",
        ARRAY(SELECT s.* FROM JSONB_ARRAY_ELEMENTS_TEXT(source.data -> 'thematiques') AS s)                          AS "thematiques",
        ARRAY(SELECT s.* FROM JSONB_ARRAY_ELEMENTS_TEXT(source.data -> 'types') AS s)                                AS "types",
        ARRAY(SELECT s.* FROM JSONB_ARRAY_ELEMENTS_TEXT(source.data -> 'justificatifs') AS s)                        AS "justificatifs",
        ARRAY(SELECT s.* FROM JSONB_ARRAY_ELEMENTS_TEXT(source.data -> 'pre_requis') AS s)                           AS "pre_requis",
        ARRAY(SELECT s.* FROM JSONB_ARRAY_ELEMENTS_TEXT(source.data -> 'modes_orientation_accompagnateur') AS s)     AS "modes_orientation_accompagnateur",
        ARRAY(SELECT s.* FROM JSONB_ARRAY_ELEMENTS_TEXT(source.data -> 'modes_orientation_beneficiaire') AS s)       AS "modes_orientation_beneficiaire",
        NULLIF(TRIM(source.data ->> 'modes_orientation_accompagnateur_autres'), '')                                  AS "modes_orientation_accompagnateur_autres",
        NULLIF(TRIM(source.data ->> 'modes_orientation_beneficiaire_autres'), '')                                    AS "modes_orientation_beneficiaire_autres",
        NULLIF(TRIM(source.data ->> 'adresse'), '')                                                                  AS "adresse",
        NULLIF(TRIM(source.data ->> 'code_insee'), '')                                                               AS "code_insee",
        NULLIF(TRIM(source.data ->> 'code_postal'), '')                                                              AS "code_postal",
        NULLIF(TRIM(source.data ->> 'commune'), '')                                                                  AS "commune",
        NULLIF(TRIM(source.data ->> 'complement_adresse'), '')                                                       AS "complement_adresse",
        NULLIF(TRIM(source.data ->> 'contact_nom_prenom'), '')                                                       AS "contact_nom_prenom",
        NULLIF(TRIM(source.data ->> 'courriel'), '')                                                                 AS "courriel",
        NULLIF(TRIM(source.data ->> 'formulaire_en_ligne'), '')                                                      AS "formulaire_en_ligne",
        NULLIF(TRIM(source.data ->> 'frais_autres'), '')                                                             AS "frais_autres",
        NULLIF(TRIM(source.data ->> 'frais'), '')                                                                    AS "frais",
        NULLIF(TRIM(source.data ->> 'id'), '')                                                                       AS "id",
        NULLIF(TRIM(source.data ->> 'lien_source'), '')                                                              AS "lien_source",
        NULLIF(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(source.data ->> 'nom'), '\.{2,}$', '…'), '(?<!etc)\.$', ''), '') AS "nom",
        NULLIF(TRIM(source.data ->> 'presentation_resume'), '')                                                      AS "presentation_resume",
        NULLIF(TRIM(source.data ->> 'presentation_detail'), '')                                                      AS "presentation_detail",
        NULLIF(TRIM(source.data ->> 'prise_rdv'), '')                                                                AS "prise_rdv",
        NULLIF(TRIM(source.data ->> 'recurrence'), '')                                                               AS "recurrence",
        NULLIF(TRIM(source.data ->> 'source'), '')                                                                   AS "source",
        NULLIF(TRIM(source.data ->> 'structure_id'), '')                                                             AS "structure_id",
        NULLIF(TRIM(source.data ->> 'telephone'), '')                                                                AS "telephone",
        NULLIF(TRIM(source.data ->> 'zone_diffusion_code'), '')                                                      AS "zone_diffusion_code",
        NULLIF(TRIM(source.data ->> 'zone_diffusion_nom'), '')                                                       AS "zone_diffusion_nom",
        NULLIF(TRIM(source.data ->> 'zone_diffusion_type'), '')                                                      AS "zone_diffusion_type"
    FROM source
),

-- dora removes suggested structures from its api, but does not remove the associated services
-- therefore filter these orphan services
final AS (
    SELECT services.*
    FROM services INNER JOIN structures ON services.structure_id = structures.id
)

SELECT * FROM final
