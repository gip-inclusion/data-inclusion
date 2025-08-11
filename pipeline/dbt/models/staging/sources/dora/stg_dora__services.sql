WITH source AS (
    {{ stg_source_header('dora', 'services') }}),

structures AS (
    SELECT * FROM {{ ref('stg_dora__structures') }}
),

services AS (
    SELECT
        CAST((data ->> 'date_maj') AS TIMESTAMP WITH TIME ZONE)                                             AS "date_maj",
        CAST((data ->> 'latitude') AS FLOAT)                                                                AS "latitude",
        CAST((data ->> 'longitude') AS FLOAT)                                                               AS "longitude",
        CAST(CAST((data ->> 'temps_passe_semaines') AS FLOAT) AS INT)                                       AS "temps_passe_semaines",
        CAST((data ->> 'temps_passe_duree_hebdomadaire') AS FLOAT)                                          AS "temps_passe_duree_hebdomadaire",
        ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(data -> 'modes_accueil'))                             AS "modes_accueil",
        ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(data -> 'profils'))                                   AS "profils",
        ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(data -> 'thematiques'))                               AS "thematiques",
        ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(data -> 'types'))                                     AS "types",
        ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(data -> 'justificatifs'))                             AS "justificatifs",
        ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(data -> 'pre_requis'))                                AS "pre_requis",
        ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(data -> 'modes_orientation_accompagnateur'))          AS "modes_orientation_accompagnateur",
        ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(data -> 'modes_orientation_beneficiaire'))            AS "modes_orientation_beneficiaire",
        NULLIF(TRIM(data ->> 'modes_orientation_accompagnateur_autres'), '')                                AS "modes_orientation_accompagnateur_autres",
        NULLIF(TRIM(data ->> 'modes_orientation_beneficiaire_autres'), '')                                  AS "modes_orientation_beneficiaire_autres",
        NULLIF(TRIM(data ->> 'adresse'), '')                                                                AS "adresse",
        NULLIF(TRIM(data ->> 'code_insee'), '')                                                             AS "code_insee",
        NULLIF(TRIM(data ->> 'code_postal'), '')                                                            AS "code_postal",
        NULLIF(TRIM(data ->> 'commune'), '')                                                                AS "commune",
        NULLIF(TRIM(data ->> 'complement_adresse'), '')                                                     AS "complement_adresse",
        NULLIF(TRIM(data ->> 'contact_nom_prenom'), '')                                                     AS "contact_nom_prenom",
        NULLIF(TRIM(data ->> 'courriel'), '')                                                               AS "courriel",
        NULLIF(TRIM(data ->> 'formulaire_en_ligne'), '')                                                    AS "formulaire_en_ligne",
        NULLIF(TRIM(data ->> 'frais_autres'), '')                                                           AS "frais_autres",
        NULLIF(TRIM(data ->> 'frais'), '')                                                                  AS "frais",
        NULLIF(TRIM(data ->> 'id'), '')                                                                     AS "id",
        NULLIF(TRIM(data ->> 'lien_source'), '')                                                            AS "lien_source",
        NULLIF(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(data ->> 'nom'), '\.{2,}$', '…'), '(?<!etc)\.$', ''), '') AS "nom",
        NULLIF(TRIM(data ->> 'presentation_resume'), '')                                                    AS "presentation_resume",
        NULLIF(TRIM(data ->> 'presentation_detail'), '')                                                    AS "presentation_detail",
        NULLIF(TRIM(data ->> 'prise_rdv'), '')                                                              AS "prise_rdv",
        NULLIF(TRIM(data ->> 'recurrence'), '')                                                             AS "recurrence",
        NULLIF(TRIM(data ->> 'source'), '')                                                                 AS "source",
        NULLIF(TRIM(data ->> 'structure_id'), '')                                                           AS "structure_id",
        NULLIF(TRIM(data ->> 'telephone'), '')                                                              AS "telephone",
        NULLIF(TRIM(data ->> 'zone_diffusion_code'), '')                                                    AS "zone_diffusion_code",
        NULLIF(TRIM(data ->> 'zone_diffusion_nom'), '')                                                     AS "zone_diffusion_nom",
        NULLIF(TRIM(data ->> 'zone_diffusion_type'), '')                                                    AS "zone_diffusion_type"
    FROM source
),

-- dora removes suggested structures from its api, but does not remove the associated services
-- therefore filter these orphan services
final AS (
    SELECT services.*
    FROM services INNER JOIN structures ON services.structure_id = structures.id
)

SELECT * FROM final
