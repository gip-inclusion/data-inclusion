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
        NULLIF(TRIM(data ->> 'adresse'), '')                                                                AS "adresse",
        NULLIF(TRIM(data ->> 'code_insee'), '')                                                             AS "code_insee",
        NULLIF(TRIM(data ->> 'code_postal'), '')                                                            AS "code_postal",
        NULLIF(TRIM(data ->> 'commune'), '')                                                                AS "commune",
        NULLIF(TRIM(data ->> 'complement_adresse'), '')                                                     AS "complement_adresse",
        NULLIF(TRIM(data ->> 'conditions_acces'), '')                                                       AS "conditions_acces",
        CAST((data ->> 'contact_public') AS BOOLEAN)                                                        AS "contact_public",
        NULLIF(TRIM(data ->> 'contact_nom_prenom'), '')                                                     AS "contact_nom_prenom",
        NULLIF(TRIM(data ->> 'courriel'), '')                                                               AS "courriel",
        NULLIF(TRIM(data ->> 'description'), '')                                                            AS "description",
        NULLIF(TRIM(data ->> 'frais_autres'), '')                                                           AS "frais_autres",
        NULLIF(TRIM(data ->> 'frais'), '')                                                                  AS "frais",
        NULLIF(TRIM(data ->> 'horaires_accueil'), '')                                                       AS "horaires_accueil",
        NULLIF(TRIM(data ->> 'id'), '')                                                                     AS "id",
        NULLIF(TRIM(data ->> 'lien_mobilisation'), '')                                                      AS "lien_mobilisation",
        NULLIF(TRIM(data ->> 'lien_source'), '')                                                            AS "lien_source",
        NULLIF(ARRAY(SELECT JSONB_ARRAY_ELEMENTS_TEXT(NULLIF(data -> 'mobilisable_par', 'null'))), '{}')    AS "mobilisable_par",
        NULLIF(ARRAY(SELECT JSONB_ARRAY_ELEMENTS_TEXT(NULLIF(data -> 'modes_mobilisation', 'null'))), '{}') AS "modes_mobilisation",
        NULLIF(TRIM(data ->> 'mobilisation_precisions'), '')                                                AS "mobilisation_precisions",
        NULLIF(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(data ->> 'nom'), '\.{2,}$', '…'), '(?<!etc)\.$', ''), '') AS "nom",
        NULLIF(ARRAY(SELECT JSONB_ARRAY_ELEMENTS_TEXT(NULLIF(data -> 'modes_accueil', 'null'))), '{}')      AS "modes_accueil",
        NULLIF(TRIM(data ->> 'presentation_resume'), '')                                                    AS "presentation_resume",
        NULLIF(TRIM(data ->> 'presentation_detail'), '')                                                    AS "presentation_detail",
        NULLIF(ARRAY(SELECT JSONB_ARRAY_ELEMENTS_TEXT(NULLIF(data -> 'publics', 'null'))), '{}')            AS "publics",
        NULLIF(TRIM(data ->> 'publics_precisions'), '')                                                     AS "publics_precisions",
        NULLIF(TRIM(data ->> 'recurrence'), '')                                                             AS "recurrence",
        NULLIF(TRIM(data ->> 'source'), '')                                                                 AS "source",
        NULLIF(TRIM(data ->> 'structure_id'), '')                                                           AS "structure_id",
        NULLIF(TRIM(data ->> 'telephone'), '')                                                              AS "telephone",
        NULLIF(ARRAY(SELECT JSONB_ARRAY_ELEMENTS_TEXT(NULLIF(data -> 'thematiques', 'null'))), '{}')        AS "thematiques",
        NULLIF(TRIM(data ->> 'zone_diffusion_code'), '')                                                    AS "zone_diffusion_code",
        NULLIF(TRIM(data ->> 'zone_diffusion_nom'), '')                                                     AS "zone_diffusion_nom",
        NULLIF(TRIM(data ->> 'zone_diffusion_type'), '')                                                    AS "zone_diffusion_type",
        NULLIF(ARRAY(SELECT JSONB_ARRAY_ELEMENTS_TEXT(NULLIF(data -> 'zone_eligibilite', 'null'))), '{}')   AS "zone_eligibilite",
        NULLIF(TRIM(data -> 'types' ->> 0), '')                                                             AS "type",
        data -> 'labels_financement'                                                                        AS "labels_financement",
        data -> 'documents_a_completer'                                                                     AS "documents_a_completer",
        NULLIF(TRIM(data ->> 'formulaire_en_ligne_a_completer'), '')                                        AS "formulaire_en_ligne_a_completer"
    FROM source
),

-- dora removes suggested structures from its api, but does not remove the associated services
-- therefore filter these orphan services
final AS (
    -- After quite some investigation by multiple developers it has been found too difficult
    -- to determine exactly WHY there were systematic duplicates in the production API (not in staging or dev)
    -- *something* there is non-deterministic but chances are that this API will someday evolve,
    -- and the fix is VERY obvious on our side so let's just do it (also, we know they are exact duplicates)
    SELECT DISTINCT services.*
    FROM services INNER JOIN structures ON services.structure_id = structures.id
)

SELECT * FROM final
