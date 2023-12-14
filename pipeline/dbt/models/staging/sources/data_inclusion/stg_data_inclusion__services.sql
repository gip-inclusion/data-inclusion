WITH source AS (
    {{ stg_source_header('data_inclusion_extra', 'services') }}
),

final AS (
    SELECT
        _di_source_id                                                                                                          AS "_di_source_id",
        'data-inclusion'                                                                                                       AS "source",
        CAST(data ->> 'contact_public' AS BOOLEAN)                                                                             AS "contact_public",
        CAST(data ->> 'cumulable' AS BOOLEAN)                                                                                  AS "cumulable",
        TO_DATE(data ->> 'date_creation', 'DD/MM/YYYY')                                                                        AS "date_creation",
        TO_DATE(data ->> 'date_maj', 'DD/MM/YYYY')                                                                             AS "date_maj",
        TO_DATE(data ->> 'date_suspension', 'DD/MM/YYYY')                                                                      AS "date_suspension",
        CAST(data ->> 'latitude' AS FLOAT)                                                                                     AS "latitude",
        CAST(data ->> 'longitude' AS FLOAT)                                                                                    AS "longitude",
        ARRAY_REMOVE(ARRAY(SELECT value FROM JSONB_EACH_TEXT(data) WHERE key ~* 'modes_accueil.\d+'), NULL)                    AS "modes_accueil",
        ARRAY_REMOVE(ARRAY(SELECT value FROM JSONB_EACH_TEXT(data) WHERE key ~* 'profils.\d+'), NULL)                          AS "profils",
        ARRAY_REMOVE(ARRAY(SELECT value FROM JSONB_EACH_TEXT(data) WHERE key ~* 'thematiques.\d+'), NULL)                      AS "thematiques",
        ARRAY_REMOVE(ARRAY(SELECT value FROM JSONB_EACH_TEXT(data) WHERE key ~* 'types.\d+'), NULL)                            AS "types",
        ARRAY_REMOVE(ARRAY(SELECT value FROM JSONB_EACH_TEXT(data) WHERE key ~* 'justificatifs.\d+'), NULL)                    AS "justificatifs",
        ARRAY_REMOVE(ARRAY(SELECT value FROM JSONB_EACH_TEXT(data) WHERE key ~* 'pre_requis.\d+'), NULL)                       AS "pre_requis",
        data ->> 'adresse'                                                                                                     AS "adresse",
        data ->> 'code_insee'                                                                                                  AS "code_insee",
        data ->> 'code_postal'                                                                                                 AS "code_postal",
        data ->> 'commune'                                                                                                     AS "commune",
        data ->> 'complement_adresse'                                                                                          AS "complement_adresse",
        data ->> 'contact_nom'                                                                                                 AS "contact_nom",
        data ->> 'contact_prenom'                                                                                              AS "contact_prenom",
        data ->> 'courriel'                                                                                                    AS "courriel",
        data ->> 'formulaire_en_ligne'                                                                                         AS "formulaire_en_ligne",
        data ->> 'frais_autres'                                                                                                AS "frais_autres",
        ARRAY_REMOVE(ARRAY(SELECT value FROM JSONB_EACH_TEXT(data) WHERE key ~* 'frais.\d+'), NULL)                            AS "frais",
        data ->> 'id'                                                                                                          AS "id",
        data ->> 'lien_source'                                                                                                 AS "lien_source",
        data ->> 'nom'                                                                                                         AS "nom",
        data ->> 'presentation_resume'                                                                                         AS "presentation_resume",
        data ->> 'presentation_detail'                                                                                         AS "presentation_detail",
        data ->> 'prise_rdv'                                                                                                   AS "prise_rdv",
        data ->> 'recurrence'                                                                                                  AS "recurrence",
        data ->> 'structure_id'                                                                                                AS "structure_id",
        data ->> 'telephone'                                                                                                   AS "telephone",
        data ->> 'zone_diffusion_code'                                                                                         AS "zone_diffusion_code",
        data ->> 'zone_diffusion_nom'                                                                                          AS "zone_diffusion_nom",
        data ->> 'zone_diffusion_type'                                                                                         AS "zone_diffusion_type",
        ARRAY_REMOVE(ARRAY(SELECT value FROM JSONB_EACH_TEXT(data) WHERE key ~* 'modes_orientation_beneficiaire.\d+'), NULL)   AS "modes_orientation_beneficiaire",
        data ->> 'modes_orientation_beneficiaire_autres'                                                                       AS "modes_orientation_beneficiaire_autres",
        ARRAY_REMOVE(ARRAY(SELECT value FROM JSONB_EACH_TEXT(data) WHERE key ~* 'modes_orientation_accompagnateur.\d+'), NULL) AS "modes_orientation_accompagnateur",
        data ->> 'modes_orientation_accompagnateur_autres'                                                                     AS "modes_orientation_accompagnateur_autres"
    FROM source
    WHERE
        NOT COALESCE(CAST(data ->> '__ignore__' AS BOOLEAN), FALSE)
)

SELECT * FROM final
