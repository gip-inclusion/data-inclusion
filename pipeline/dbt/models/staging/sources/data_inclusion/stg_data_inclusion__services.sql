WITH source AS (
    {{ stg_source_header('data_inclusion_extra', 'services') }}
),

final AS (
    SELECT
        _di_source_id                                                                    AS "_di_source_id",
        (data ->> 'contact_public')::BOOLEAN                                             AS "contact_public",
        (data ->> 'cumulable')::BOOLEAN                                                  AS "cumulable",
        (data ->> 'date_creation')::TIMESTAMP WITH TIME ZONE                             AS "date_creation",
        (data ->> 'date_maj')::TIMESTAMP WITH TIME ZONE                                  AS "date_maj",
        (data ->> 'date_suspension')::TIMESTAMP WITH TIME ZONE                           AS "date_suspension",
        (data ->> 'latitude')::FLOAT                                                     AS "latitude",
        (data ->> 'longitude')::FLOAT                                                    AS "longitude",
        ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(data -> 'modes_accueil'))::TEXT [] AS "modes_accueil",
        ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(data -> 'profils'))::TEXT []       AS "profils",
        ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(data -> 'thematiques'))::TEXT []   AS "thematiques",
        ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(data -> 'types'))::TEXT []         AS "types",
        STRING_TO_ARRAY(NULLIF(TRIM(data ->> 'justificatifs'), ''), ',')                 AS "justificatifs",
        STRING_TO_ARRAY(NULLIF(TRIM(data ->> 'pre_requis'), ''), ',')                    AS "pre_requis",
        data ->> 'adresse'                                                               AS "adresse",
        data ->> 'code_insee'                                                            AS "code_insee",
        data ->> 'code_postal'                                                           AS "code_postal",
        data ->> 'commune'                                                               AS "commune",
        data ->> 'complement_adresse'                                                    AS "complement_adresse",
        NULLIF(TRIM(data ->> 'contact_nom'), '')                                         AS "contact_nom",
        NULLIF(TRIM(data ->> 'contact_prenom'), '')                                      AS "contact_prenom",
        NULLIF(TRIM(data ->> 'courriel'), '')                                            AS "courriel",
        data ->> 'formulaire_en_ligne'                                                   AS "formulaire_en_ligne",
        data ->> 'frais_autres'                                                          AS "frais_autres",
        data ->> 'frais'                                                                 AS "frais",
        data ->> 'id'                                                                    AS "id",
        data ->> 'lien_source'                                                           AS "lien_source",
        data ->> 'nom'                                                                   AS "nom",
        data ->> 'presentation_resume'                                                   AS "presentation_resume",
        data ->> 'presentation_detail'                                                   AS "presentation_detail",
        data ->> 'prise_rdv'                                                             AS "prise_rdv",
        data ->> 'recurrence'                                                            AS "recurrence",
        data ->> 'source'                                                                AS "source",
        data ->> 'structure_id'                                                          AS "structure_id",
        NULLIF(TRIM(data ->> 'telephone'), '')                                           AS "telephone",
        NULLIF(TRIM(data ->> 'zone_diffusion_code'), '')                                 AS "zone_diffusion_code",
        NULLIF(TRIM(data ->> 'zone_diffusion_nom'), '')                                  AS "zone_diffusion_nom",
        data ->> 'zone_diffusion_type'                                                   AS "zone_diffusion_type"
    FROM source
)

SELECT * FROM final
