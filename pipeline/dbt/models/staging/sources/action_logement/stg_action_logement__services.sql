WITH source AS (
    {{ stg_source_header('action_logement', 'services') }}),

final AS (
    SELECT
        source.data ->> 'id'                                      AS "id",
        CURRENT_DATE AT TIME ZONE 'Europe/Paris'                  AS "date_maj",
        ARRAY_REMOVE(ARRAY(
            SELECT x.value FROM JSONB_EACH_TEXT(source.data) AS x
            WHERE x.key ~* 'modes_accueil.\d+'
        ), NULL)                                                  AS "modes_accueil",
        ARRAY_REMOVE(ARRAY(
            SELECT x.value FROM JSONB_EACH_TEXT(source.data) AS x
            WHERE x.key ~* 'profils.\d+'
        ), NULL)                                                  AS "profils",
        ARRAY_REMOVE(ARRAY(
            SELECT x.value FROM JSONB_EACH_TEXT(source.data) AS x
            WHERE x.key ~* 'thematiques.\d+'
        ), NULL)                                                  AS "thematiques",
        ARRAY_REMOVE(ARRAY(
            SELECT x.value FROM JSONB_EACH_TEXT(source.data) AS x
            WHERE x.key ~* 'types.\d+'
        ), NULL)                                                  AS "types",
        ARRAY_REMOVE(ARRAY(
            SELECT x.value FROM JSONB_EACH_TEXT(source.data) AS x
            WHERE x.key ~* 'justificatifs.\d+'
        ), NULL)                                                  AS "justificatifs",
        ARRAY_REMOVE(ARRAY(
            SELECT x.value FROM JSONB_EACH_TEXT(source.data) AS x
            WHERE x.key ~* 'pre_requis.\d+'
        ), NULL)                                                  AS "pre_requis",
        ARRAY_REMOVE(ARRAY(
            SELECT x.value FROM JSONB_EACH_TEXT(source.data) AS x
            WHERE x.key ~* 'modes_orientation_accompagnateur.\d+'
        ), NULL)                                                  AS "modes_orientation_accompagnateur",
        ARRAY_REMOVE(ARRAY(
            SELECT x.value FROM JSONB_EACH_TEXT(source.data) AS x
            WHERE x.key ~* 'modes_orientation_beneficiaire.\d+'
        ), NULL)                                                  AS "modes_orientation_beneficiaire",
        ARRAY_REMOVE(ARRAY(
            SELECT x.value FROM JSONB_EACH_TEXT(source.data) AS x
            WHERE x.key ~* 'frais.\d+'
        ), NULL)                                                  AS "frais",
        source.data ->> 'modes_orientation_accompagnateur_autres' AS "modes_orientation_accompagnateur_autres",
        source.data ->> 'modes_orientation_beneficiaire_autres'   AS "modes_orientation_beneficiaire_autres",
        source.data ->> 'lien_mobilisation'                       AS "formulaire_en_ligne",
        source.data ->> 'frais_autres'                            AS "frais_autres",
        source.data ->> 'nom'                                     AS "nom",
        source.data ->> 'page_web'                                AS "page_web",
        CASE
            WHEN LENGTH(source.data ->> 'description') >= 280
                THEN LEFT(source.data ->> 'description', 279) || '…'
            ELSE source.data ->> 'description'
        END                                                       AS "presentation_resume",
        source.data ->> 'description'                             AS "presentation_detail",
        source.data ->> 'prise_rdv'                               AS "prise_rdv",
        source.data ->> 'recurrence'                              AS "recurrence",
        NULLIF(TRIM(source.data ->> 'zone_diffusion_code'), '')   AS "zone_diffusion_code",
        NULLIF(TRIM(source.data ->> 'zone_diffusion_nom'), '')    AS "zone_diffusion_nom",
        source.data ->> 'zone_diffusion_type'                     AS "zone_diffusion_type",
        source.data ->> 'lien_source.1'                           AS "lien_source"
    FROM source
    WHERE
        NOT COALESCE(CAST(source.data ->> '__ignore__' AS BOOLEAN), FALSE)
)

SELECT * FROM final
