WITH source AS (
    SELECT * FROM {{ ref('_stg_carif_oref__source_filtered') }}
),

-- TODO(vmttn): there are around 10 actions sharing the same "numero" value
-- these actions are closely related to each other, but do not have strictly the same content

final AS (
    SELECT DISTINCT ON (1)
        NULLIF(TRIM(actions.data ->> '@numero'), '')                                                                         AS "numero",
        source.data ->> '@numero'                                                                                            AS "numero_formation",
        NULLIF(TRIM(organismes_formateurs.data ->> '@numero'), '')                                                           AS "numero_organisme_formateur",
        CAST(NULLIF(actions.data ->> '@datemaj', '00000000') AS DATE)                                                        AS "date_maj",
        CAST(actions.data ->> 'code-perimetre-recrutement' AS INTEGER)                                                       AS "code_perimetre_recrutement",
        NULLIF(TRIM(actions.data ->> 'conditions-specifiques'), '')                                                          AS "conditions_specifiques",
        NULLIF(TRIM(actions.data ->> 'detail-conditions-prise-en-charge'), '')                                               AS "detail_conditions_prise_en_charge",
        NULLIF(TRIM(actions.data ->> 'info-public-vise'), '')                                                                AS "info_public_vise",
        CAST(actions.data ->> 'modalites-enseignement' AS INTEGER)                                                           AS "modalites_enseignement",
        NULLIF(TRIM(actions.data ->> 'modalites-recrutement'), '')                                                           AS "modalites_recrutement",
        CAST(actions.data ->> 'prise-en-charge-frais-possible' AS BOOLEAN)                                                   AS "prise_en_charge_frais_possible",
        CAST(REPLACE(actions.data ->> 'prix-total-TTC', ',', '.') AS FLOAT)                                                  AS "prix_total_ttc",
        NULLIF(
            ARRAY_REMOVE(
                ARRAY(
                    SELECT NULLIF(TRIM(x.urlweb), '')
                    FROM JSONB_ARRAY_ELEMENTS_TEXT(actions.data -> 'url-action' -> 'urlweb') AS x (urlweb)
                ),
                NULL
            ),
            '{}'
        )                                                                                                                    AS "url_action",
        CAST(MD5(lieux_de_formation.data ->> 'coordonnees') AS TEXT)                                                         AS "hash_coordonnees_lieu_de_formation_principal",
        CAST(JSONB_PATH_QUERY_FIRST(actions.data, '$.extras[*] ? (@.\@info == "duree-hebdo") .extra[*]') ->> '$' AS INTEGER) AS "duree_hebdo",
        CAST(actions.data ->> 'nombre-heures-total' AS INTEGER)                                                              AS "nombre_heures_total",
        CAST(actions.data ->> 'modalites-entrees-sorties' AS INTEGER)                                                        AS "modalites_entrees_sorties",
        CAST(actions.data ->> 'conventionnement' AS BOOLEAN)                                                                 AS "conventionnement",
        NULLIF(TRIM(actions.data ->> 'duree-indicative'), '')                                                                AS "duree_indicative",
        NULLIF(TRIM(actions.data ->> 'frais-restants'), '')                                                                  AS "frais_restants",
        JSONB_BUILD_OBJECT(
            -- noqa: disable=layout.spacing
            'conventionnement',          actions.data -> 'conventionnement',
            'duree-indicative',          actions.data -> 'duree-indicative',
            'frais-restants',            actions.data -> 'frais-restants',
            'info-public-vise',          actions.data -> 'info-public-vise',
            'modalites-enseignement',    actions.data -> 'modalites-enseignement',
            'modalites-entrees-sorties', actions.data -> 'modalites-entrees-sorties',
            'modalites-recrutement',     actions.data -> 'modalites-recrutement',
            'nombre-heures-total',       actions.data -> 'nombre-heures-total',
            'organisme-financeur',       actions.data -> 'organisme-financeur',
            'session',                   actions.data -> 'session'
            -- noqa: enable=layout.spacing
        )                                                                                                                    AS "raw"
    FROM source,
        JSONB_PATH_QUERY(source.data, '$.action[*]') AS actions (data),
        JSONB_PATH_QUERY(actions.data, '$.organisme\-formateur[*]') AS organismes_formateurs (data),
        JSONB_PATH_QUERY(actions.data, '$.lieu\-de\-formation[*]') AS lieux_de_formation (data)
    ORDER BY
        NULLIF(TRIM(actions.data ->> '@numero'), ''),
        (lieux_de_formation.data ->> '@tag') = 'principal' DESC
)

SELECT * FROM final
