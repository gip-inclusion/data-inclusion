WITH source AS (
    {{ stg_source_header('mes_aides', 'aides') }}),

final AS (
    SELECT
        NULLIF(TRIM(data ->> 'Nom'), '')                                   AS "nom",
        data ->> 'ID'                                                      AS "id",
        NULLIF(TRIM(data ->> 'Organisme'), '')                             AS "organisme",
        NULLIF(TRIM(data ->> 'Organisme Financeur'), '')                   AS "organisme_financeur",
        NULLIF(TRIM(data ->> 'Description'), '')                           AS "description",
        CAST(data ->> 'Montant' AS FLOAT)                                  AS "montant",
        NULLIF(TRIM(data ->> 'Cumulable'), '') = 'checked'                 AS "cumulable",
        NULLIF(TRIM(data ->> 'Site'), '')                                  AS "site",
        UNACCENT(LOWER(NULLIF(TRIM(data ->> 'Zone géographique'), '')))    AS "zone_geographique",
        CAST(data ->> 'Créée le' AS DATE)                                  AS "creee_le",
        CAST(data ->> 'Modifiée le' AS DATE)                               AS "modifiee_le",
        CAST(data ->> 'Mise à jour le' AS DATE)                            AS "mise_a_jour_le",
        CAST(data ->> 'Expiration' AS DATE)                                AS "expiration",
        COALESCE(NULLIF(TRIM(data ->> 'En ligne'), '') = 'checked', FALSE) AS "en_ligne",
        NULLIF(TRIM(data ->> 'Voir l''aide'), '')                          AS "voir_l_aide",
        NULLIF(TRIM(data ->> 'Autres Conditions'), '')                     AS "autres_conditions",
        NULLIF(TRIM(data ->> 'Autres Justificatifs'), '')                  AS "autres_justificatifs",
        NULLIF(TRIM(data ->> 'Bon à savoir'), '')                          AS "bon_a_savoir",
        NULLIF(TRIM(data ->> 'Démarches'), '')                             AS "demarches",
        NULLIF(TRIM(data ->> 'Modalité et versement'), '')                 AS "modalite_et_versement",
        NULLIF(TRIM(data ->> 'Documentation Url'), '')                     AS "documentation_url",
        NULLIF(TRIM(data ->> 'Règlement Url'), '')                         AS "reglement_url",
        NULLIF(TRIM(data ->> 'Formulaire Url'), '')                        AS "formulaire_url",
        NULLIF(TRIM(data ->> 'Contact Dossier'), '')                       AS "contact_dossier"

    FROM source
)

SELECT * FROM final
