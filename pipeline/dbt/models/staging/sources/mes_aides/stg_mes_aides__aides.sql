WITH source AS (
    {{ stg_source_header('mes_aides', 'aides') }}),

final AS (
    SELECT
        NULLIF(TRIM(data ->> 'nom'), '')                               AS "nom",
        data ->> 'id'                                                  AS "id",
        NULLIF(TRIM(data ->> 'site'), '')                              AS "site",
        NULLIF(TRIM(data ->> 'organisme'), '')                         AS "organisme",
        NULLIF(TRIM(data ->> 'organismeFinanceur'), '')                AS "organisme_financeur",
        NULLIF(TRIM(data ->> 'description'), '')                       AS "description",
        CAST(data ->> 'montant' AS FLOAT)                              AS "montant",
        CAST(data ->> 'cumulable' AS BOOLEAN)                          AS "cumulable",
        UNACCENT(LOWER(NULLIF(TRIM(data ->> 'zoneGeographique'), ''))) AS "zone_geographique",
        NULLIF(TRIM(data ->> 'type'), '')                              AS "type",
        NULLIF(TRIM(data -> 'conditions' ->> 'autres'), '')            AS "conditions__autres",
        NULLIF(TRIM(data -> 'justificatifs' ->> 'autres'), '')         AS "justificatifs__autres",
        NULLIF(TRIM(data -> 'contact' ->> 'dossier'), '')              AS "contact__dossier",
        NULLIF(TRIM(data -> 'demarche' ->> 'formulaireUrl'), '')       AS "demarche__formulaire_url",
        NULLIF(TRIM(data -> 'demarche' ->> 'bonASavoir'), '')          AS "demarche__bon_a_savoir",
        NULLIF(TRIM(data -> 'demarche' ->> 'reglementUrl'), '')        AS "demarche__reglement_url",
        NULLIF(TRIM(data -> 'demarche' ->> 'documentationUrl'), '')    AS "demarche__documentation_url",
        NULLIF(TRIM(data -> 'demarche' ->> 'modalites'), '')           AS "demarche__modalites",
        CAST(data -> 'metadata' ->> 'dateCreation' AS DATE)            AS "metadata__date_creation",
        CAST(data -> 'metadata' ->> 'dateModification' AS DATE)        AS "metadata__date_modification",
        CAST(data -> 'metadata' ->> 'expiration' AS DATE)              AS "metadata__expiration"
    FROM source
)

SELECT * FROM final
