WITH source AS (
    {{ stg_source_header('mes_aides', 'permis_velo') }}),

final AS (
    SELECT
        fields ->> 'ID'                                                       AS "id",
        SUBSTRING(fields ->> 'Liaisons Villes' FROM '^(.+?) \(\d+\)')         AS "liaisons_villes_nom",
        SUBSTRING(fields ->> 'Liaisons Villes' FROM '^.+? \((\d+)\)')         AS "liaisons_villes_code_postal",
        NULLIF(TRIM(fields ->> 'N° Départements'), '')                        AS "num_departement",
        NULLIF(TRIM(fields ->> 'Liaisons Régions'), '')                       AS "liaisons_region",
        NULLIF(TRIM(fields ->> 'Nom'), '')                                    AS "nom",
        NULLIF(TRIM(fields ->> 'Description'), '')                            AS "description",
        NULLIF(TRIM(fields ->> 'Bon à savoir'), '')                           AS "bon_a_savoir",
        STRING_TO_ARRAY(fields ->> 'Liaisons Besoins', ', ')                  AS "liaisons_besoins",
        NULLIF(TRIM(fields ->> 'Zone géographique'), '')                      AS "zone_geographique",
        NULLIF(TRIM(fields ->> 'Démarches'), '')                              AS "demarche",
        NULLIF(TRIM(fields ->> 'Modalité et versement'), '')                  AS "modalite_versement",
        REPLACE(fields ->> 'SIRET', ' ', '')                                  AS "siret",
        NULLIF(TRIM(fields ->> 'Slug Organisme'), '')                         AS "slug_organisme",
        NULLIF(TRIM(fields ->> 'Liaison Organisme'), '')                      AS "liaison_organisme",
        NULLIF(TRIM(fields ->> 'Site'), '')                                   AS "site",
        NULLIF(TRIM(fields ->> 'Nom Organisme'), '')                          AS "nom_organisme",
        NULLIF(TRIM(fields ->> 'Organisme Type'), '')                         AS "organisme_type",
        CAST(fields ->> 'Créée le' AS DATE)                                   AS "creee_le",
        CAST(fields ->> 'Modifié le' AS DATE)                                 AS "modifie_le",
        CAST(fields ->> 'En ligne' AS BOOLEAN)                                AS "en_ligne",
        NULLIF(TRIM(fields ->> 'Contact Email'), '')                          AS "contact_email",
        NULLIF(TRIM(fields ->> 'Contact Tel'), '')                            AS "contact_telephone",
        NULLIF(TRIM(fields ->> 'Autres Conditions'), '')                      AS "autres_conditions",
        NULLIF(TRIM(REPLACE(fields ->> 'Autres Justificatifs', '•', '')), '') AS "autres_justificatifs",
        NULLIF(TRIM(fields ->> 'Url Mes Aides'), '')                          AS "url_mes_aides",
        NULLIF(TRIM(fields ->> 'Formulaire Url'), '')                         AS "formulaire_url"
    FROM (
        SELECT data -> 'fields' AS "fields"
        FROM source
    )
)

SELECT * FROM final
