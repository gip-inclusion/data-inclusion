WITH source AS (
    {{ stg_source_header('monenfant', 'creches') }}
),

creches AS (
    SELECT
        _di_source_id                                                                                                   AS "_di_source_id",
        CAST(data ->> 'avip' AS BOOLEAN)                                                                                AS "avip",
        CAST(data ->> 'longitude' AS FLOAT)                                                                             AS "longitude",
        CAST(data ->> 'latitude' AS FLOAT)                                                                              AS "latitude",
        CAST(data #>> '{details,modalite,autre}' AS BOOLEAN)                                                            AS "details__modalite__autre",
        CAST(data #>> '{details,modalite,intercom}' AS BOOLEAN)                                                         AS "details__modalite__intercom",
        CAST(data #>> '{details,modalite,cafMsaPsu}' AS BOOLEAN)                                                        AS "details__modalite__caf_msa_psu",
        CAST(data #>> '{details,modalite,cafMsaPaje}' AS BOOLEAN)                                                       AS "details__modalite__caf_msa_paje",
        CAST(data #>> '{details,modalite,entreprise}' AS BOOLEAN)                                                       AS "details__modalite__entreprise",
        CAST(ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(data -> '{details,modalite,conditionTravail}')) AS TEXT [])  AS "details__modalite__condition_travail",
        CAST(ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(data -> '{details,modalite,conditionDomicile}')) AS TEXT []) AS "details__modalite__condition_domicile",
        CAST(data #>> '{details,modalite,conseilDepartemental}' AS BOOLEAN)                                             AS "details__modalite__conseil_departemental",
        CAST(data #>> '{details,modalite,modalitesTarifairePhraseType}' AS BOOLEAN)                                     AS "details__modalite__modalites_tarifaire_phrase_type",
        CAST(data #>> '{details,infosPratiques,repas}' AS BOOLEAN)                                                      AS "details__infos_pratiques__repas",
        CAST(data #>> '{details,infosPratiques,couches}' AS BOOLEAN)                                                    AS "details__infos_pratiques__couches",
        CAST(data #>> '{details,infosPratiques,rendezvous}' AS BOOLEAN)                                                 AS "details__infos_pratiques__rendezvous",
        CAST(data #>> '{details,infosPratiques,fermetureAn}' AS BOOLEAN)                                                AS "details__infos_pratiques__fermeture_an",
        CAST(data #>> '{details,infosPratiques,fermetureAout}' AS BOOLEAN)                                              AS "details__infos_pratiques__fermeture_aout",
        CAST(data #>> '{details,infosPratiques,fermetureJuillet}' AS BOOLEAN)                                           AS "details__infos_pratiques__fermeture_juillet",
        CAST(data #>> '{details,infosPratiques,ouvertureAnnuelle}' AS BOOLEAN)                                          AS "details__infos_pratiques__ouverture_annuelle",
        data ->> 'resultId'                                                                                             AS "id",
        NULLIF(data ->> 'ville', '')                                                                                    AS "ville",
        NULLIF(data ->> 'telephone', '')                                                                                AS "telephone",
        data ->> 'nom'                                                                                                  AS "nom",
        NULLIF(data ->> 'mail', '')                                                                                     AS "mail",
        data ->> 'resultId'                                                                                             AS "result_id",
        data ->> 'omegaId'                                                                                              AS "omega_id",
        data ->> 'statusCompte'                                                                                         AS "status_compte",
        TO_DATE(data ->> 'derniereModifDate', 'DD/MM/YYYY')                                                             AS "derniere_modif_date",
        data ->> 'sousCategorie'                                                                                        AS "sous_categorie",
        data ->> 'dispo'                                                                                                AS "dispo",
        ARRAY(SELECT * FROM JSONB_ARRAY_ELEMENTS_TEXT(data -> 'dispoCreche'))                                           AS "dispo__creche",
        data ->> 'resultStructId'                                                                                       AS "result_struct_id",
        data ->> 'adresse'                                                                                              AS "adresse",
        NULLIF(data #>> '{details,website}', '')                                                                        AS "details__website",
        data #>> '{details,dispo,allNonRenseigne}'                                                                      AS "details__dispo__all_non_renseigne",
        data #>> '{details,typeStructure}'                                                                              AS "details__type_structure",
        CASE
            WHEN LENGTH(data #>> '{details,modalite,conditionAdmision}') = 1 THEN NULL
            WHEN data #>> '{details,modalite,conditionAdmision}' = '' THEN NULL
            ELSE data #>> '{details,modalite,conditionAdmision}'
        END                                                                                                             AS "details__modalite__condition_admision",
        CASE
            WHEN LENGTH(data #>> '{details,modalite,modalitesInscription}') = 1 THEN NULL
            WHEN data #>> '{details,modalite,modalitesInscription}' = '' THEN NULL
            ELSE data #>> '{details,modalite,modalitesInscription}'
        END                                                                                                             AS "details__modalite__modalites_inscription",
        data #>> '{details,modalite,modalitesTarifaireLibre}'                                                           AS "details__modalite__modalites_tarifaire_libre",
        data #>> '{details,modalite,modalitesTarifaireTexte}'                                                           AS "details__modalite__modalites_tarifaire_texte",
        data #>> '{details,infosPratiques,ageMax}'                                                                      AS "details__infos_pratiques__age_max",
        data #>> '{details,infosPratiques,ageMin}'                                                                      AS "details__infos_pratiques__age_min",
        data #>> '{details,infosPratiques,capacite}'                                                                    AS "details__infos_pratiques__capacite",
        data #>> '{details,infosPratiques,handicap}'                                                                    AS "details__infos_pratiques__handicap",
        data #>> '{details,infosPratiques,ageMaxUnit}'                                                                  AS "details__infos_pratiques__age_max_unit",
        data #>> '{details,infosPratiques,ageMinUnit}'                                                                  AS "details__infos_pratiques__age_min_unit",
        NULLIF(data #>> '{details,infosPratiques,jourHoraire}', '')                                                     AS "details__infos_pratiques__jour_horaire",
        data #>> '{details,infosPratiques,jourHoraireJson}'                                                             AS "details__infos_pratiques__jour_horaire_json",
        data #>> '{details,presentation,equipe}'                                                                        AS "details__presentation__equipe",
        data #>> '{details,presentation,gestionnaire}'                                                                  AS "details__presentation__gestionnaire",
        data #>> '{details,presentation,nomGestionnaire}'                                                               AS "details__presentation__nom_gestionnaire",
        CASE
            WHEN LENGTH(data #>> '{details,presentation,structureProjet}') = 1 THEN NULL
            WHEN data #>> '{details,presentation,structureProjet}' = '' THEN NULL
            ELSE data #>> '{details,presentation,structureProjet}'
        END                                                                                                             AS "details__presentation__structure_projet",
        data ->> 'type'                                                                                                 AS "type"
    FROM source
),

final AS (
    SELECT DISTINCT ON (id) *
    FROM creches
)

SELECT * FROM final
