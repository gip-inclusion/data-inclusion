WITH source AS (
    {{ stg_source_header('mes_aides', 'permis_velo') }}
),

final AS (
    SELECT
        _di_source_id                                                          AS "_di_source_id",
        data #>> '{fields,ID}'                                                 AS "id",
        TRIM(SUBSTRING(data #>> '{fields,Liaisons Villes}' FROM '^(.*) \('))   AS "liaisons_ville_nom",
        TRIM(SUBSTRING(data #>> '{fields,Liaisons Villes}' FROM '\((\d+)\)$')) AS "liaisons_code_postal",
        data #>> '{fields,N° Départements}'                                    AS "num_departement",
        data #>> '{fields,Liaisons Régions}'                                   AS "liaisons_region",
        data #>> '{fields,Nom}'                                                AS "nom",
        data #>> '{fields,Description}'                                        AS "description",
        data #>> '{fields,Bon à savoir}'                                       AS "bon_a_savoir",
        STRING_TO_ARRAY(data #>> '{fields,Liaisons Besoins}', ', ')            AS "liaisons_besoins_mes_aides",
        data #>> '{fields,Slug Thématiques}'                                   AS "slug_thematique_mes_aides",
        data #>> '{fields,Zone géographique}'                                  AS "zone_geographique",
        (
            SELECT ARRAY_AGG(element)
            FROM JSONB_ARRAY_ELEMENTS_TEXT(data -> 'fields' -> 'Nature') AS element -- noqa: AL05
        )                                                                      AS "nature",
        data #>> '{fields,Démarches}'                                          AS "demarche",
        data #>> '{fields,"Modalité et versement"}'                            AS "modalite_versement",
        data #>> '{fields,"Méthode"}'                                          AS "methode",
        data #>> '{fields,SIRET}'                                              AS "siret_structure",
        data #>> '{fields,Slug Organisme}'                                     AS "slug_organisme_structure",
        data #>> '{fields,Site}'                                               AS "site",
        data #>> '{fields,Nom Organisme}'                                      AS "nom_organisme_structure",
        data #>> '{fields,Organisme Type}'                                     AS "typologie_structure",
        CAST(data #>> '{fields,Créée le}' AS DATE)                             AS "creee_le",
        CAST(data #>> '{fields,Modifiée le}' AS DATE)                          AS "modifiee_le",
        CAST(data #>> '{fields,En ligne}' AS BOOLEAN)                          AS "en_ligne",
        TRIM(data #>> '{fields,Contact Email}')                                AS "contact_email",
        data #>> '{fields,Contact Tel}'                                        AS "contact_telephone",
        data #>> '{fields,Autres Conditions}'                                  AS "autres_conditions",
        REPLACE(data #>> '{fields,Autres Justificatifs}', '•', '')             AS "autres_justificatifs",
        data #>> '{fields,Url Mes Aides}'                                      AS "url_mes_aides",
        data #>> '{fields,Formulaire Url}'                                     AS "formulaire_url",
        STRING_TO_ARRAY(data #>> '{fields,Type}', ', ')                        AS "types"
    FROM source
)

SELECT * FROM final
