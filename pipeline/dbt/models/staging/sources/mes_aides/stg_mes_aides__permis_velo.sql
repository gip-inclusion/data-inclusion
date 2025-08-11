WITH source AS (
    {{ stg_source_header('mes_aides', 'permis_velo') }}
),

final AS (
    SELECT
        data #>> '{fields,ID}'                                               AS "id",
        SUBSTRING(data #>> '{fields,Liaisons Villes}' FROM '^(.+?) \(\d+\)') AS "liaisons_villes_nom",
        SUBSTRING(data #>> '{fields,Liaisons Villes}' FROM '^.+? \((\d+)\)') AS "liaisons_villes_code_postal",
        data #>> '{fields,N° Départements}'                                  AS "num_departement",
        data #>> '{fields,Liaisons Régions}'                                 AS "liaisons_region",
        data #>> '{fields,Nom}'                                              AS "nom",
        data #>> '{fields,Description}'                                      AS "description",
        data #>> '{fields,Bon à savoir}'                                     AS "bon_a_savoir",
        STRING_TO_ARRAY(data #>> '{fields,Liaisons Besoins}', ', ')          AS "liaisons_besoins",
        data #>> '{fields,Zone géographique}'                                AS "zone_geographique",
        data #>> '{fields,Démarches}'                                        AS "demarche",
        data #>> '{fields,"Modalité et versement"}'                          AS "modalite_versement",
        REPLACE(data #>> '{fields,SIRET}', ' ', '')                          AS "siret",
        data #>> '{fields,Slug Organisme}'                                   AS "slug_organisme",
        data #>> '{fields,Site}'                                             AS "site",
        data #>> '{fields,Nom Organisme}'                                    AS "nom_organisme",
        data #>> '{fields,Organisme Type}'                                   AS "organisme_type",
        (data #>> '{fields,Créée le}')::DATE                                 AS "creee_le",
        (data #>> '{fields,Modifiée le}')::DATE                              AS "modifiee_le",
        (data #>> '{fields,En ligne}')::BOOLEAN                              AS "en_ligne",
        TRIM(data #>> '{fields,Contact Email}')                              AS "contact_email",
        data #>> '{fields,Contact Tel}'                                      AS "contact_telephone",
        data #>> '{fields,Autres Conditions}'                                AS "autres_conditions",
        REPLACE(data #>> '{fields,Autres Justificatifs}', '•', '')           AS "autres_justificatifs",
        data #>> '{fields,Url Mes Aides}'                                    AS "url_mes_aides",
        data #>> '{fields,Formulaire Url}'                                   AS "formulaire_url"
    FROM source
)

SELECT * FROM final
