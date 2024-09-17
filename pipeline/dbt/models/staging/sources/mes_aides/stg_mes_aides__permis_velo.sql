WITH source AS (
    {{ stg_source_header('mes_aides', 'permis_velo') }}
),

final AS (
    SELECT
        _di_source_id                                                          AS "_di_source_id",
        data #>> '{fields,ID}'                                                 AS "id",
        TRIM(SUBSTRING(data #>> '{fields,Liaisons Villes}' FROM '^(.*) \('))   AS "ville_nom",
        TRIM(SUBSTRING(data #>> '{fields,Liaisons Villes}' FROM '\((\d+)\)$')) AS "code_postal",
        SUBSTRING(
            data #>> '{fields,Liaisons Départements}' FROM POSITION('-' IN data #>> '{fields,Liaisons Départements}') + 1  -- noqa
        )                                                                      AS "departement",
        data #>> '{fields,Liaisons Régions}'                                   AS "region",
        data #>> '{fields,Nom}'                                                AS "nom",
        data #>> '{fields,Liaisons Besoins}'                                   AS "besoins_mes_aides",
        data #>> '{fields,Slug Thématiques}'                                   AS "thematique_mes_aides",
        data #>> '{fields,Zone géographique}'                                  AS "zone_diffusion_type",
        data #>> '{fields,Nature}'                                             AS "frais_autres",
        data #>> '{fields,Démarches}'                                          AS "mode_orientation_beneficiare_autre",
        data #>> '{fields,"Modalité et versement"}'                            AS "presentation_detail",
        data #>> '{fields,"Méthode"}'                                          AS "methode",
        data #>> '{fields,SIRET}'                                              AS "siret_structure",
        data #>> '{fields,Slug Organisme}'                                     AS "lien_source_structure",
        data #>> '{fields,Site}'                                               AS "page_web",
        data #>> '{fields,Nom Organisme}'                                      AS "nom_structure",
        data #>> '{fields,Organisme Type}'                                     AS "typologie_structure",
        CAST(data #>> '{fields,Créée le}' AS DATE)                             AS "date_creation",
        CAST(data #>> '{fields,Modifiée le}' AS DATE)                          AS "date_maj",
        CAST(data #>> '{fields,En ligne}' AS BOOLEAN)                          AS "mode_acceuil",
        TRIM(data #>> '{fields,Contact Email}')                                AS "courriel",
        data #>> '{fields,Contact Tel}'                                        AS "telephone",
        data #>> '{fields,Autres Conditions}'                                  AS "autres_conditions",
        data #>> '{fields,Url Mes Aides}'                                      AS "lien_source",
        STRING_TO_ARRAY(data #>> '{fields,Type}', ',')                         AS "types"
    FROM source
)

SELECT * FROM final
