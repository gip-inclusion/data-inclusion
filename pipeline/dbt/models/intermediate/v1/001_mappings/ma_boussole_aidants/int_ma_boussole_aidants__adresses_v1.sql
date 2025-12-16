WITH structures AS (
    SELECT * FROM {{ ref('stg_ma_boussole_aidants__structures') }}
),

communes AS (
    SELECT * FROM {{ ref('stg_decoupage_administratif__communes') }}
),

arrondissements AS (
    SELECT * FROM {{ ref('stg_decoupage_administratif__arrondissements') }}
),

communes_associees_deleguees AS (
    SELECT * FROM {{ ref('stg_decoupage_administratif__communes_associees_deleguees') }}
),

final AS (
    SELECT
        'ma-boussole-aidants'                                                            AS "source",
        'ma-boussole-aidants--' || structures.id_structure                               AS "id",
        structures.adresse__code_postal                                                  AS "code_postal",
        structures.adresse__ligne_adresse                                                AS "adresse",
        structures.adresse__latitude                                                     AS "latitude",
        structures.adresse__longitude                                                    AS "longitude",
        COALESCE(communes.nom, communes_associees_deleguees.nom, arrondissements.nom)    AS "commune",
        COALESCE(communes.code, communes_associees_deleguees.code, arrondissements.code) AS "code_insee",
        NULL                                                                             AS "complement_adresse"
    FROM structures
    LEFT JOIN communes ON structures.ville__code_commune = communes.code
    LEFT JOIN arrondissements ON structures.ville__code_commune = arrondissements.code
    LEFT JOIN communes_associees_deleguees ON structures.ville__code_commune = communes_associees_deleguees.code
)

SELECT * FROM final
