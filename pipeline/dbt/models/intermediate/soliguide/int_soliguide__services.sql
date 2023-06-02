WITH lieux AS (
    SELECT * FROM {{ ref('stg_soliguide__lieux') }}
),

services AS (
    SELECT * FROM {{ ref('stg_soliguide__services') }}
),

categories AS (
    SELECT * FROM {{ ref('stg_soliguide__categories') }}
),

di_thematique_by_soliguide_categorie_code AS (
    SELECT x.*
    FROM (
        VALUES
        -- Mapping: https://grist.incubateur.anct.gouv.fr/o/datainclusion/tAh1eF3DeE3D/Mappings/p/1
        ('1200', 'mobilite'),
        ('1201', 'mobilite'),
        ('1202', 'mobilite--louer-un-vehicule'),
        ('1203', 'mobilite--comprendre-et-utiliser-les-transports-en-commun'),
        ('1204', 'mobilite--aides-a-la-reprise-demploi-ou-a-la-formation'),
        ('501', 'numerique--acceder-a-du-materiel'),
        ('502', 'numerique--acceder-a-une-connexion-internet'),
        ('504', 'equipement-et-alimentation--acces-a-un-telephone-et-un-abonnement')
    ) AS x (categorie, thematique)
),

final AS (
    SELECT
        services.id                                              AS "id",
        services._di_source_id                                   AS "source",
        NULL::TEXT []                                            AS "types",
        NULL                                                     AS "prise_rdv",
        NULL::TEXT []                                            AS "frais",
        NULL                                                     AS "frais_autres",
        NULL::TEXT []                                            AS "profils",
        NULL                                                     AS "pre_requis",
        NULL                                                     AS "cumulable",
        NULL                                                     AS "justificatifs",
        NULL                                                     AS "longitude",
        NULL                                                     AS "latitude",
        NULL                                                     AS "date_creation",
        NULL                                                     AS "date_suspension",
        NULL                                                     AS "telephone",
        NULL                                                     AS "courriel",
        NULL                                                     AS "contact_public",
        services.updated_at                                      AS "date_maj",
        NULL                                                     AS "zone_diffusion_type",
        NULL                                                     AS "zone_diffusion_code",
        NULL                                                     AS "zone_diffusion_nom",
        NULL                                                     AS "formulaire_en_ligne",
        lieux.position_ville                                     AS "commune",
        lieux.position_code_postal                               AS "code_postal",
        lieux.position_code_postal                               AS "code_insee",
        lieux.position_adresse                                   AS "adresse",
        lieux.position_complement_adresse                        AS "complement_adresse",
        NULL                                                     AS "recurrence",
        services.lieu_id                                         AS "structure_id",
        categories.label || COALESCE(' : ' || services.name, '') AS "nom",
        'https://soliguide.fr/fr/fiche/' || lieux.seo_url        AS "lien_source",
        ARRAY(
            SELECT di_thematique_by_soliguide_categorie_code.thematique
            FROM di_thematique_by_soliguide_categorie_code
            WHERE services.categorie = di_thematique_by_soliguide_categorie_code.categorie
        )::TEXT []                                               AS "thematiques",
        NULL::TEXT []                                            AS "modes_accueil",
        CASE LENGTH(services.description) <= 280
            WHEN TRUE THEN services.description
            WHEN FALSE THEN LEFT(services.description, 279) || '…'
        END                                                      AS "presentation_resume",
        CASE LENGTH(services.description) <= 280
            WHEN TRUE THEN NULL
            WHEN FALSE THEN services.description
        END                                                      AS "presentation_detail"
    FROM services LEFT JOIN lieux ON services.lieu_id = lieux.id
    LEFT JOIN categories ON services.categorie = categories.code
    ORDER BY 1
)

SELECT * FROM final
