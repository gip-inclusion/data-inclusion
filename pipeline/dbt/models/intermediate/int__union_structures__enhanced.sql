WITH structures AS (
    SELECT * FROM {{ ref('int__union_structures') }}
),

adresses AS (
    SELECT * FROM {{ ref('int__union_adresses__enhanced') }}
),

valid_site_web AS (
    SELECT
        input_url,
        "url"
    FROM {{ ref('int__union_urls__enhanced') }}
    WHERE status_code > 0
),

structures_to_validate AS (
    SELECT
        {{
            dbt_utils.star(
                from=ref('int__union_structures'),
                relation_alias='structures',
                except=[
                    "nom",
                    "telephone",
                    "site_web",
                ]
            )
        }},
        processings.format_phone_number(structures.telephone) AS "telephone",
        valid_site_web.url                                    AS "site_web",
        CASE
            WHEN LENGTH(structures.nom) <= 150 THEN structures.nom
            ELSE LEFT(structures.nom, 149) || '…'
        END                                                   AS "nom"
    FROM structures
    LEFT JOIN valid_site_web ON structures.site_web = valid_site_web.input_url
),

valid_structures AS (
    SELECT structures.*
    FROM structures_to_validate AS structures
    LEFT JOIN
        LATERAL
        LIST_STRUCTURE_ERRORS(
            structures.accessibilite,
            structures.antenne,
            structures.courriel,
            structures.date_maj,
            structures.horaires_ouverture,
            structures.id,
            structures.labels_autres,
            structures.labels_nationaux,
            structures.lien_source,
            structures.nom,
            structures.presentation_detail,
            structures.presentation_resume,
            structures.rna,
            structures.siret,
            structures.site_web,
            structures.source,
            structures.telephone,
            structures.thematiques,
            structures.typologie
        ) AS errors ON TRUE
    WHERE errors.field IS NULL
),

final AS (
    SELECT
        structures.*,
        adresses.longitude          AS "longitude",
        adresses.latitude           AS "latitude",
        adresses.complement_adresse AS "complement_adresse",
        adresses.commune            AS "commune",
        adresses.adresse            AS "adresse",
        adresses.code_postal        AS "code_postal",
        adresses.code_insee         AS "code_insee"
    FROM valid_structures AS structures
    LEFT JOIN adresses ON structures._di_adresse_surrogate_id = adresses._di_surrogate_id
)

SELECT * FROM final
