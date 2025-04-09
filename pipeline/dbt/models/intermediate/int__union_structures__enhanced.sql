WITH structures AS (
    SELECT * FROM {{ ref('int__union_structures') }}
),

adresses AS (
    SELECT * FROM {{ ref('int__union_adresses__enhanced') }}
),

valid_structures AS (
    SELECT
        {{
            dbt_utils.star(
                from=ref('int__union_structures'),
                relation_alias='structures',
                except=[
                    "nom",
                    "telephone",
                ]
            )
        }},
        processings.format_phone_number(structures.telephone) AS "telephone",
        CASE
            WHEN LENGTH(structures.nom) <= 150 THEN structures.nom
            ELSE LEFT(structures.nom, 149) || '…'
        END AS "nom"
    FROM structures
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
        valid_structures.*,
        adresses.longitude          AS "longitude",
        adresses.latitude           AS "latitude",
        adresses.complement_adresse AS "complement_adresse",
        adresses.commune            AS "commune",
        adresses.adresse            AS "adresse",
        adresses.code_postal        AS "code_postal",
        adresses.code_insee         AS "code_insee"
    FROM
        valid_structures
    LEFT JOIN adresses ON valid_structures._di_adresse_surrogate_id = adresses._di_surrogate_id
)

SELECT * FROM final
