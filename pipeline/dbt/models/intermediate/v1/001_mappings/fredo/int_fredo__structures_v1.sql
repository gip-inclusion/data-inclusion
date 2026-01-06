WITH structures AS (
    SELECT * FROM {{ ref('stg_fredo__structures') }}
),

telephones AS (
    SELECT DISTINCT ON (structure_id) *
    FROM {{ ref('stg_fredo__telephones') }}
    ORDER BY structure_id
),

emails AS (
    SELECT DISTINCT ON (structure_id) *
    FROM {{ ref('stg_fredo__emails') }}
    ORDER BY structure_id
),

reseaux_porteurs AS (
    SELECT
        types_structures.structure_id,
        ARRAY_AGG(DISTINCT mapping.reseau_porteur) AS "reseaux_porteurs"
    FROM {{ ref('stg_fredo__types') }} AS types_structures
    INNER JOIN {{ ref('_map_fredo__reseaux_porteurs') }} AS "mapping"
        ON types_structures.value = mapping.type_structure
    GROUP BY types_structures.structure_id
),

final AS (
    SELECT
        'fredo'                                           AS "source",
        'fredo--' || structures.id                        AS "id",
        'fredo--' || structures.id                        AS "adresse_id",
        structures.nom                                    AS "nom",
        CAST(structures.last_update AS DATE)              AS "date_maj",
        'https://fredo.re/ad/' || SLUGIFY(structures.nom) AS "lien_source",
        structures.siret                                  AS "siret",
        telephones.value                                  AS "telephone",
        emails.value                                      AS "courriel",
        structures.lien_source                            AS "site_web",
        structures.presentation_resume                    AS "description",
        structures.horaires_ouverture                     AS "horaires_accueil",
        NULL                                              AS "accessibilite_lieu",
        reseaux_porteurs.reseaux_porteurs                 AS "reseaux_porteurs"
    FROM structures
    LEFT JOIN telephones ON structures.id = telephones.structure_id
    LEFT JOIN emails ON structures.id = emails.structure_id
    LEFT JOIN reseaux_porteurs ON structures.id = reseaux_porteurs.structure_id
)

SELECT * FROM final
