WITH structures AS (
    SELECT * FROM {{ ref('stg_emplois_de_linclusion__siaes') }}
    UNION
    SELECT * FROM {{ ref('stg_emplois_de_linclusion__organisations') }}
),

adresses AS (
    SELECT * FROM {{ ref('int_emplois_de_linclusion__adresses_v1') }}
),

reseaux_porteurs_mapping AS (
    SELECT * FROM {{ ref('_map_emplois__reseaux_porteurs_v1') }}
),

final AS (
    SELECT
        'emplois-de-linclusion--' || structures.id                                         AS "id",
        adresses.id                                                                        AS "adresse_id",
        'emplois-de-linclusion'                                                            AS "source",
        structures.nom                                                                     AS "nom",
        structures.date_maj                                                                AS "date_maj",
        structures.telephone                                                               AS "telephone",
        structures.courriel                                                                AS "courriel",
        structures.site_web                                                                AS "site_web",
        structures.siret                                                                   AS "siret",
        structures.lien_source                                                             AS "lien_source",
        NULLIF(
            -- remove descriptions that are too short to be meaningful
            REGEXP_REPLACE(structures.description, '^.{1,5}$', ''),
            ''
        )                                                                                  AS "description",
        NULLIF(ARRAY_REMOVE(ARRAY[reseaux_porteurs_mapping.reseaux_porteurs], NULL), '{}') AS "reseaux_porteurs",
        NULL                                                                               AS "horaires_accueil",
        NULL                                                                               AS "accessibilite_lieu"
    FROM structures
    LEFT JOIN adresses ON ('emplois-de-linclusion--' || structures.id) = adresses.id
    LEFT JOIN reseaux_porteurs_mapping
        ON structures.kind = reseaux_porteurs_mapping.kind
)

SELECT * FROM final
