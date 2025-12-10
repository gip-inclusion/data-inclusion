WITH aides AS (
    SELECT * FROM {{ ref('stg_mes_aides__aides') }}
),

telephones AS (
    SELECT DISTINCT ON (aide_id)
        aide_id,
        value
    FROM {{ ref('stg_mes_aides__aides__telephones') }}
    ORDER BY aide_id ASC, index ASC
),

emails AS (
    SELECT DISTINCT ON (aide_id)
        aide_id,
        value
    FROM {{ ref('stg_mes_aides__aides__emails') }}
    ORDER BY aide_id ASC, index ASC
),

final AS (
    SELECT
        'mes-aides'                                       AS "source",
        'mes-aides--' || aides.id                         AS "id",
        NULL                                              AS "adresse_id",
        aides.organisme                                   AS "nom",
        COALESCE(aides.mise_a_jour_le, aides.modifiee_le) AS "date_maj",
        telephones.value                                  AS "telephone",
        emails.value                                      AS "courriel",
        SUBSTRING(aides.site FROM '^(?:.+://)?[^/]+')     AS "site_web",
        NULL                                              AS "siret",
        aides.voir_l_aide                                 AS "lien_source",
        NULL                                              AS "description",
        CAST(NULL AS TEXT [])                             AS "reseaux_porteurs",
        NULL                                              AS "horaires_accueil",
        NULL                                              AS "accessibilite_lieu"
    FROM aides
    LEFT JOIN telephones ON aides.id = telephones.aide_id
    LEFT JOIN emails ON aides.id = emails.aide_id
    WHERE aides.en_ligne
)

SELECT * FROM final
