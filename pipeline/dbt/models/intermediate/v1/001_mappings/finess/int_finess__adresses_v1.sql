WITH etablissements AS (
    SELECT * FROM {{ ref('stg_finess__etablissements') }}
),

map_types_voies AS (
    SELECT
        type_voie,
        libelle_type_voie
    FROM (
        VALUES
        -- noqa: disable=layout.spacing
        ('R',    'rue'),
        ('AV',   'avenue'),
        ('BD',   'boulevard'),
        ('PL',   'place'),
        ('RTE',  'route'),
        ('CHE',  'chemin'),
        ('CHEM', 'chemin'),
        ('ALL',  'allée'),
        ('IMP',  'impasse'),
        ('CRS',  'cours'),
        ('QU',   'quai'),
        ('QUA',  'quartier'),
        ('SQ',   'square'),
        ('PAS',  'passage'),
        ('VOI',  'voie'),
        ('LD',   'lieu-dit'),
        ('RPT',  'rond-point'),
        ('PROM', 'promenade'),
        ('ESP',  'esplanade'),
        ('MTE',  'montée'),
        ('RLE',  'ruelle'),
        ('PTE',  'porte'),
        ('FG',   'faubourg'),
        ('HAM',  'hameau'),
        ('TRA',  'traverse')
    -- noqa: enable=layout.spacing
    ) AS x (type_voie, libelle_type_voie)
),

final AS (
    SELECT
        'finess'                                                                             AS "source",
        'finess--' || etablissements.numero_finess_et                                        AS "id",
        CAST(NULL AS FLOAT)                                                                  AS "longitude",
        CAST(NULL AS FLOAT)                                                                  AS "latitude",
        NULLIF(CONCAT_WS(
            ' ',
            etablissements.lieu_dit_bp,
            etablissements.complement_distribution
        ), '')                                                                               AS "complement_adresse",
        TRIM(
            SUBSTRING(
                etablissements.ligne_acheminement FROM '\d{5} (.*?)(?= CEDEX|$)'
            )
        )                                                                                    AS "commune",
        LEFT(etablissements.ligne_acheminement, 5)                                           AS "code_postal",
        -- cf: https://www.atih.sante.fr/constitution-codes-geographiques
        REGEXP_REPLACE(etablissements.departement, '9[A-F]', '97') || etablissements.commune AS "code_insee",
        NULLIF(CONCAT_WS(
            ' ',
            etablissements.numero_voie,
            etablissements.complement_voie,
            COALESCE(map_types_voies.libelle_type_voie, etablissements.type_voie),
            COALESCE(etablissements.libelle_voie, etablissements.lieu_dit_bp)
        ), '')                                                                               AS "adresse"
    FROM etablissements
    LEFT JOIN map_types_voies ON etablissements.type_voie = map_types_voies.type_voie
)

SELECT * FROM final
