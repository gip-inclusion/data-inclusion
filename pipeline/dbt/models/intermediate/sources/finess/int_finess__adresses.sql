WITH etablissements AS (
    SELECT * FROM {{ ref('stg_finess__etablissements') }}
),

-- this subset of categories
-- cf https://finess.sante.gouv.fr/fininter/jsp/pdf.do?xsl=CategEta.xsl
interesting_etablissement_categories AS (
    SELECT x.*
    FROM (
        VALUES
        -- Etab. et Services d'Hébergement pour Adultes Handicapés
        ('252'), ('253'), ('255'), ('370'), ('382'), ('395'), ('437'), ('448'), ('449'),
        -- Etab.et Services de Travail Protégé pour Adultes Handicapés
        ('246'), ('247'),
        -- Etab.et Services de Réinsertion Prof pour Adultes Handicapés
        ('198'), ('249'),
        -- Etablissements de l'Aide Sociale à l'Enfance
        ('159'), ('166'), ('172'), ('175'), ('176'), ('177'), ('236'), ('411'),
        -- Etablissements pour Adultes et Familles en Difficulté
        ('214'), ('216'), ('219'), ('442'), ('443'),
        -- Autres Etablissements Sociaux d'Hébergement et d'Accueil
        ('256'), ('257'), ('271'),
        -- Logements en Structure Collective
        ('258'), ('259'),
        -- Centre Planification ou Education Familiale
        ('228'),
        -- Centre de soins et de prévention
        ('636')
    ) AS x (categetab)
),

final AS (
    SELECT
        etablissements.nofinesset                                                            AS "id",
        etablissements.compvoie                                                              AS "complement_adresse",
        CAST(NULL AS FLOAT)                                                                  AS "longitude",
        CAST(NULL AS FLOAT)                                                                  AS "latitude",
        etablissements._di_source_id                                                         AS "source",
        TRIM(SUBSTRING(etablissements.ligneacheminement FROM '\d{5} (.*?)(?= CEDEX|$)'))     AS "commune",
        LEFT(etablissements.ligneacheminement, 5)                                            AS "code_postal",
        -- cf: https://www.atih.sante.fr/constitution-codes-geographiques
        REGEXP_REPLACE(etablissements.departement, '9[A-F]', '97') || etablissements.commune AS "code_insee",
        etablissements.compldistrib
        || ' ' || etablissements.numvoie
        || ' ' || etablissements.typvoie
        || ' ' || etablissements.voie
        || ' ' || etablissements.lieuditbp                                                   AS "adresse"
    FROM etablissements
    WHERE etablissements.categetab IN (
        SELECT interesting_etablissement_categories.categetab
        FROM interesting_etablissement_categories
    )
)

SELECT * FROM final
