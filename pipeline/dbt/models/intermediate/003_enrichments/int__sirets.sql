WITH RECURSIVE structures AS (
    SELECT * FROM {{ ref('int__union_structures') }}
),

etablissements AS (
    SELECT * FROM {{ ref('stg_sirene__etablissement_historique') }}
),

successions AS (
    SELECT * FROM {{ ref('stg_sirene__etablissement_succession') }}
),

filtered_etablissements AS (
    SELECT DISTINCT ON (1)
        etablissements.siret,
        etablissements.date_debut,
        etablissements.etat_administratif_etablissement
    FROM etablissements
    INNER JOIN structures ON etablissements.siret = structures.siret
    ORDER BY 1, 2 DESC
),

filtered_successions AS (
    -- start with the set of sirets in the data
    SELECT
        structures.siret,
        CAST(NULL AS DATE) AS "date_lien_succession",
        NULL               AS "siret_etablissement_successeur"
    FROM structures
    UNION
    -- find successors recursively
    (
        SELECT DISTINCT ON (1)
            filtered_successions.siret,
            successions.date_lien_succession,
            successions.siret_etablissement_successeur
        FROM filtered_successions
        INNER JOIN successions
            ON
                COALESCE(filtered_successions.siret_etablissement_successeur, filtered_successions.siret) = successions.siret_etablissement_predecesseur
                AND successions.date_lien_succession > COALESCE(filtered_successions.date_lien_succession, '2000-01-01')
        ORDER BY 1, 2 DESC
    )
-- detect cycles and prevent infinite loop
) CYCLE siret_etablissement_successeur SET cycle USING chemin,  -- noqa: layout.cte_newline

last_successions AS (
    SELECT DISTINCT ON (1)
        filtered_successions.siret,
        filtered_successions.date_lien_succession,
        filtered_successions.siret_etablissement_successeur
    FROM filtered_successions
    ORDER BY 1, 2 DESC NULLS LAST
)

SELECT
    structures._di_surrogate_id,
    structures.siret,
    CASE
        WHEN
            filtered_etablissements.etat_administratif_etablissement = 'fermé'
            AND last_successions.siret_etablissement_successeur IS NULL
            THEN filtered_etablissements.date_debut
        WHEN
            filtered_etablissements.etat_administratif_etablissement = 'fermé'
            AND last_successions.siret_etablissement_successeur IS NOT NULL
            AND etablissements_successeur.etat_administratif_etablissement = 'fermé'
            THEN etablissements_successeur.date_debut
    END AS "date_fermeture",
    CASE
        WHEN (
            filtered_etablissements.etat_administratif_etablissement = 'fermé'
            AND last_successions.siret_etablissement_successeur IS NULL
        )
        OR (
            filtered_etablissements.etat_administratif_etablissement = 'fermé'
            AND last_successions.siret_etablissement_successeur IS NOT NULL
            AND etablissements_successeur.etat_administratif_etablissement = 'fermé'
        )
            THEN 'fermé-définitivement'
        WHEN
            filtered_etablissements.etat_administratif_etablissement = 'fermé'
            AND last_successions.siret_etablissement_successeur IS NOT NULL
            AND etablissements_successeur.etat_administratif_etablissement = 'actif'
            THEN 'successeur-ouvert'
        WHEN filtered_etablissements.siret IS NULL THEN 'inconnu'
        ELSE 'valide'
    END AS "statut",
    CASE
        WHEN (
            filtered_etablissements.etat_administratif_etablissement = 'fermé'
            AND last_successions.siret_etablissement_successeur IS NOT NULL
            AND etablissements_successeur.etat_administratif_etablissement = 'actif'
        )
        OR (
            filtered_etablissements.etat_administratif_etablissement = 'fermé'
            AND last_successions.siret_etablissement_successeur IS NOT NULL
            AND etablissements_successeur.etat_administratif_etablissement = 'fermé'
        )
            THEN last_successions.siret_etablissement_successeur
    END AS "siret_successeur"
FROM structures
LEFT JOIN filtered_etablissements ON structures.siret = filtered_etablissements.siret
LEFT JOIN last_successions ON structures.siret = last_successions.siret
LEFT JOIN filtered_etablissements AS etablissements_successeur ON last_successions.siret_etablissement_successeur = etablissements_successeur.siret
WHERE structures.siret IS NOT NULL
