BEGIN;

-- 1. Clean up previous runs (for idempotency)
UPDATE datawarehouse
SET
    data_normalized = JSONB_SET(
        data_normalized,
        '{sirene_date_fermeture}',
        'null'::JSONB
    )
WHERE
    batch_id = '{{ run_id }}'
    AND src_url = '{{ params.src_url }}'
    AND data ? 'siret';


-- 2. First join with sirene_etablissement_historique to detect closed
-- structures
WITH dwh_sirets AS (
    SELECT
        data ->> 'id' AS id,
        data ->> 'siret' AS siret
    FROM
        datawarehouse
    WHERE
        src_url = '{{ params.src_url }}'
        AND batch_id = '{{ run_id }}'
),

with_sirene_etat_updates AS (
    SELECT
        dwh_sirets.id,
        dwh_sirets.siret,
        sirene_etablissement_historique."dateDebut" AS sirene_date_debut,
        sirene_etablissement_historique."etatAdministratifEtablissement"
        AS sirene_etat_admin_etablissement -- noqa: L016
    FROM
        dwh_sirets
    INNER JOIN
        sirene_etablissement_historique ON
            dwh_sirets.siret = sirene_etablissement_historique.siret
    WHERE
        sirene_etablissement_historique."changementEtatAdministratifEtablissement" -- noqa: L016
),

latest_debut_date_by_siret AS (
    SELECT
        id,
        siret,
        MAX(sirene_date_debut) AS closure_date
    FROM
        with_sirene_etat_updates
    GROUP BY
        id,
        siret
),

sirets_latest_etat AS (
    SELECT
        with_sirene_etat_updates.id,
        with_sirene_etat_updates.siret,
        with_sirene_etat_updates.sirene_date_debut,
        with_sirene_etat_updates.sirene_etat_admin_etablissement
    FROM
        with_sirene_etat_updates
    INNER JOIN
        latest_debut_date_by_siret ON
            with_sirene_etat_updates.siret = latest_debut_date_by_siret.siret
            AND with_sirene_etat_updates.id = latest_debut_date_by_siret.id
    WHERE
        with_sirene_etat_updates.sirene_date_debut
        = latest_debut_date_by_siret.closure_date
),

-- 3. Second join with sirene_etablissement_succession to find eventual
-- successors
closed_structures AS (
    SELECT *
    FROM
        sirets_latest_etat
    WHERE
        sirene_etat_admin_etablissement = 'F'
),

closed_structures_with_successor AS (
    SELECT
        closed_structures.*,
        sirene_etablissement_succession."siretEtablissementSuccesseur"
        AS sirene_etab_successeur
    FROM
        closed_structures
    LEFT JOIN
        sirene_etablissement_succession ON
            closed_structures.siret
            = sirene_etablissement_succession."siretEtablissementPredecesseur"
)

-- 4. Annotate with results
UPDATE
datawarehouse
SET
    data_normalized
    = data_normalized
    || JSONB_BUILD_OBJECT(
        'sirene_date_fermeture',
        closed_structures_with_successor.sirene_date_debut
    )
    || JSONB_BUILD_OBJECT(
        'sirene_etab_successeur',
        closed_structures_with_successor.sirene_etab_successeur
    )
FROM
    closed_structures_with_successor
WHERE
    datawarehouse.batch_id = '{{ run_id }}'
    AND datawarehouse.src_url = '{{ params.src_url }}'
    AND datawarehouse.data ? 'siret'
    AND datawarehouse.data ->> 'id' = closed_structures_with_successor.id;

COMMIT;
