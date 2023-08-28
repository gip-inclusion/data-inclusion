WITH structures AS (
    SELECT * FROM {{ ref('int__union_structures') }}
),

sirene_etablissement_historique AS (
    SELECT *
    FROM {{ source('data_inclusion', 'sirene_etablissement_historique') }}
),

sirene_etablissement_succession AS (
    SELECT *
    FROM {{ source('data_inclusion', 'sirene_etablissement_succession') }}
),

structures_with_states_updates AS (
    SELECT
        structures._di_surrogate_id,
        structures.siret,
        sirene_etablissement_historique."dateDebut"                      AS "sirene_date_debut",
        sirene_etablissement_historique."etatAdministratifEtablissement" AS "sirene_etat_admin_etablissement"
    FROM
        structures
    INNER JOIN
        sirene_etablissement_historique ON
        structures.siret = sirene_etablissement_historique.siret
    WHERE
        sirene_etablissement_historique."changementEtatAdministratifEtablissement"
),

latest_debut_date_by_siret AS (
    SELECT
        _di_surrogate_id,
        MAX(sirene_date_debut) AS closure_date
    FROM structures_with_states_updates
    GROUP BY _di_surrogate_id
),

structures_with_latest_state AS (
    SELECT
        structures_with_states_updates._di_surrogate_id,
        structures_with_states_updates.siret,
        structures_with_states_updates.sirene_date_debut AS "sirene_date_fermeture",
        structures_with_states_updates.sirene_etat_admin_etablissement
    FROM structures_with_states_updates
    INNER JOIN
        latest_debut_date_by_siret ON
        structures_with_states_updates._di_surrogate_id = latest_debut_date_by_siret._di_surrogate_id
    WHERE
        structures_with_states_updates.sirene_date_debut = latest_debut_date_by_siret.closure_date
),

structures_with_deprecated_siret AS (
    SELECT *
    FROM structures_with_latest_state
    WHERE sirene_etat_admin_etablissement = 'F'
),

latest_succession_by_siret AS (
    SELECT DISTINCT ON ("siretEtablissementPredecesseur") *
    FROM sirene_etablissement_succession
    ORDER BY
        "siretEtablissementPredecesseur" ASC,
        "dateLienSuccession" DESC
),

final AS (
    SELECT
        structures_with_deprecated_siret.*,
        latest_succession_by_siret."siretEtablissementSuccesseur" AS sirene_etab_successeur
    FROM
        structures_with_deprecated_siret
    LEFT JOIN
        latest_succession_by_siret ON structures_with_deprecated_siret.siret = latest_succession_by_siret."siretEtablissementPredecesseur"
)

SELECT * FROM final
