WITH structures AS (
    SELECT * FROM {{ ref('int__enhanced_structures') }}
),

typologies_de_structures AS (
    SELECT * FROM {{ ref('typologies_de_structures') }}
),

labels_nationaux AS (
    SELECT * FROM {{ ref('labels_nationaux') }}
),

thematiques AS (
    SELECT * FROM {{ ref('thematiques') }}
),

null_id AS (
    SELECT
        _di_surrogate_id AS "_di_surrogate_id",
        source           AS "_di_source_id",
        id               AS "id",
        'id'             AS "field",
        id::TEXT         AS "value",
        'id_est_nul'     AS "type"
    FROM structures
    WHERE id IS NULL
),

malformed_siret AS (
    SELECT
        _di_surrogate_id AS "_di_surrogate_id",
        source           AS "_di_source_id",
        id               AS "id",
        'siret'          AS "field",
        siret::TEXT      AS "value",
        'siret_malformé' AS "type"
    FROM structures
    WHERE siret IS NOT NULL AND NOT siret ~ '^\d{14}$'
),

malformed_rna AS (
    SELECT
        _di_surrogate_id AS "_di_surrogate_id",
        source           AS "_di_source_id",
        id               AS "id",
        'rna'            AS "field",
        rna::TEXT        AS "value",
        'rna_malformé'   AS "type"
    FROM structures
    WHERE rna IS NOT NULL AND NOT rna ~* '^W\d{9}$'
),

null_nom AS (
    SELECT
        _di_surrogate_id AS "_di_surrogate_id",
        source           AS "_di_source_id",
        id               AS "id",
        'nom'            AS "field",
        nom::TEXT        AS "value",
        'nom_est_nul'    AS "type"
    FROM structures
    WHERE nom IS NULL
),

null_commune AS (
    SELECT
        _di_surrogate_id    AS "_di_surrogate_id",
        source              AS "_di_source_id",
        id                  AS "id",
        'commune'           AS "field",
        commune::TEXT       AS "value",
        'commune_est_nulle' AS "type"
    FROM structures
    WHERE commune IS NULL
),

null_or_malformed_code_postal AS (
    SELECT
        _di_surrogate_id              AS "_di_surrogate_id",
        source                        AS "_di_source_id",
        id                            AS "id",
        'code_postal'                 AS "field",
        code_postal::TEXT             AS "value",
        'code_postal_nul_ou_malformé' AS "type"
    FROM structures
    WHERE code_postal IS NULL OR NOT code_postal ~ '^\d{5}$'
),

malformed_code_insee AS (
    SELECT
        _di_surrogate_id      AS "_di_surrogate_id",
        source                AS "_di_source_id",
        id                    AS "id",
        'code_insee'          AS "field",
        code_insee::TEXT      AS "value",
        'code_insee_malformé' AS "type"
    FROM structures
    WHERE code_insee IS NOT NULL AND NOT code_insee ~ '^.{5}$'
),

null_adresse AS (
    SELECT
        _di_surrogate_id    AS "_di_surrogate_id",
        source              AS "_di_source_id",
        id                  AS "id",
        'adresse'           AS "field",
        adresse::TEXT       AS "value",
        'adresse_est_nulle' AS "type"
    FROM structures
    WHERE adresse IS NULL
),

null_date_maj AS (
    SELECT
        _di_surrogate_id     AS "_di_surrogate_id",
        source               AS "_di_source_id",
        id                   AS "id",
        'date_maj'           AS "field",
        date_maj::TEXT       AS "value",
        'date_maj_est_nulle' AS "type"
    FROM structures
    WHERE date_maj IS NULL
),

invalid_typologie AS (
    SELECT
        _di_surrogate_id     AS "_di_surrogate_id",
        source               AS "_di_source_id",
        id                   AS "id",
        'typologie'          AS "field",
        typologie::TEXT      AS "value",
        'typologie_invalide' AS "type"
    FROM structures
    WHERE typologie IS NOT NULL AND typologie NOT IN (SELECT value FROM typologies_de_structures)
),

invalid_labels_nationaux AS (
    SELECT
        _di_surrogate_id             AS "_di_surrogate_id",
        source                       AS "_di_source_id",
        id                           AS "id",
        'labels_nationaux'           AS "field",
        labels_nationaux::TEXT       AS "value",
        'labels_nationaux_invalides' AS "type"
    FROM structures
    WHERE labels_nationaux IS NOT NULL
        AND NOT labels_nationaux <@ ARRAY(SELECT value FROM labels_nationaux)
),

invalid_thematiques AS (
    SELECT
        _di_surrogate_id        AS "_di_surrogate_id",
        source                  AS "_di_source_id",
        id                      AS "id",
        'thematiques'           AS "field",
        thematiques::TEXT       AS "value",
        'thematiques_invalides' AS "type"
    FROM structures
    WHERE thematiques IS NOT NULL AND NOT thematiques <@ ARRAY(SELECT value FROM thematiques)
),


too_long_presentation_resume AS (
    SELECT
        _di_surrogate_id                  AS "_di_surrogate_id",
        source                            AS "_di_source_id",
        id                                AS "id",
        'presentation_resume'             AS "field",
        presentation_resume::TEXT         AS "value",
        'presentation_resume_trop_longue' AS "type"
    FROM structures
    WHERE presentation_resume IS NOT NULL AND LENGTH(presentation_resume) > 280
),

invalid_courriel AS (
    SELECT
        _di_surrogate_id    AS "_di_surrogate_id",
        source              AS "_di_source_id",
        id                  AS "id",
        'courriel'          AS "field",
        courriel::TEXT      AS "value",
        'courriel_invalide' AS "type"
    FROM structures
    WHERE courriel IS NOT NULL AND NOT courriel ~ '^[a-zA-Z0-9.!#$%&''*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$'  -- noqa: L016
),

final AS (
    SELECT * FROM null_id
    UNION ALL
    SELECT * FROM malformed_siret
    UNION ALL
    SELECT * FROM malformed_rna
    UNION ALL
    SELECT * FROM null_nom
    UNION ALL
    SELECT * FROM null_commune
    UNION ALL
    SELECT * FROM null_or_malformed_code_postal
    UNION ALL
    SELECT * FROM malformed_code_insee
    UNION ALL
    SELECT * FROM null_adresse
    UNION ALL
    SELECT * FROM null_date_maj
    UNION ALL
    SELECT * FROM invalid_typologie
    UNION ALL
    SELECT * FROM invalid_labels_nationaux
    UNION ALL
    SELECT * FROM invalid_thematiques
    UNION ALL
    SELECT * FROM too_long_presentation_resume
    UNION ALL
    SELECT * FROM invalid_courriel
)

SELECT * FROM final
