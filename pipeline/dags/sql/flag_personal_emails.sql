/*
 * Annotate the datawarehouse's structures rows with a flag indicating whether
 * the email could be considered as a personal email.
 *
 * The process is limited to the given `batch_id` and `src_url`.
 *
 * The `has_personal_email` flag is set in the `data_normalized`.
 */
BEGIN;


-- 1. Prep scoped structure data from datawarehouse
CREATE TEMP TABLE filtered_dwh AS
SELECT
    data ->> 'id' AS id,
    data ->> 'courriel' AS courriel,
    LOWER(SPLIT_PART(data ->> 'courriel', '@', 1)) AS courriel_local_part,
    LOWER(SPLIT_PART(data ->> 'courriel', '@', 2)) AS courriel_domain,
    data ->> 'nom' AS nom
FROM
    datawarehouse
WHERE
    -- limit to the given batch
    batch_id = '{{ run_id }}'
    -- limit to the given src_url
    AND src_url = '{{ params.src_url }}'
    -- select structure rows
    AND data ? 'siret'
    -- ignore missing emails
    AND data ->> 'courriel' IS NOT NULL
    -- select emails from providers
    AND SPLIT_PART(data ->> 'courriel', '@', 2) IN (
        'wanadoo.fr',
        'orange.fr',
        'gmail.com',
        'free.fr',
        'laposte.net',
        'yahoo.fr',
        'hotmail.fr',
        'nordnet.fr',
        'sfr.fr',
        'outlook.fr',
        'laposte.fr'
    )
    -- ignore common patterns for non personal emails
    AND NOT SPLIT_PART(data ->> 'courriel', '@', 1) ~ 'mairie|commune|adil';


-- 2. Prep insee's firstname data
CREATE TEMP TABLE filtered_prenoms AS
SELECT DISTINCT LOWER(preusuel) AS prenom
FROM
    external_insee_fichier_prenoms
WHERE
    LENGTH(preusuel) > 2
    AND nombre > 100
    AND preusuel != 'SAINT';


-- 3. Setup indexes to speed up the join
CREATE INDEX ON filtered_dwh (courriel_local_part);
CREATE INDEX ON filtered_dwh (courriel_domain);
CREATE INDEX ON filtered_prenoms (prenom);


-- 4. Detect plausible personal emails
CREATE TEMP TABLE results AS
SELECT filtered_dwh.id
FROM
    filtered_dwh,
    filtered_prenoms
WHERE
    -- email starts with a known firstname
    filtered_dwh.courriel_local_part ~ ('^' || filtered_prenoms.prenom || '\.')
    -- email is not to similar to the structure name
    AND WORD_SIMILARITY(
        filtered_dwh.courriel_local_part, filtered_dwh.nom
    ) < 0.17;


-- 5. Clean up previous runs (for idempotency)
UPDATE datawarehouse
SET
    data_normalized = JSONB_SET(
        data_normalized,
        '{has_personal_email}',
        'false'
    )
WHERE
    batch_id = '{{ run_id }}'
    AND src_url = '{{ params.src_url }}'
    AND data ? 'siret';


-- 6. Annotate with results
UPDATE
datawarehouse
SET
    data_normalized = JSONB_SET(
        data_normalized,
        '{has_personal_email}',
        'true'
    )
FROM
    results
WHERE
    datawarehouse.batch_id = '{{ run_id }}'
    AND datawarehouse.src_url = '{{ params.src_url }}'
    AND datawarehouse.data ? 'siret'
    AND datawarehouse.data ->> 'id' = results.id;


COMMIT;
