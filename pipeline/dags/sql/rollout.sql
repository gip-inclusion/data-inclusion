/*
 * Copy valid structures and services data to the api's production tables.
 */

-- 1. Empty production tables
DELETE FROM
service;

DELETE FROM
structure;

-- 2. Insert structures from the validated data in staging
WITH valid_staging_structures_flat AS (
    SELECT
        structures.*,
        datawarehouse.batch_id,
        datawarehouse.created_at,
        datawarehouse.src_url,
        datawarehouse.data_normalized ->> 'code_insee' AS normalized_code_insee,
        datawarehouse.data_normalized ->> 'siret' AS normalized_siret
    FROM
        datawarehouse,
        JSONB_TO_RECORD(datawarehouse.data) AS structures(
            id TEXT,
            siret TEXT,
            rna TEXT,
            nom TEXT,
            commune TEXT,
            code_postal TEXT,
            code_insee TEXT,
            adresse TEXT,
            complement_adresse TEXT,
            longitude DOUBLE PRECISION,
            latitude DOUBLE PRECISION,
            typologie TEXT,
            telephone TEXT,
            courriel TEXT,
            site_web TEXT,
            presentation_resume TEXT,
            presentation_detail TEXT,
            source TEXT,
            date_maj TIMESTAMP WITH TIME ZONE,
            antenne BOOL,
            lien_source TEXT,
            horaires_ouverture TEXT,
            accessibilite TEXT,
            labels_nationaux TEXT [],
            labels_autres TEXT [],
            thematiques TEXT []
        )
    WHERE
        (datawarehouse.data_normalized ->> 'is_valid')::BOOL
        AND datawarehouse.data ? 'siret'
        -- exclude soliguide data from rollout
        AND datawarehouse.data ->> 'source' != 'soliguide'
        AND datawarehouse.logical_date = '{{ dag_run.conf.logical_date }}'
),

staging_structures_final AS (
    SELECT
        id,
        rna,
        nom,
        commune,
        code_postal,
        adresse,
        complement_adresse,
        longitude,
        latitude,
        typologie,
        telephone,
        courriel,
        site_web,
        presentation_resume,
        presentation_detail,
        source,
        date_maj,
        antenne,
        lien_source,
        horaires_ouverture,
        accessibilite,
        batch_id,
        created_at,
        src_url,
        COALESCE(siret, normalized_siret) AS "siret",
        COALESCE(code_insee, normalized_code_insee) AS "code_insee",
        COALESCE(labels_nationaux, ARRAY []::TEXT []) AS "labels_nationaux",
        COALESCE(labels_autres, ARRAY []::TEXT []) AS "labels_autres",
        COALESCE(thematiques, ARRAY []::TEXT []) AS "thematiques"
    FROM
        valid_staging_structures_flat
)

INSERT INTO
structure (
    id,
    rna,
    nom,
    commune,
    code_postal,
    adresse,
    complement_adresse,
    longitude,
    latitude,
    typologie,
    telephone,
    courriel,
    site_web,
    presentation_resume,
    presentation_detail,
    source,
    date_maj,
    antenne,
    lien_source,
    horaires_ouverture,
    accessibilite,
    batch_id,
    created_at,
    src_url,
    siret,
    code_insee,
    labels_nationaux,
    labels_autres,
    thematiques
) OVERRIDING USER VALUE
SELECT *
FROM
    staging_structures_final ON CONFLICT DO NOTHING;

-- 3. Insert services from the validated data in staging
WITH valid_staging_services_with_structure_index AS (
    SELECT
        datawarehouse.*,
        structure.index
    FROM
        structure
    LEFT JOIN datawarehouse ON structure.batch_id = datawarehouse.batch_id
            AND structure.src_url = datawarehouse.src_url
            AND structure.id = (datawarehouse.data ->> 'structure_id')
    WHERE
        (datawarehouse.data_normalized ->> 'is_valid')::BOOL
        AND datawarehouse.data ? 'structure_id'
),

valid_staging_services_flat AS (
    SELECT
        service.*,
        valid_staging_services_with_structure_index.index AS structure_index
    FROM
        valid_staging_services_with_structure_index,
        JSONB_TO_RECORD(
            valid_staging_services_with_structure_index.data
        ) AS service(
            id TEXT,
            structure_id TEXT,
            source TEXT,
            nom TEXT,
            presentation_resume TEXT,
            types TEXT [],
            thematiques TEXT [],
            prise_rdv TEXT,
            frais TEXT [],
            frais_autres TEXT,
            profils TEXT []
        )
),

staging_services_final AS (
    SELECT
        structure_index,
        id,
        nom,
        presentation_resume,
        prise_rdv,
        frais_autres,
        COALESCE(types, ARRAY []::TEXT []) AS "types",
        COALESCE(thematiques, ARRAY []::TEXT []) AS "thematiques",
        COALESCE(frais, ARRAY []::TEXT []) AS "frais",
        COALESCE(profils, ARRAY []::TEXT []) AS "profils"
    FROM
        valid_staging_services_flat
)

INSERT INTO
service (
    structure_index,
    id,
    nom,
    presentation_resume,
    prise_rdv,
    frais_autres,
    types,
    thematiques,
    frais,
    profils
) OVERRIDING USER VALUE
SELECT *
FROM
    staging_services_final ON CONFLICT DO NOTHING;
