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
        datawarehouse.src_alias,
        datawarehouse.data_normalized ->> 'code_insee' AS normalized_code_insee
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
            antenne BOOLEAN,
            lien_source TEXT,
            horaires_ouverture TEXT,
            accessibilite TEXT,
            labels_nationaux TEXT [],
            labels_autres TEXT [],
            thematiques TEXT []
        )
    WHERE
        (datawarehouse.data_normalized ->> 'is_valid')::BOOLEAN
        AND datawarehouse.data ? 'siret'
        -- exclude soliguide data from rollout
        AND datawarehouse.data ->> 'source' NOT IN ('soliguide', 'etab_publics')
        AND datawarehouse.logical_date = '{{ dag_run.conf.logical_date }}'
),

annotations AS (
    SELECT DISTINCT ON (1, 4)
        annotation_dataset.source,
        annotation_annotation.siret,
        annotation_annotation.is_parent,
        annotation_datasetrow.data ->> 'id' AS "id"
    FROM
        annotation_annotation
    INNER JOIN
        annotation_datasetrow ON
            annotation_annotation.row_id = annotation_datasetrow.id
    INNER JOIN
        annotation_dataset ON
            annotation_datasetrow.dataset_id = annotation_dataset.id
    WHERE
        NOT annotation_annotation.closed
        AND NOT annotation_annotation.irrelevant
        AND NOT annotation_annotation.skipped
        AND annotation_dataset.source != ''
    ORDER BY
        annotation_dataset.source ASC,
        annotation_datasetrow.data ->> 'id' ASC,
        annotation_annotation.created_at DESC
),

staging_structures_final AS (
    SELECT
        valid_staging_structures_flat.id,
        valid_staging_structures_flat.rna,
        valid_staging_structures_flat.nom,
        valid_staging_structures_flat.commune,
        valid_staging_structures_flat.code_postal,
        valid_staging_structures_flat.adresse,
        valid_staging_structures_flat.complement_adresse,
        valid_staging_structures_flat.longitude,
        valid_staging_structures_flat.latitude,
        valid_staging_structures_flat.typologie,
        valid_staging_structures_flat.telephone,
        valid_staging_structures_flat.courriel,
        valid_staging_structures_flat.site_web,
        valid_staging_structures_flat.presentation_resume,
        valid_staging_structures_flat.presentation_detail,
        valid_staging_structures_flat.source,
        valid_staging_structures_flat.date_maj,
        valid_staging_structures_flat.lien_source,
        valid_staging_structures_flat.horaires_ouverture,
        valid_staging_structures_flat.accessibilite,
        valid_staging_structures_flat.batch_id,
        valid_staging_structures_flat.created_at,
        valid_staging_structures_flat.src_url,
        CASE
            WHEN valid_staging_structures_flat.siret IS NULL
                AND annotations.siret IS NOT NULL
                THEN annotations.is_parent
            ELSE valid_staging_structures_flat.antenne
        END AS "antenne",
        COALESCE(
            valid_staging_structures_flat.siret, annotations.siret
        ) AS "siret",
        COALESCE(
            valid_staging_structures_flat.code_insee,
            valid_staging_structures_flat.normalized_code_insee
        ) AS "code_insee",
        COALESCE(
            valid_staging_structures_flat.labels_nationaux, ARRAY []::TEXT []
        ) AS "labels_nationaux",
        COALESCE(
            valid_staging_structures_flat.labels_autres, ARRAY []::TEXT []
        ) AS "labels_autres",
        COALESCE(
            valid_staging_structures_flat.thematiques, ARRAY []::TEXT []
        ) AS "thematiques"
    FROM
        valid_staging_structures_flat
    LEFT JOIN
        annotations ON
            valid_staging_structures_flat.src_alias = annotations.source
            AND valid_staging_structures_flat.id = annotations.id
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
    lien_source,
    horaires_ouverture,
    accessibilite,
    batch_id,
    created_at,
    src_url,
    antenne,
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
        (datawarehouse.data_normalized ->> 'is_valid')::BOOLEAN
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
