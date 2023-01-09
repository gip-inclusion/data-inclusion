WITH valid_structures_flat AS (
    SELECT
        src_alias,
        data ->> 'id' AS "id",
        data ->> 'siret' AS "siret",
        data ->> 'rna' AS "rna",
        data ->> 'nom' AS "nom",
        data ->> 'commune' AS "commune",
        data ->> 'code_postal' AS "code_postal",
        COALESCE(
            data ->> 'code_insee', data_normalized ->> 'code_insee'
        ) AS "code_insee",
        data ->> 'adresse' AS "adresse",
        data ->> 'complement_adresse' AS "complement_adresse",
        (data ->> 'longitude')::FLOAT AS "longitude",
        (data ->> 'latitude')::FLOAT AS "latitude",
        data ->> 'typologie' AS "typologie",
        data ->> 'telephone' AS "telephone",
        -- remove personal emails
        CASE
            WHEN
                (data_normalized ->> 'has_personal_email')::BOOL THEN NULL
            ELSE data ->> 'courriel'
        END AS "courriel",
        data ->> 'site_web' AS "site_web",
        data ->> 'presentation_resume' AS "presentation_resume",
        data ->> 'presentation_detail' AS "presentation_detail",
        data ->> 'source' AS "source",
        data ->> 'date_maj' AS "date_maj",
        (data ->> 'antenne')::BOOL AS "antenne",
        data ->> 'lien_source' AS "lien_source",
        data ->> 'horaires_ouverture' AS "horaires_ouverture",
        data ->> 'accessibilite' AS "accessibilite",
        ARRAY(
            SELECT JSONB_ARRAY_ELEMENTS_TEXT(
                    CASE
                        WHEN
                            data -> 'labels_nationaux' = 'null' THEN '[]'::JSONB
                        ELSE data -> 'labels_nationaux'
                    END
                )
        ) AS "labels_nationaux",
        ARRAY(
            SELECT JSONB_ARRAY_ELEMENTS_TEXT(
                    CASE
                        WHEN
                            data -> 'labels_autres' = 'null' THEN '[]'::JSONB
                        ELSE data -> 'labels_autres'
                    END
                )
        ) AS "labels_autres",
        ARRAY(
            SELECT JSONB_ARRAY_ELEMENTS_TEXT(
                    CASE
                        WHEN
                            data -> 'thematiques' = 'null' THEN '[]'::JSONB
                        ELSE data -> 'thematiques'
                    END
                )
        ) AS "thematiques"
    FROM
        datawarehouse
    WHERE
        (data_normalized ->> 'is_valid')::BOOL
        AND data ? 'siret'
        -- exclude soliguide data
        AND data ->> 'source' != 'soliguide'
        AND logical_date = '{{ dag_run.conf.logical_date }}'
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
        annotation_annotation.data ->> 'id' ASC,
        annotation_annotation.created_at DESC
),

valid_structures_final AS (
    SELECT
        valid_structures_flat.id,
        valid_structures_flat.rna,
        valid_structures_flat.nom,
        valid_structures_flat.commune,
        valid_structures_flat.code_postal,
        valid_structures_flat.code_insee,
        valid_structures_flat.adresse,
        valid_structures_flat.complement_adresse,
        valid_structures_flat.longitude,
        valid_structures_flat.latitude,
        valid_structures_flat.typologie,
        valid_structures_flat.telephone,
        valid_structures_flat.courriel,
        valid_structures_flat.site_web,
        valid_structures_flat.presentation_resume,
        valid_structures_flat.presentation_detail,
        valid_structures_flat.source,
        valid_structures_flat.date_maj,
        valid_structures_flat.lien_source,
        valid_structures_flat.horaires_ouverture,
        valid_structures_flat.accessibilite,
        valid_structures_flat.labels_nationaux,
        valid_structures_flat.labels_autres,
        valid_structures_flat.thematiques,
        COALESCE(valid_structures_flat.siret, annotations.siret) AS "siret",
        COALESCE(
            valid_structures_flat.antenne, annotations.is_parent
        ) AS "antenne"
    FROM
        valid_structures_flat
    LEFT JOIN
        annotations ON
            valid_structures_flat.src_alias = annotations.source
            AND valid_structures_flat.id = annotations.id

)

SELECT *
FROM
    valid_structures_final;
