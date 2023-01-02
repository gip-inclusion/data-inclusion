WITH valid_structures_flat AS (
    SELECT
        data ->> 'id' AS "id",
        COALESCE(data ->> 'siret', data_normalized ->> 'siret') AS "siret",
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
        data ->> 'antenne' AS "antenne",
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
)

SELECT *
FROM
    valid_structures_flat;
