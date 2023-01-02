WITH valid_structures AS (
    SELECT
        batch_id,
        src_url,
        data ->> 'id' AS "id"
    FROM
        datawarehouse
    WHERE
        (data_normalized ->> 'is_valid')::BOOL
        AND data ? 'siret'
        -- exclude soliguide data from rollout
        AND data ->> 'source' != 'soliguide'
        AND logical_date = '{{ dag_run.conf.logical_date }}'
),

valid_services AS (
    SELECT datawarehouse.*
    FROM
        valid_structures
    INNER JOIN datawarehouse
        ON valid_structures.batch_id = datawarehouse.batch_id
            AND valid_structures.src_url = datawarehouse.src_url
            AND valid_structures.id = (datawarehouse.data ->> 'structure_id')
    WHERE
        (datawarehouse.data_normalized ->> 'is_valid')::BOOL
        AND datawarehouse.data ? 'structure_id'
),

valid_services_flat AS (
    SELECT
        data ->> 'id' AS "id",
        data ->> 'structure_id' AS "structure_id",
        data ->> 'source' AS "source",
        data ->> 'nom' AS "nom",
        data ->> 'presentation_resume' AS "presentation_resume",
        data ->> 'prise_rdv' AS "prise_rdv",
        data ->> 'frais_autres' AS "frais_autres",
        ARRAY(
            SELECT JSONB_ARRAY_ELEMENTS_TEXT(
                    CASE
                        WHEN
                            data -> 'types' = 'null' THEN '[]'::JSONB
                        ELSE data -> 'types'
                    END
                )
        ) AS "types",
        ARRAY(
            SELECT JSONB_ARRAY_ELEMENTS_TEXT(
                    CASE
                        WHEN
                            data -> 'thematiques' = 'null' THEN '[]'::JSONB
                        ELSE data -> 'thematiques'
                    END
                )
        ) AS "thematiques",
        ARRAY(
            SELECT JSONB_ARRAY_ELEMENTS_TEXT(
                    CASE
                        WHEN
                            data -> 'frais' = 'null' THEN '[]'::JSONB
                        ELSE data -> 'frais'
                    END
                )
        ) AS "frais",
        ARRAY(
            SELECT JSONB_ARRAY_ELEMENTS_TEXT(
                    CASE
                        WHEN
                            data -> 'profils' = 'null' THEN '[]'::JSONB
                        ELSE data -> 'profils'
                    END
                )
        ) AS "profils"
    FROM
        valid_services
)

SELECT *
FROM
    valid_services_flat;
