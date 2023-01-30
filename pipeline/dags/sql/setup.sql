/*
The "datalake" table stores the raw data collected each day.

Each row corresponds to a row in a source.

The only transformation done at this point should be the conversion to json,
when the source data is stored in a non json format (csv, excel, etc.).
This is required for storage in a schemaless jsonb column.

* "src_alias": name for the datasource
* "src_url": url to the datasource
* "file": ressource path in the datalake bucket on s3. The stream associated to
a given row can be deduced from this column.
* "data": raw data
* "data_normalized": used to store source row id
*/
CREATE TABLE IF NOT EXISTS datalake (
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    logical_date DATE,
    batch_id TEXT,
    src_alias TEXT,
    src_url TEXT,
    file TEXT,
    data JSONB,
    data_normalized JSONB DEFAULT '{}'::JSONB
) PARTITION BY RANGE (logical_date);

CREATE INDEX IF NOT EXISTS datalake_id_idx ON datalake (
    (data_normalized ->> 'id')
);
CREATE INDEX IF NOT EXISTS datalake_batch_id_idx ON datalake (batch_id);
CREATE INDEX IF NOT EXISTS datalake_src_alias_idx ON datalake (src_alias);

-- Alias the latest datalake partition for convenience
CREATE OR REPLACE VIEW datalake_latest AS (
    SELECT * -- noqa: L044
    FROM datalake
    WHERE logical_date = '{{ dag_run.logical_date.astimezone(dag.timezone)|ds|replace("-", "_") }}' -- noqa: L016
);

CREATE TABLE IF NOT EXISTS datawarehouse (
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    logical_date DATE,
    batch_id TEXT,
    src_alias TEXT,
    src_url TEXT,
    file TEXT,
    data JSONB,
    data_normalized JSONB DEFAULT '{}'::JSONB
) PARTITION BY RANGE (logical_date);

CREATE INDEX IF NOT EXISTS datawarehouse_id_idx ON datawarehouse (
    (data ->> 'id')
);
CREATE INDEX IF NOT EXISTS datawarehouse_batch_id_idx ON datawarehouse (
    batch_id
);
CREATE INDEX IF NOT EXISTS datawarehouse_src_alias_idx ON datawarehouse (
    src_alias
);

CREATE TABLE IF NOT EXISTS flux_v2 (
    id UUID DEFAULT gen_random_uuid(),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    logical_date DATE,
    batch_id TEXT,
    src_alias TEXT,
    src_url TEXT,
    type TEXT,
    row_id TEXT,
    data_prev JSONB,
    data_next JSONB,
    PRIMARY KEY (logical_date, id)
) PARTITION BY RANGE (logical_date);

CREATE INDEX IF NOT EXISTS flux_id_idx ON flux_v2 (id);
CREATE INDEX IF NOT EXISTS flux_row_id_idx ON flux_v2 (row_id);
CREATE INDEX IF NOT EXISTS flux_batch_id_idx ON flux_v2 (batch_id);
CREATE INDEX IF NOT EXISTS flux_src_alias_idx ON flux_v2 (src_alias);

CREATE TABLE IF NOT EXISTS datalake_{{ dag_run.logical_date.astimezone(dag.timezone)|ds|replace("-", "_") }} PARTITION OF datalake FOR -- noqa: L016
VALUES
FROM
    (
        '{{ data_interval_start.astimezone(dag.timezone)|ds|replace("-", "_") }}'
    ) TO (
        '{{ data_interval_end.astimezone(dag.timezone)|ds|replace("-", "_") }}'
        );

CREATE TABLE IF NOT EXISTS datawarehouse_{{ dag_run.logical_date.astimezone(dag.timezone)|ds|replace("-", "_") }} PARTITION OF datawarehouse FOR -- noqa: L016
VALUES
FROM
    (
        '{{ data_interval_start.astimezone(dag.timezone)|ds|replace("-", "_") }}'
    ) TO (
        '{{ data_interval_end.astimezone(dag.timezone)|ds|replace("-", "_") }}'
        );

CREATE TABLE IF NOT EXISTS flux_v2_{{ dag_run.logical_date.astimezone(dag.timezone)|ds|replace("-", "_") }} PARTITION OF flux_v2 FOR -- noqa: L016
VALUES
FROM
    (
        '{{ data_interval_start.astimezone(dag.timezone)|ds|replace("-", "_") }}'
    ) TO (
        '{{ data_interval_end.astimezone(dag.timezone)|ds|replace("-", "_") }}'
        );
