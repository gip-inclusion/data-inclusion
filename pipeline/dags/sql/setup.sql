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
