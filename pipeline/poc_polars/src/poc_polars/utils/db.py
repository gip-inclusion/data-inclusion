import json
import os
from dataclasses import dataclass, field

import polars as pl
from sqlalchemy import MetaData, Table, create_engine, inspect, text
from sqlalchemy.exc import NoSuchTableError, ProgrammingError


class TableNotFoundError(Exception):
    """Raised when a table or schema does not exist."""

    pass


def _convert_lists_to_json(df: pl.DataFrame) -> pl.DataFrame:
    for col in df.columns:
        if df[col].dtype.base_type() == pl.List:
            df = df.with_columns(
                pl.col(col)
                .map_elements(
                    lambda x: json.dumps(x.to_list()) if x is not None else None,
                    return_dtype=pl.Utf8,
                )
                .alias(col)
            )
    return df


@dataclass
class DatabaseConnection:
    url: str = field(
        default_factory=lambda: os.getenv(
            "DATABASE_URL",
            "postgresql://data-inclusion:data-inclusion@localhost:5455/data-inclusion",
        )
    )

    def __post_init__(self):
        self._engine = create_engine(self.url)

    @property
    def host(self) -> str:
        return self._engine.url.host or "localhost"

    @property
    def port(self) -> int:
        return self._engine.url.port or 5432

    def read_table(
        self, schema: str, table: str, infer_schema_length: int | None = None
    ) -> pl.DataFrame:
        try:
            with self._engine.connect() as conn:
                return pl.read_database(
                    f'SELECT * FROM "{schema}"."{table}"',
                    connection=conn,
                    infer_schema_length=infer_schema_length,
                )
        except ProgrammingError as e:
            raise TableNotFoundError(f"{schema}.{table}") from e

    def read_query(self, query: str) -> pl.DataFrame:
        try:
            with self._engine.connect() as conn:
                return pl.read_database(query, connection=conn)
        except ProgrammingError as e:
            if "does not exist" in str(e):
                raise TableNotFoundError(str(e)) from e
            raise

    def execute(self, sql: str) -> None:
        with self._engine.connect() as conn:
            conn.execute(text(sql))
            conn.commit()

    def create_schema(self, schema: str) -> None:
        inspector = inspect(self._engine)
        if schema not in inspector.get_schema_names():
            self.execute(f'CREATE SCHEMA "{schema}"')

    def write_table(
        self,
        df: pl.DataFrame,
        schema: str,
        table: str,
        if_table_exists: str = "replace",
    ) -> None:
        df = _convert_lists_to_json(df)
        df.write_database(
            f'"{schema}"."{table}"',
            connection=self.url,
            if_table_exists=if_table_exists,
        )

    def drop_table(self, schema: str, table: str) -> None:
        metadata = MetaData(schema=schema)
        try:
            tbl = Table(table, metadata, autoload_with=self._engine)
            tbl.drop(self._engine, checkfirst=True)
        except NoSuchTableError:
            pass

    def table_exists(self, schema: str, table: str) -> bool:
        inspector = inspect(self._engine)
        return table in inspector.get_table_names(schema=schema)
