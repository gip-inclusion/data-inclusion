from dataclasses import dataclass
from datetime import datetime, timedelta

import polars as pl

from data_inclusion.processings import check_urls

from ..utils.db import DatabaseConnection, TableNotFoundError


@dataclass
class UrlChecker:
    db: DatabaseConnection
    cache_schema: str = "poc_enrichments"
    cache_table: str = "url_checks"
    batch_size: int = 1000
    max_retries: int = 10
    cache_duration_days: int = 30

    def run(
        self,
        structures: pl.DataFrame,
        services: pl.DataFrame,
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        print("  Running URL checks...")
        self.db.create_schema(self.cache_schema)
        self._ensure_cache_table()

        all_urls = self._extract_urls(structures, services)
        print(f"    - Found {len(all_urls)} unique URLs")

        urls_to_check = self._get_urls_to_check(all_urls)
        print(f"    - {len(urls_to_check)} URLs need checking")

        if urls_to_check:
            self._check_and_cache_urls(urls_to_check)

        structures = self._apply_url_results(structures, "site_web")
        services = self._apply_url_results(services, "lien_source")

        return structures, services

    def _ensure_cache_table(self) -> None:
        if not self.db.table_exists(self.cache_schema, self.cache_table):
            self.db.execute(f"""
                CREATE TABLE "{self.cache_schema}"."{self.cache_table}" (
                    input_url TEXT PRIMARY KEY,
                    valid_url TEXT,
                    status_code INTEGER,
                    error_message TEXT,
                    attempt_count INTEGER DEFAULT 1,
                    last_checked_at TIMESTAMP DEFAULT NOW()
                )
            """)

    def _extract_urls(
        self, structures: pl.DataFrame, services: pl.DataFrame
    ) -> list[str]:
        urls = set()
        for row in structures.to_dicts():
            if row.get("site_web"):
                urls.add(row["site_web"])
        for row in services.to_dicts():
            if row.get("lien_source"):
                urls.add(row["lien_source"])
        return list(urls)

    def _get_urls_to_check(self, all_urls: list[str]) -> list[str]:
        if not all_urls:
            return []

        try:
            cached = self.db.read_table(self.cache_schema, self.cache_table)
            cached_dict = {r["input_url"]: r for r in cached.to_dicts()}
        except TableNotFoundError:
            cached_dict = {}

        cutoff = datetime.now() - timedelta(days=self.cache_duration_days)
        urls_to_check = []

        for url in all_urls:
            if url not in cached_dict:
                urls_to_check.append(url)
            else:
                entry = cached_dict[url]
                last_checked = entry.get("last_checked_at")
                status_code = entry.get("status_code", 0)
                attempt_count = entry.get("attempt_count", 0)
                if last_checked and last_checked < cutoff:
                    urls_to_check.append(url)
                elif status_code < 0 and attempt_count < self.max_retries:
                    urls_to_check.append(url)

        return urls_to_check[: self.batch_size]

    def _check_and_cache_urls(self, urls: list[str]) -> None:
        print(f"    - Checking {len(urls)} URLs...")
        results = check_urls(urls)

        try:
            cached = self.db.read_table(self.cache_schema, self.cache_table)
            cached_dict = {r["input_url"]: r for r in cached.to_dicts()}
        except TableNotFoundError:
            cached_dict = {}

        rows = []
        for input_url, valid_url, status_code, error in results:
            attempt_count = cached_dict.get(input_url, {}).get("attempt_count", 0) + 1
            rows.append(
                {
                    "input_url": input_url,
                    "valid_url": valid_url,
                    "status_code": status_code,
                    "error_message": error,
                    "attempt_count": attempt_count,
                    "last_checked_at": datetime.now(),
                }
            )

        if rows:
            for row in rows:
                self._upsert_url(row)
            print(f"    - Cached {len(rows)} URL check results")

    def _upsert_url(self, row: dict) -> None:
        escaped_input = row["input_url"].replace("'", "''")
        escaped_valid = (row["valid_url"] or "").replace("'", "''")
        escaped_error = (row["error_message"] or "").replace("'", "''")
        error_sql = f"'{escaped_error}'" if row["error_message"] else "NULL"
        self.db.execute(
            f'INSERT INTO "{self.cache_schema}"."{self.cache_table}" '
            f"(input_url, valid_url, status_code, error_message, "
            f"attempt_count, last_checked_at) "
            f"VALUES ('{escaped_input}', '{escaped_valid}', "
            f"{row['status_code']}, {error_sql}, {row['attempt_count']}, NOW()) "
            f"ON CONFLICT (input_url) DO UPDATE SET "
            f"valid_url = EXCLUDED.valid_url, "
            f"status_code = EXCLUDED.status_code, "
            f"error_message = EXCLUDED.error_message, "
            f"attempt_count = EXCLUDED.attempt_count, "
            f"last_checked_at = EXCLUDED.last_checked_at"
        )

    def _apply_url_results(self, df: pl.DataFrame, url_column: str) -> pl.DataFrame:
        if url_column not in df.columns:
            return df

        try:
            cached = self.db.read_table(self.cache_schema, self.cache_table)
            cached_dict = {r["input_url"]: r for r in cached.to_dicts()}
        except TableNotFoundError:
            return df

        rows = []
        for row in df.to_dicts():
            url = row.get(url_column)
            if url and url in cached_dict:
                result = cached_dict[url]
                if result["status_code"] and result["status_code"] > 0:
                    row[url_column] = result["valid_url"] or url
            rows.append(row)

        return pl.DataFrame(rows, infer_schema_length=None)


def run_url_checks(
    db: DatabaseConnection,
    structures: pl.DataFrame,
    services: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    checker = UrlChecker(db)
    return checker.run(structures, services)
