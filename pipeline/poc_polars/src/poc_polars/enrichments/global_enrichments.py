"""
Global enrichments that run across all sources.

These enrichments must run AFTER all sources' staging and intermediate layers,
and BEFORE the marts layer. Order matters:

1. Phone formatting - normalize for matching
2. Email checks - PII detection + Brevo verification
3. SIRET validation - cached globally
4. URL checks - cached globally
5. Deduplication - MUST run LAST because it needs all cleaned data
"""

from dataclasses import dataclass

import polars as pl

from data_inclusion.processings import format_phone_number

from ..utils.db import DatabaseConnection
from .deduplicate import run_deduplication
from .emails import run_email_checks
from .siret import enrich_sirets
from .urls import run_url_checks


def _format_phones(df: pl.DataFrame) -> pl.DataFrame:
    """Format all phone numbers in the DataFrame using libphonenumbers."""
    if "telephone" not in df.columns:
        return df

    rows = df.to_dicts()
    for row in rows:
        if row.get("telephone"):
            row["telephone"] = format_phone_number(row["telephone"])
    return pl.DataFrame(rows, infer_schema_length=None)


@dataclass
class GlobalEnrichments:
    db: DatabaseConnection
    skip_urls: bool = False
    skip_deduplication: bool = False

    def run(
        self,
        structures: pl.DataFrame,
        services: pl.DataFrame,
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """
        Run all global enrichments on structures/services from all sources.

        Geocoding is handled at the source level (in intermediate) since it
        requires source-specific address tables.

        Returns enriched structures and services with additional columns:
        - _cluster_id: deduplication cluster
        - _has_pii: personal email detected
        - _email_is_bad: email hardbounced or objected in Brevo
        - _siret_status: SIRET validation status
        """
        print("\n" + "=" * 60)
        print("GLOBAL ENRICHMENTS")
        print("=" * 60)

        # 1. Format phone numbers (for consistent matching in dedup)
        print("  Formatting phone numbers...")
        structures = _format_phones(structures)
        services = _format_phones(services)

        # 2. Email checks (PII detection + Brevo verification)
        structures, services = run_email_checks(self.db, structures, services)

        # 3. SIRET validation (cached globally)
        structures = enrich_sirets(self.db, structures)

        # 4. URL checks (cached globally)
        if not self.skip_urls:
            structures, services = run_url_checks(self.db, structures, services)
        else:
            print("  Skipping URL checks...")

        # 5. Deduplication - runs LAST for best data quality
        if not self.skip_deduplication:
            structures = run_deduplication(self.db, structures)
        else:
            print("  Skipping deduplication...")
            if "_cluster_id" not in structures.columns:
                structures = structures.with_columns(
                    pl.lit(None).cast(pl.Int64).alias("_cluster_id")
                )

        return structures, services
