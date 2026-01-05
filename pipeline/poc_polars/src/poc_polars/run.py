#!/usr/bin/env python3
"""
PoC Polars Pipeline Runner

Pipeline structure:
1. STAGING: All sources process raw data into staging tables
2. INTERMEDIATE: All sources transform staging into intermediate tables
3. ENRICHMENTS: Global enrichments run across all sources (dedup, email, etc.)
4. MARTS: Final marts tables are produced with enrichment results
"""

import argparse
import sys
from datetime import datetime

import polars as pl

from .enrichments import GlobalEnrichments
from .intermediate.france_travail import FranceTravailIntermediate
from .intermediate.soliguide import SoliguideIntermediate
from .marts.france_travail import FranceTravailMarts
from .marts.soliguide import SoliguideMarts
from .staging.france_travail import FranceTravailStaging
from .staging.soliguide import SoliguideStaging
from .utils.db import DatabaseConnection

SOURCES = {
    "france-travail": {
        "staging": FranceTravailStaging,
        "intermediate": FranceTravailIntermediate,
        "marts": FranceTravailMarts,
    },
    "soliguide": {
        "staging": SoliguideStaging,
        "intermediate": SoliguideIntermediate,
        "marts": SoliguideMarts,
    },
}


def _cast_null_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Cast Null-typed columns to String for concat compatibility."""
    for col in df.columns:
        if df[col].dtype == pl.Null:
            df = df.with_columns(pl.col(col).cast(pl.Utf8))
    return df


def run_pipeline(
    db: DatabaseConnection,
    sources: list[str] | None = None,
    skip_enrichments: bool = False,
) -> dict[str, dict[str, pl.DataFrame]]:
    sources = sources or list(SOURCES.keys())

    print("=" * 60)
    print("PoC Polars Pipeline")
    print(f"Sources: {', '.join(sources)}")
    print(f"Started at: {datetime.now()}")
    if skip_enrichments:
        print("(Enrichments SKIPPED)")
    print("=" * 60)

    all_results = {}

    # PHASE 1: STAGING
    print("\n" + "=" * 60)
    print("PHASE 1: STAGING")
    print("=" * 60)
    for source in sources:
        if source not in SOURCES:
            print(f"Unknown source: {source}")
            continue
        print(f"\n[STAGING] {source.upper()}")
        print("-" * 40)
        staging = SOURCES[source]["staging"](db)
        all_results[source] = {"staging": staging.run()}

    # PHASE 2: INTERMEDIATE
    print("\n" + "=" * 60)
    print("PHASE 2: INTERMEDIATE")
    print("=" * 60)
    for source in sources:
        if source not in SOURCES:
            continue
        print(f"\n[INTERMEDIATE] {source.upper()}")
        print("-" * 40)
        intermediate = SOURCES[source]["intermediate"](db)
        all_results[source]["intermediate"] = intermediate.run()

    # PHASE 3: GLOBAL ENRICHMENTS
    print("\n" + "=" * 60)
    print("PHASE 3: GLOBAL ENRICHMENTS")
    print("=" * 60)

    all_structures, all_services = [], []
    for source in sources:
        source_key = source.replace("-", "_")
        structures = db.read_table(
            "poc_intermediate", f"int_{source_key}__structures_v1"
        )
        all_structures.append(structures)
        services = db.read_table("poc_intermediate", f"int_{source_key}__services_v1")
        all_services.append(services)

    all_structures = [_cast_null_columns(df) for df in all_structures]
    all_services = [_cast_null_columns(df) for df in all_services]
    combined_structures = pl.concat(all_structures, how="diagonal")
    combined_services = pl.concat(all_services, how="diagonal")

    print(f"  Combined {len(combined_structures)} structures")
    print(f"  Combined {len(combined_services)} services")

    if skip_enrichments:
        print("  (Skipping enrichments)")
        enriched_structures = combined_structures
        enriched_services = combined_services
    else:
        enrichments = GlobalEnrichments(db)
        enriched_structures, enriched_services = enrichments.run(
            combined_structures, combined_services
        )

    # PHASE 4: MARTS
    print("\n" + "=" * 60)
    print("PHASE 4: MARTS")
    print("=" * 60)

    rejections_structures, rejections_services = [], []

    for source in sources:
        if source not in SOURCES:
            continue
        print(f"\n[MARTS] {source.upper()}")
        print("-" * 40)

        source_structures = enriched_structures.filter(pl.col("source") == source)
        source_services = enriched_services.filter(pl.col("source") == source)

        marts = SOURCES[source]["marts"](db)
        marts_tables, rejected_str, rejected_svc = marts.run_with_data(
            source_structures, source_services
        )
        all_results[source]["marts"] = marts_tables

        if len(rejected_str) > 0:
            rejections_structures.append(rejected_str)
        if len(rejected_svc) > 0:
            rejections_services.append(rejected_svc)

    # PHASE 5: STORE REJECTIONS
    print("\n" + "=" * 60)
    print("STORING REJECTIONS")
    print("=" * 60)

    db.create_schema("poc_rejected")

    if rejections_structures:
        all_rej_str = pl.concat(rejections_structures, how="diagonal")
        db.write_table(
            all_rej_str,
            "poc_rejected",
            "rejected_structures",
            if_table_exists="replace",
        )
        print(f"  - Stored {len(all_rej_str)} rejected structures")
    else:
        print("  - No rejected structures")

    if rejections_services:
        all_rej_svc = pl.concat(rejections_services, how="diagonal")
        db.write_table(
            all_rej_svc, "poc_rejected", "rejected_services", if_table_exists="replace"
        )
        print(f"  - Stored {len(all_rej_svc)} rejected services")
    else:
        print("  - No rejected services")

    print("\n" + "=" * 60)
    print(f"Completed at: {datetime.now()}")
    print("=" * 60)

    return all_results


def main():
    parser = argparse.ArgumentParser(description="Run the PoC Polars pipeline")
    parser.add_argument(
        "--source",
        action="append",
        dest="sources",
        choices=list(SOURCES.keys()),
        help="Source to process (can be repeated)",
    )
    parser.add_argument(
        "--skip-enrichments",
        action="store_true",
        help="Skip enrichment steps",
    )
    args = parser.parse_args()

    db = DatabaseConnection()

    try:
        db.read_query("SELECT 1")
        print(f"✅ Connected to database at {db.host}:{db.port}")
    except ConnectionError as e:
        print(f"❌ Failed to connect to database: {e}")
        sys.exit(1)

    run_pipeline(db, sources=args.sources, skip_enrichments=args.skip_enrichments)


if __name__ == "__main__":
    main()
