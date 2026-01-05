#!/usr/bin/env python3
"""
Pipeline statistics and diagnostics.

Shows comparisons with DBT, rejection summaries, and enrichment stats.
"""

import argparse
import sys

from .run import SOURCES
from .utils.db import DatabaseConnection, TableNotFoundError


def compare_with_dbt(db: DatabaseConnection, sources: list[str]) -> None:
    print("\n" + "=" * 60)
    print("COMPARISON: PoC vs DBT")
    print("=" * 60)

    comparisons = []
    for source in sources:
        source_key = source.replace("-", "_")
        comparisons.extend(
            [
                (
                    "poc_marts.marts__structures_v1",
                    "public_marts.marts__structures_v1",
                    f"source = '{source}'",
                ),
                (
                    "poc_marts.marts__services_v1",
                    "public_marts.marts__services_v1",
                    f"source = '{source}'",
                ),
                (
                    f"poc_intermediate.int_{source_key}__structures_v1",
                    f"public_intermediate.int_{source_key}__structures_v1",
                    None,
                ),
                (
                    f"poc_intermediate.int_{source_key}__services_v1",
                    f"public_intermediate.int_{source_key}__services_v1",
                    None,
                ),
            ]
        )

    for poc_table, dbt_table, filter_cond in comparisons:
        print(f"\n{'-' * 40}")
        print(f"Comparing: {poc_table}")
        print(f"      vs:  {dbt_table}")

        try:
            filt = f" WHERE {filter_cond}" if filter_cond else ""
            poc_n = db.read_query(f"SELECT COUNT(*) as cnt FROM {poc_table}{filt}")[
                "cnt"
            ][0]
            dbt_n = db.read_query(f"SELECT COUNT(*) as cnt FROM {dbt_table}{filt}")[
                "cnt"
            ][0]
            print(f"  PoC rows: {poc_n}")
            print(f"  DBT rows: {dbt_n}")
            if poc_n == dbt_n:
                print("  ✅ Row counts match!")
            else:
                print(f"  ⚠️  Row count difference: {poc_n - dbt_n:+d}")
        except TableNotFoundError as e:
            print(f"  ❌ Table not found: {e}")


def show_rejected(db: DatabaseConnection) -> None:
    print("\n" + "=" * 60)
    print("REJECTED ROWS SUMMARY")
    print("=" * 60)

    for table in ["rejected_structures", "rejected_services"]:
        try:
            n = db.read_query(f"SELECT COUNT(*) as cnt FROM poc_rejected.{table}")[
                "cnt"
            ][0]
        except TableNotFoundError:
            print(f"\n{table}: Table does not exist")
            continue

        if n == 0:
            print(f"\n{table}: No rejected rows ✅")
            continue

        print(f"\n{table}: {n} rejected rows")

        by_source = db.read_query(f"""
            SELECT source, COUNT(*) as cnt FROM poc_rejected.{table}
            GROUP BY source ORDER BY cnt DESC
        """)
        print("  By source:")
        for row in by_source.to_dicts():
            print(f"    - {row['source']}: {row['cnt']} rows")

        by_reason = db.read_query(f"""
            SELECT field, reason, COUNT(*) as cnt FROM poc_rejected.{table}
            GROUP BY field, reason ORDER BY cnt DESC LIMIT 10
        """)
        print("  By reason:")
        for row in by_reason.to_dicts():
            reason = (row.get("reason") or "Unknown")[:40]
            print(f"    - {row['field']}: {reason} ({row['cnt']} rows)")

        sample = db.read_query(f"""
            SELECT source, id, nom, field, reason, value
            FROM poc_rejected.{table} LIMIT 3
        """)
        print("  Sample rejections:")
        for row in sample.to_dicts():
            nom = (row.get("nom") or "")[:30]
            val = (row.get("value") or "")[:40]
            print(f"    - [{row['source']}] {row['id']}: {nom}")
            print(f"      {row['field']}: {row['reason']}")
            print(f"      value: {val}...")


def show_enrichments(db: DatabaseConnection) -> None:
    print("\n" + "=" * 60)
    print("ENRICHMENTS SUMMARY")
    print("=" * 60)

    try:
        siret_stats = db.read_query("""
            SELECT "_siret_status", COUNT(*) as cnt
            FROM poc_marts.marts__structures_v1
            GROUP BY "_siret_status" ORDER BY cnt DESC
        """)
        print("\nSIRET Status:")
        for row in siret_stats.to_dicts():
            print(f"  - {row['_siret_status'] or 'NULL'}: {row['cnt']}")
    except TableNotFoundError:
        print("\nSIRET Status: Table not found")

    try:
        closed_stats = db.read_query("""
            SELECT "_is_closed", COUNT(*) as cnt
            FROM poc_marts.marts__structures_v1
            GROUP BY "_is_closed"
        """)
        print("\nClosed Status:")
        for row in closed_stats.to_dicts():
            print(f"  - {'Closed' if row['_is_closed'] else 'Open'}: {row['cnt']}")
    except TableNotFoundError:
        pass

    try:
        cluster_stats = db.read_query("""
            SELECT
                COUNT(*) as total,
                COUNT("_cluster_id") as with_cluster,
                COUNT(DISTINCT "_cluster_id") as num_clusters
            FROM poc_marts.marts__structures_v1
        """).to_dicts()[0]
        print("\nDeduplication:")
        print(f"  - Total structures: {cluster_stats['total']}")
        print(f"  - With cluster_id: {cluster_stats['with_cluster']}")
        print(f"  - Number of clusters: {cluster_stats['num_clusters']}")
    except TableNotFoundError:
        pass

    try:
        url_n = db.read_query("SELECT COUNT(*) as cnt FROM poc_enrichments.url_checks")[
            "cnt"
        ][0]
        print(f"\nURL Checks: {url_n} cached")
    except TableNotFoundError:
        pass

    try:
        geo_n = db.read_query("SELECT COUNT(*) as cnt FROM poc_enrichments.geocodages")[
            "cnt"
        ][0]
        print(f"Geocoding: {geo_n} cached")
    except TableNotFoundError:
        pass

    try:
        pii_stats = db.read_query("""
            SELECT "_has_pii", COUNT(*) as cnt
            FROM poc_marts.marts__structures_v1
            GROUP BY "_has_pii"
        """)
        print("\nPII Detection:")
        for row in pii_stats.to_dicts():
            print(f"  - {'Has PII' if row['_has_pii'] else 'No PII'}: {row['cnt']}")
    except TableNotFoundError:
        pass

    try:
        bad_n = db.read_query("""
            SELECT COUNT(*) as cnt FROM poc_marts.marts__structures_v1
            WHERE "_email_is_bad" = TRUE
        """)["cnt"][0]
        if bad_n > 0:
            print(f"\nBad Emails (Brevo): {bad_n} structures")
    except TableNotFoundError:
        pass


def main():
    parser = argparse.ArgumentParser(description="Show pipeline statistics")
    parser.add_argument(
        "--source",
        action="append",
        dest="sources",
        choices=list(SOURCES.keys()),
        help="Source to check (can be repeated)",
    )
    parser.add_argument("--compare", action="store_true", help="Compare with DBT")
    parser.add_argument("--rejected", action="store_true", help="Show rejected rows")
    parser.add_argument(
        "--enrichments", action="store_true", help="Show enrichment stats"
    )
    parser.add_argument("--all", action="store_true", help="Show all stats")
    args = parser.parse_args()

    db = DatabaseConnection()

    try:
        db.read_query("SELECT 1")
    except ConnectionError as e:
        print(f"❌ Failed to connect to database: {e}")
        sys.exit(1)

    sources = args.sources or list(SOURCES.keys())
    show_all = args.all or not (args.compare or args.rejected or args.enrichments)

    if args.compare or show_all:
        compare_with_dbt(db, sources)
    if args.rejected or show_all:
        show_rejected(db)
    if args.enrichments or show_all:
        show_enrichments(db)


if __name__ == "__main__":
    main()
