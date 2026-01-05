from dataclasses import asdict, dataclass

import polars as pl

from data_inclusion.processings import deduplicate
from data_inclusion.processings.deduplicate import DeduplicateInput

from ..utils.db import DatabaseConnection, TableNotFoundError


@dataclass
class StructureDeduplicator:
    db: DatabaseConnection

    def run(self, structures: pl.DataFrame) -> pl.DataFrame:
        print("  Running deduplication...")

        # Load addresses to join with structures
        adresses = self._load_all_addresses()

        inputs = self._prepare_inputs(structures, adresses)
        if not inputs:
            return structures.with_columns(
                pl.lit(None).cast(pl.Int64).alias("_cluster_id")
            )

        print(f"    - Deduplicating {len(inputs)} structures...")
        try:
            # deduplicate expects list of dicts, not DeduplicateInput objects
            input_dicts = [asdict(inp) for inp in inputs]
            clusters = deduplicate(data=input_dicts)
        except Exception as e:
            print(f"    - Deduplication failed: {e}")
            return structures.with_columns(
                pl.lit(None).cast(pl.Int64).alias("_cluster_id")
            )

        cluster_map = {c["structure_id"]: c["cluster_id"] for c in clusters}
        num_clusters = len(set(cluster_map.values()))
        print(f"    - Found {num_clusters} clusters with {len(cluster_map)} duplicates")

        # Use Polars operations to add cluster_id column
        structure_ids = structures["id"].to_list()
        cluster_ids = [cluster_map.get(sid) for sid in structure_ids]

        return structures.with_columns(
            pl.Series("_cluster_id", cluster_ids, dtype=pl.Int64)
        )

    def _load_all_addresses(self) -> dict[str, dict]:
        """Load all addresses from intermediate tables for all sources."""
        adresses = {}

        # Try to load addresses for each known source
        source_patterns = [
            ("poc_intermediate", "int_soliguide__adresses_v1"),
            ("poc_intermediate", "int_france_travail__adresses_v1"),
        ]

        for schema, table in source_patterns:
            try:
                df = self.db.read_table(schema, table)
                for row in df.to_dicts():
                    adresses[row.get("id")] = row
            except TableNotFoundError:
                pass

        return adresses

    def _prepare_inputs(
        self, structures: pl.DataFrame, adresses: dict[str, dict]
    ) -> list[DeduplicateInput]:
        inputs = []
        for row in structures.to_dicts():
            if not row.get("nom"):
                continue

            # Join with address data
            addr = adresses.get(row.get("adresse_id"), {})

            try:
                inputs.append(
                    DeduplicateInput(
                        _di_surrogate_id=row["id"],
                        adresse=addr.get("adresse") or "",
                        code_insee=addr.get("code_insee") or "",
                        code_postal=addr.get("code_postal") or "",
                        commune=addr.get("commune") or "",
                        courriel=row.get("courriel") or "",
                        date_maj=str(row.get("date_maj") or ""),
                        latitude=float(addr["latitude"])
                        if addr.get("latitude")
                        else 0.0,
                        longitude=float(addr["longitude"])
                        if addr.get("longitude")
                        else 0.0,
                        nom=row.get("nom") or "",
                        siret=row.get("siret") or "",
                        source=row.get("source") or "",
                        telephone=row.get("telephone") or "",
                    )
                )
            except (KeyError, TypeError, ValueError):
                continue
        return inputs


def run_deduplication(db: DatabaseConnection, structures: pl.DataFrame) -> pl.DataFrame:
    deduplicator = StructureDeduplicator(db)
    return deduplicator.run(structures)
