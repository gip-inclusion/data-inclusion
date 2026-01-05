from dataclasses import dataclass

import polars as pl

from ..utils.db import DatabaseConnection, TableNotFoundError


def validate_siret(siret: str | None) -> bool:
    if siret is None or len(siret) != 14 or not siret.isdigit():
        return False
    total = sum(
        int(siret[i]) * (2 if i % 2 == 0 else 1)
        if int(siret[i]) * (2 if i % 2 == 0 else 1) < 10
        else int(siret[i]) * (2 if i % 2 == 0 else 1) - 9
        for i in range(14)
    )
    return total % 10 == 0


def validate_siret_luhn(siret: str | None) -> bool:
    if siret is None or len(siret) != 14 or not siret.isdigit():
        return False
    digits = [int(d) for d in siret]
    checksum = 0
    for i, d in enumerate(digits):
        if i % 2 == 0:
            d = d * 2
            if d > 9:
                d = d - 9
        checksum += d
    return checksum % 10 == 0


@dataclass
class SiretEnrichment:
    db: DatabaseConnection

    def enrich(self, structures: pl.DataFrame) -> pl.DataFrame:
        sirets = (
            structures.filter(pl.col("siret").is_not_null())["siret"].unique().to_list()
        )
        if not sirets:
            return structures.with_columns(
                pl.lit(None).alias("_siret_status"),
                pl.lit(False).alias("_is_closed"),
            )

        sirene_data = self._lookup_sirene(sirets)
        sirene_dict = {r["siret"]: r for r in sirene_data.to_dicts()}

        rows = []
        for row in structures.to_dicts():
            siret = row.get("siret")
            if siret and siret in sirene_dict:
                info = sirene_dict[siret]
                row["_siret_status"] = info.get("statut", "valide")
                row["_is_closed"] = info.get("etat") == "fermé"
            elif siret:
                if validate_siret_luhn(siret):
                    row["_siret_status"] = "inconnu"
                    row["_is_closed"] = False
                else:
                    row["_siret_status"] = "invalide"
                    row["_is_closed"] = False
            else:
                row["_siret_status"] = None
                row["_is_closed"] = False
            rows.append(row)

        return pl.DataFrame(rows, infer_schema_length=None)

    def _lookup_sirene(self, sirets: list[str]) -> pl.DataFrame:
        if not sirets:
            return pl.DataFrame({"siret": [], "etat": [], "statut": []})

        try:
            sirets_str = ",".join(f"'{s}'" for s in sirets)
            query = f"""
                SELECT DISTINCT ON (siret)
                    siret,
                    etat_administratif_etablissement as etat,
                    date_debut
                FROM public_staging.stg_sirene__etablissement_historique
                WHERE siret IN ({sirets_str})
                ORDER BY siret, date_debut DESC
            """
            result = self.db.read_query(query)
            return result.with_columns(
                pl.when(pl.col("etat") == "fermé")
                .then(pl.lit("fermé-définitivement"))
                .otherwise(pl.lit("valide"))
                .alias("statut")
            )
        except TableNotFoundError as e:
            print(f"  Warning: Could not lookup SIRENE data: {e}")
            return pl.DataFrame({"siret": [], "etat": [], "statut": []})


def enrich_sirets(db: DatabaseConnection, structures: pl.DataFrame) -> pl.DataFrame:
    enricher = SiretEnrichment(db)
    return enricher.enrich(structures)
