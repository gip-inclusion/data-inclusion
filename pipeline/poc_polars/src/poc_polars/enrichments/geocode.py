from dataclasses import dataclass
from datetime import datetime, timedelta

import polars as pl

from data_inclusion.processings import geocode

from ..utils.db import DatabaseConnection, TableNotFoundError


@dataclass
class Geocoder:
    db: DatabaseConnection
    cache_schema: str = "poc_enrichments"
    cache_table: str = "geocodages"
    batch_size: int = 5000
    cache_duration_days: int = 7
    min_score_threshold: float = 0.765

    def run(
        self,
        structures: pl.DataFrame,
        services: pl.DataFrame,
        adresses: pl.DataFrame,
    ) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        print("  Running geocoding...")
        self.db.create_schema(self.cache_schema)
        self._ensure_cache_table()

        addresses_to_geocode = self._get_addresses_to_geocode(adresses)
        print(f"    - {len(addresses_to_geocode)} addresses need geocoding")

        if addresses_to_geocode:
            self._geocode_and_cache(addresses_to_geocode)

        adresses = self._apply_geocoding_results(adresses)
        structures = self._apply_geocoding_to_structures(structures, adresses)
        services = self._apply_geocoding_to_services(services, adresses)

        return structures, services, adresses

    def _ensure_cache_table(self) -> None:
        if not self.db.table_exists(self.cache_schema, self.cache_table):
            self.db.execute(f"""
                CREATE TABLE "{self.cache_schema}"."{self.cache_table}" (
                    adresse_id TEXT PRIMARY KEY,
                    geocoded_at TIMESTAMP DEFAULT NOW(),
                    input_adresse TEXT,
                    input_code_postal TEXT,
                    input_code_insee TEXT,
                    input_commune TEXT,
                    result_adresse TEXT,
                    result_commune TEXT,
                    result_code_postal TEXT,
                    result_code_insee TEXT,
                    result_code_arrondissement TEXT,
                    longitude FLOAT,
                    latitude FLOAT,
                    score FLOAT,
                    result_type TEXT
                )
            """)

    def _get_addresses_to_geocode(self, adresses: pl.DataFrame) -> list[dict]:
        try:
            cached = self.db.read_table(self.cache_schema, self.cache_table)
            cached_dict = {r["adresse_id"]: r for r in cached.to_dicts()}
        except TableNotFoundError:
            cached_dict = {}

        cutoff = datetime.now() - timedelta(days=self.cache_duration_days)
        to_geocode = []

        for row in adresses.to_dicts():
            adresse_id = row.get("id")
            if not adresse_id:
                continue

            if adresse_id not in cached_dict:
                to_geocode.append(row)
            else:
                cached = cached_dict[adresse_id]
                geocoded_at = cached.get("geocoded_at")
                score = cached.get("score")
                if score is None:
                    to_geocode.append(row)
                elif score < self.min_score_threshold:
                    if geocoded_at and geocoded_at < cutoff:
                        to_geocode.append(row)
                elif cached.get("input_adresse") != row.get("adresse"):
                    to_geocode.append(row)
                elif cached.get("input_code_postal") != row.get("code_postal"):
                    to_geocode.append(row)
                elif cached.get("input_code_insee") != row.get("code_insee"):
                    to_geocode.append(row)
                elif cached.get("input_commune") != row.get("commune"):
                    to_geocode.append(row)

        return to_geocode[: self.batch_size]

    def _geocode_and_cache(self, addresses: list[dict]) -> None:
        print(f"    - Geocoding {len(addresses)} addresses...")

        inputs = []
        for addr in addresses:
            if not any(
                [
                    addr.get("code_postal"),
                    addr.get("code_insee"),
                    addr.get("commune"),
                ]
            ):
                continue
            inputs.append(
                {
                    "id": addr["id"],
                    "adresse": addr.get("adresse") or "",
                    "code_postal": addr.get("code_postal") or "",
                    "code_insee": addr.get("code_insee") or "",
                    "commune": addr.get("commune") or "",
                }
            )

        if not inputs:
            return

        try:
            results = geocode(inputs)
        except Exception as e:
            print(f"    - Geocoding failed: {e}")
            return

        results_dict = {r["id"]: r for r in results}
        addr_dict = {a["id"]: a for a in addresses}

        for adresse_id, result in results_dict.items():
            addr = addr_dict.get(adresse_id, {})
            result_citycode = result.get("result_citycode") or ""
            code_insee = result_citycode
            code_arrondissement = None
            if result_citycode[:3] in ("751", "693", "132"):
                code_arrondissement = result_citycode
                if result_citycode[:3] == "751":
                    code_insee = "75056"
                elif result_citycode[:3] == "693":
                    code_insee = "69123"
                elif result_citycode[:3] == "132":
                    code_insee = "13055"

            self._upsert_geocoding(
                adresse_id=adresse_id,
                input_adresse=addr.get("adresse"),
                input_code_postal=addr.get("code_postal"),
                input_code_insee=addr.get("code_insee"),
                input_commune=addr.get("commune"),
                result_adresse=result.get("result_name"),
                result_commune=result.get("result_city"),
                result_code_postal=result.get("result_postcode"),
                result_code_insee=code_insee,
                result_code_arrondissement=code_arrondissement,
                longitude=self._safe_float(result.get("longitude")),
                latitude=self._safe_float(result.get("latitude")),
                score=self._safe_float(result.get("result_score")),
                result_type=result.get("result_type"),
            )

        print(f"    - Cached {len(results)} geocoding results")

    def _safe_float(self, val) -> float | None:
        if val is None:
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    def _upsert_geocoding(self, **kwargs) -> None:
        def sql_val(v):
            if v is None:
                return "NULL"
            if isinstance(v, (int, float)):
                return str(v)
            return f"'{str(v).replace(chr(39), chr(39) + chr(39))}'"

        cols = ", ".join(kwargs.keys())
        vals = ", ".join(sql_val(v) for v in kwargs.values())
        updates = ", ".join(f"{k} = EXCLUDED.{k}" for k in kwargs.keys())

        self.db.execute(f"""
            INSERT INTO "{self.cache_schema}"."{self.cache_table}"
                ({cols}, geocoded_at) VALUES ({vals}, NOW())
            ON CONFLICT (adresse_id) DO UPDATE SET
                {updates}, geocoded_at = NOW()
        """)

    def _apply_geocoding_results(self, adresses: pl.DataFrame) -> pl.DataFrame:
        try:
            cached = self.db.read_table(self.cache_schema, self.cache_table)
            cached_dict = {r["adresse_id"]: r for r in cached.to_dicts()}
        except TableNotFoundError:
            return adresses

        rows = []
        for row in adresses.to_dicts():
            adresse_id = row.get("id")
            if adresse_id and adresse_id in cached_dict:
                geo = cached_dict[adresse_id]
                score = geo.get("score")
                if score and score >= self.min_score_threshold:
                    row["adresse"] = geo.get("result_adresse") or row.get("adresse")
                    row["commune"] = geo.get("result_commune") or row.get("commune")
                    row["code_postal"] = geo.get("result_code_postal") or row.get(
                        "code_postal"
                    )
                    row["code_insee"] = geo.get("result_code_insee") or row.get(
                        "code_insee"
                    )
                    row["longitude"] = geo.get("longitude") or row.get("longitude")
                    row["latitude"] = geo.get("latitude") or row.get("latitude")
            rows.append(row)

        return pl.DataFrame(rows, infer_schema_length=None)

    def _apply_geocoding_to_structures(
        self, structures: pl.DataFrame, adresses: pl.DataFrame
    ) -> pl.DataFrame:
        addr_dict = {r["id"]: r for r in adresses.to_dicts()}
        rows = []
        for row in structures.to_dicts():
            addr_id = row.get("adresse_id")
            if addr_id and addr_id in addr_dict:
                addr = addr_dict[addr_id]
                row["commune"] = addr.get("commune")
                row["code_postal"] = addr.get("code_postal")
                row["code_insee"] = addr.get("code_insee")
                row["adresse"] = addr.get("adresse")
                row["longitude"] = addr.get("longitude")
                row["latitude"] = addr.get("latitude")
            rows.append(row)
        return pl.DataFrame(rows, infer_schema_length=None)

    def _apply_geocoding_to_services(
        self, services: pl.DataFrame, adresses: pl.DataFrame
    ) -> pl.DataFrame:
        addr_dict = {r["id"]: r for r in adresses.to_dicts()}
        rows = []
        for row in services.to_dicts():
            addr_id = row.get("adresse_id")
            if addr_id and addr_id in addr_dict:
                addr = addr_dict[addr_id]
                row["commune"] = addr.get("commune")
                row["code_postal"] = addr.get("code_postal")
                row["code_insee"] = addr.get("code_insee")
                row["adresse"] = addr.get("adresse")
                row["longitude"] = addr.get("longitude")
                row["latitude"] = addr.get("latitude")
            rows.append(row)
        return pl.DataFrame(rows, infer_schema_length=None)


def run_geocoding(
    db: DatabaseConnection,
    structures: pl.DataFrame,
    services: pl.DataFrame,
    adresses: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    geocoder = Geocoder(db)
    return geocoder.run(structures, services, adresses)
