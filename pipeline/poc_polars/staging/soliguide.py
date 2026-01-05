import json
import re
from dataclasses import dataclass
from datetime import date, datetime

import polars as pl

from ..utils.db import DatabaseConnection

TIMESTAMP_PATTERN = re.compile(r"(\d{4}-\d{2}-\d{2})T\d{2}:\d{2}:\d{2}\.\d{3}Z")


def parse_soliguide_timestamp(value: str | None) -> date | None:
    if not value:
        return None
    match = TIMESTAMP_PATTERN.match(value)
    if not match:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y-%m-%d").date()
    except Exception:
        return None


def to_bool(v) -> bool | None:
    if v is None:
        return None
    return bool(v) if isinstance(v, bool) else None


LIEUX_SCHEMA = {
    "lieu_id": pl.Utf8,
    "updated_at": pl.Date,
    "position__coordinates__x": pl.Float64,
    "position__coordinates__y": pl.Float64,
    "modalities__inconditionnel": pl.Boolean,
    "modalities__appointment__checked": pl.Boolean,
    "modalities__inscription__checked": pl.Boolean,
    "modalities__orientation__checked": pl.Boolean,
    "name": pl.Utf8,
    "position__city": pl.Utf8,
    "position__city_code": pl.Utf8,
    "position__country": pl.Utf8,
    "description": pl.Utf8,
    "seo_url": pl.Utf8,
    "position__postal_code": pl.Utf8,
    "position__address": pl.Utf8,
    "position__additional_information": pl.Utf8,
    "position__department": pl.Utf8,
    "position__department_code": pl.Utf8,
    "publics__accueil": pl.Int64,
    "publics__age__min": pl.Int64,
    "publics__age__max": pl.Int64,
    "publics__description": pl.Utf8,
    "entity_mail": pl.Utf8,
    "entity_website": pl.Utf8,
    "temp_infos__message__name": pl.Utf8,
    "temp_infos__closure__actif": pl.Boolean,
    "temp_infos__hours__actif": pl.Boolean,
    "temp_infos__hours__hours": pl.Utf8,
    "newhours": pl.Utf8,
    "modalities__other": pl.Utf8,
    "modalities__price__precisions": pl.Utf8,
    "modalities__appointment__precisions": pl.Utf8,
    "modalities__inscription__precisions": pl.Utf8,
    "modalities__orientation__precisions": pl.Utf8,
    "modalities__pmr__checked": pl.Boolean,
}

SERVICES_SCHEMA = {
    "lieu_id": pl.Utf8,
    "id": pl.Utf8,
    "category": pl.Utf8,
    "description": pl.Utf8,
    "hours": pl.Utf8,
    "saturated__status": pl.Utf8,
    "different_hours": pl.Boolean,
    "close__actif": pl.Boolean,
    "close__date_debut": pl.Date,
    "close__date_fin": pl.Date,
    "modalities__other": pl.Utf8,
    "modalities__inconditionnel": pl.Boolean,
    "modalities__appointment__checked": pl.Boolean,
    "modalities__appointment__precisions": pl.Utf8,
    "modalities__price__checked": pl.Boolean,
    "modalities__price__precisions": pl.Utf8,
    "modalities__inscription__checked": pl.Boolean,
    "modalities__inscription__precisions": pl.Utf8,
    "modalities__orientation__checked": pl.Boolean,
    "modalities__orientation__precisions": pl.Utf8,
    "publics__description": pl.Utf8,
    "different_modalities": pl.Boolean,
    "different_publics": pl.Boolean,
}

PHONES_SCHEMA = {"lieu_id": pl.Utf8, "label": pl.Utf8, "phone_number": pl.Utf8}
SOURCES_SCHEMA = {"lieu_id": pl.Utf8, "name": pl.Utf8}
PUBLICS_SCHEMA = {"lieu_id": pl.Utf8, "value": pl.Utf8}


@dataclass
class SoliguideStaging:
    db: DatabaseConnection
    source_schema: str = "soliguide"
    target_schema: str = "poc_staging"

    def run(self) -> dict[str, pl.DataFrame]:
        print("Running Soliguide staging transformations...")
        self.db.create_schema(self.target_schema)

        raw = self.db.read_query(
            f'SELECT data::text as data FROM "{self.source_schema}"."lieux"'
        )

        lieux = self._transform_lieux(raw)
        print(f"  - Transformed {len(lieux)} lieux")

        services = self._transform_services(raw, lieux)
        print(f"  - Transformed {len(services)} services")

        phones = self._transform_phones(raw, lieux)
        print(f"  - Transformed {len(phones)} phones")

        sources = self._transform_sources(raw, lieux)
        print(f"  - Transformed {len(sources)} sources")

        lieux_publics = self._transform_lieux_publics(raw, lieux)
        print(f"  - Transformed {len(lieux_publics)} lieux_publics")

        tables = {
            "stg_soliguide__lieux": lieux,
            "stg_soliguide__services": services,
            "stg_soliguide__phones": phones,
            "stg_soliguide__sources": sources,
            "stg_soliguide__lieux_publics": lieux_publics,
        }

        for table_name, df in tables.items():
            self.db.write_table(df, self.target_schema, table_name)
            print(f"  - Wrote {table_name}")

        return tables

    def _parse_data(self, data_str) -> dict:
        if isinstance(data_str, dict):
            return data_str
        return json.loads(data_str) if data_str else {}

    def _safe_float(self, val) -> float | None:
        if val is None:
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    def _transform_lieux(self, raw: pl.DataFrame) -> pl.DataFrame:
        all_raw_entries: list = []
        all_lieux_entries: list = []

        for row in raw.to_dicts():
            data = self._parse_data(row.get("data"))
            lieu_id = data.get("lieu_id")
            if lieu_id is None:
                continue

            updated_at = parse_soliguide_timestamp(data.get("updatedAt") or "")
            position = data.get("position") or {}
            country = position.get("country")
            all_raw_entries.append((lieu_id, data))
            all_lieux_entries.append((lieu_id, updated_at, country, data))

        services_by_lieu: dict = {}
        for lieu_id, data in all_raw_entries:
            if lieu_id not in services_by_lieu:
                services_by_lieu[lieu_id] = []
            for svc in data.get("services_all") or []:
                svc_id = svc.get("serviceObjectId")
                if svc_id:
                    services_by_lieu[lieu_id].append(svc_id)

        service_entries: list = []
        for lieu_id, updated_at, country, data in all_lieux_entries:
            for svc_id in services_by_lieu.get(lieu_id, []):
                service_entries.append((lieu_id, updated_at, svc_id))

        max_updated_per_service: dict = {}
        for lieu_id, updated_at, svc_id in service_entries:
            if updated_at is None:
                continue
            if svc_id not in max_updated_per_service:
                max_updated_per_service[svc_id] = updated_at
            elif updated_at > max_updated_per_service[svc_id]:
                max_updated_per_service[svc_id] = updated_at

        duplicates = set()
        for lieu_id, updated_at, svc_id in service_entries:
            if updated_at is None:
                continue
            max_for_service = max_updated_per_service.get(svc_id)
            if max_for_service and updated_at < max_for_service:
                duplicates.add(lieu_id)

        fr_lieux_entries: list = [
            (lid, upd, data)
            for lid, upd, country, data in all_lieux_entries
            if country == "fr"
        ]

        lieu_data: dict = {}
        for lieu_id, updated_at, data in fr_lieux_entries:
            if lieu_id not in lieu_data:
                lieu_data[lieu_id] = []
            lieu_data[lieu_id].append((updated_at, data))

        best_entries: dict = {}
        for lieu_id, entries in lieu_data.items():
            if lieu_id in duplicates:
                continue
            sorted_entries = sorted(
                entries, key=lambda x: x[0] if x[0] else date.min, reverse=True
            )
            best_entries[lieu_id] = sorted_entries[0][1]

        rows = []
        for lieu_id, data in best_entries.items():
            updated_at = parse_soliguide_timestamp(data.get("updatedAt"))
            position = data.get("position") or {}
            modalities = data.get("modalities") or {}
            publics = data.get("publics") or {}
            temp_infos = data.get("tempInfos") or {}
            name = data.get("name") or ""
            name = re.sub(r"\.{2,}$", "…", name)
            name = re.sub(r"(?<!etc)\.$", "", name)
            address = position.get("address") or ""
            if address:
                address = re.sub(r", \d\d\d\d\d.*$", "", address)
                address = address.strip(",").strip() or None
            else:
                address = None

            coords = position.get("location", {}).get("coordinates") or []
            hours_data = temp_infos.get("hours", {}).get("hours")
            newhours = data.get("newhours")

            rows.append(
                {
                    "lieu_id": str(lieu_id),
                    "updated_at": updated_at,
                    "position__coordinates__x": (
                        self._safe_float(coords[0]) if len(coords) > 0 else None
                    ),
                    "position__coordinates__y": (
                        self._safe_float(coords[1]) if len(coords) > 1 else None
                    ),
                    "modalities__inconditionnel": to_bool(
                        modalities.get("inconditionnel")
                    ),
                    "modalities__appointment__checked": to_bool(
                        modalities.get("appointment", {}).get("checked")
                    ),
                    "modalities__inscription__checked": to_bool(
                        modalities.get("inscription", {}).get("checked")
                    ),
                    "modalities__orientation__checked": to_bool(
                        modalities.get("orientation", {}).get("checked")
                    ),
                    "name": name.strip() or None,
                    "position__city": position.get("city"),
                    "position__city_code": position.get("cityCode"),
                    "position__country": position.get("country"),
                    "description": (data.get("description") or "").strip() or None,
                    "seo_url": (data.get("seo_url") or "").strip() or None,
                    "position__postal_code": position.get("postalCode"),
                    "position__address": address,
                    "position__additional_information": (
                        (position.get("additionalInformation") or "").strip() or None
                    ),
                    "position__department": position.get("department"),
                    "position__department_code": position.get("departmentCode"),
                    "publics__accueil": publics.get("accueil"),
                    "publics__age__min": publics.get("age", {}).get("min"),
                    "publics__age__max": publics.get("age", {}).get("max"),
                    "publics__description": (
                        (publics.get("description") or "").strip() or None
                    ),
                    "entity_mail": (
                        (data.get("entity", {}).get("mail") or "").strip() or None
                    ),
                    "entity_website": (
                        (data.get("entity", {}).get("website") or "").strip() or None
                    ),
                    "temp_infos__message__name": (
                        (temp_infos.get("message", {}).get("name") or "").strip()
                        or None
                    ),
                    "temp_infos__closure__actif": to_bool(
                        temp_infos.get("closure", {}).get("actif")
                    ),
                    "temp_infos__hours__actif": to_bool(
                        temp_infos.get("hours", {}).get("actif")
                    ),
                    "temp_infos__hours__hours": (
                        json.dumps(hours_data) if hours_data else None
                    ),
                    "newhours": json.dumps(newhours) if newhours else None,
                    "modalities__other": (
                        (modalities.get("other") or "").strip() or None
                    ),
                    "modalities__price__precisions": (
                        (modalities.get("price", {}).get("precisions") or "").strip()
                        or None
                    ),
                    "modalities__appointment__precisions": (
                        (
                            modalities.get("appointment", {}).get("precisions") or ""
                        ).strip()
                        or None
                    ),
                    "modalities__inscription__precisions": (
                        (
                            modalities.get("inscription", {}).get("precisions") or ""
                        ).strip()
                        or None
                    ),
                    "modalities__orientation__precisions": (
                        (
                            modalities.get("orientation", {}).get("precisions") or ""
                        ).strip()
                        or None
                    ),
                    "modalities__pmr__checked": to_bool(
                        modalities.get("pmr", {}).get("checked")
                    ),
                }
            )

        if not rows:
            return pl.DataFrame(schema=LIEUX_SCHEMA)
        return pl.DataFrame(rows, schema=LIEUX_SCHEMA, strict=False)

    def _transform_services(
        self, raw: pl.DataFrame, lieux: pl.DataFrame
    ) -> pl.DataFrame:
        valid_lieu_ids = set(lieux["lieu_id"].to_list())
        rows = []
        for row in raw.to_dicts():
            data = self._parse_data(row.get("data"))
            lieu_id = str(data.get("lieu_id"))
            if lieu_id not in valid_lieu_ids:
                continue
            for svc in data.get("services_all") or []:
                close = svc.get("close") or {}
                close_date_debut = parse_soliguide_timestamp(close.get("dateDebut"))
                close_date_fin = parse_soliguide_timestamp(close.get("dateFin"))
                modalities = svc.get("modalities") or {}
                publics = svc.get("publics") or {}
                hours = svc.get("hours")
                rows.append(
                    {
                        "lieu_id": lieu_id,
                        "id": svc.get("serviceObjectId"),
                        "category": svc.get("category"),
                        "description": (svc.get("description") or "").strip() or None,
                        "hours": json.dumps(hours) if hours else None,
                        "saturated__status": (
                            (svc.get("saturated", {}).get("status") or "").lower()
                            or None
                        ),
                        "different_hours": to_bool(svc.get("differentHours")),
                        "close__actif": to_bool(close.get("actif")),
                        "close__date_debut": close_date_debut,
                        "close__date_fin": close_date_fin,
                        "modalities__other": (
                            (modalities.get("other") or "").strip() or None
                        ),
                        "modalities__inconditionnel": to_bool(
                            modalities.get("inconditionnel")
                        ),
                        "modalities__appointment__checked": to_bool(
                            modalities.get("appointment", {}).get("checked")
                        ),
                        "modalities__appointment__precisions": (
                            (
                                modalities.get("appointment", {}).get("precisions")
                                or ""
                            ).strip()
                            or None
                        ),
                        "modalities__price__checked": to_bool(
                            modalities.get("price", {}).get("checked")
                        ),
                        "modalities__price__precisions": (
                            (
                                modalities.get("price", {}).get("precisions") or ""
                            ).strip()
                            or None
                        ),
                        "modalities__inscription__checked": to_bool(
                            modalities.get("inscription", {}).get("checked")
                        ),
                        "modalities__inscription__precisions": (
                            (
                                modalities.get("inscription", {}).get("precisions")
                                or ""
                            ).strip()
                            or None
                        ),
                        "modalities__orientation__checked": to_bool(
                            modalities.get("orientation", {}).get("checked")
                        ),
                        "modalities__orientation__precisions": (
                            (
                                modalities.get("orientation", {}).get("precisions")
                                or ""
                            ).strip()
                            or None
                        ),
                        "publics__description": (
                            (publics.get("description") or "").strip() or None
                        ),
                        "different_modalities": to_bool(svc.get("differentModalities")),
                        "different_publics": to_bool(svc.get("differentPublics")),
                    }
                )

        if not rows:
            return pl.DataFrame(schema=SERVICES_SCHEMA)
        return pl.DataFrame(rows, schema=SERVICES_SCHEMA, strict=False)

    def _transform_phones(self, raw: pl.DataFrame, lieux: pl.DataFrame) -> pl.DataFrame:
        valid_lieu_ids = set(lieux["lieu_id"].to_list())
        rows = []
        for row in raw.to_dicts():
            data = self._parse_data(row.get("data"))
            lieu_id = str(data.get("lieu_id"))
            if lieu_id not in valid_lieu_ids:
                continue
            entity = data.get("entity") or {}
            for phone in entity.get("phones") or []:
                rows.append(
                    {
                        "lieu_id": lieu_id,
                        "label": (phone.get("label") or "").strip() or None,
                        "phone_number": (phone.get("phoneNumber") or "").strip()
                        or None,
                    }
                )
        if not rows:
            return pl.DataFrame(schema=PHONES_SCHEMA)
        return pl.DataFrame(rows, schema=PHONES_SCHEMA, strict=False)

    def _transform_sources(
        self, raw: pl.DataFrame, lieux: pl.DataFrame
    ) -> pl.DataFrame:
        valid_lieu_ids = set(lieux["lieu_id"].to_list())
        rows = []
        for row in raw.to_dicts():
            data = self._parse_data(row.get("data"))
            lieu_id = str(data.get("lieu_id"))
            if lieu_id not in valid_lieu_ids:
                continue
            for src in data.get("sources") or []:
                rows.append({"lieu_id": lieu_id, "name": src.get("name")})
        if not rows:
            return pl.DataFrame(schema=SOURCES_SCHEMA)
        return pl.DataFrame(rows, schema=SOURCES_SCHEMA, strict=False)

    def _transform_lieux_publics(
        self, raw: pl.DataFrame, lieux: pl.DataFrame
    ) -> pl.DataFrame:
        valid_lieu_ids = set(lieux["lieu_id"].to_list())
        rows = []
        for row in raw.to_dicts():
            data = self._parse_data(row.get("data"))
            lieu_id = str(data.get("lieu_id"))
            if lieu_id not in valid_lieu_ids:
                continue
            publics = data.get("publics") or {}
            for key in ["administrative", "familialle", "gender", "other"]:
                for val in publics.get(key) or []:
                    if val:
                        rows.append({"lieu_id": lieu_id, "value": val.strip()})
        if not rows:
            return pl.DataFrame(schema=PUBLICS_SCHEMA)
        return pl.DataFrame(rows, schema=PUBLICS_SCHEMA, strict=False)
