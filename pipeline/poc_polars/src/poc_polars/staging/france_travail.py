import json
from dataclasses import dataclass

import polars as pl

from ..utils.db import DatabaseConnection


@dataclass
class FranceTravailStaging:
    db: DatabaseConnection
    source_schema: str = "france_travail"
    target_schema: str = "poc_staging"

    def run(self) -> dict[str, pl.DataFrame]:
        print("Running France Travail staging transformations...")
        self.db.create_schema(self.target_schema)

        agences = self._transform_agences()
        print(f"  - Transformed {len(agences)} agences")

        services = self._transform_services()
        print(f"  - Transformed {len(services)} services")

        tables = {
            "stg_france_travail__agences": agences,
            "stg_france_travail__services": services,
        }

        for table_name, df in tables.items():
            self.db.write_table(df, self.target_schema, table_name)
            print(f"  - Wrote {table_name}")

        return tables

    def _transform_agences(self) -> pl.DataFrame:
        raw = self.db.read_table(self.source_schema, "agences")

        def parse_agence(data_str):
            data = json.loads(data_str) if isinstance(data_str, str) else data_str
            adresse = data.get("adressePrincipale", {})
            contact = data.get("contact", {})
            horaires = data.get("horaires")
            if horaires is not None and not isinstance(horaires, str):
                horaires = json.dumps(horaires)
            return {
                "dispositif_adeda": data.get("dispositifADEDA", False),
                "adresse_principale__gps_lat": (
                    float(adresse.get("gpsLat", 0)) if adresse.get("gpsLat") else None
                ),
                "adresse_principale__gps_lon": (
                    float(adresse.get("gpsLon", 0)) if adresse.get("gpsLon") else None
                ),
                "adresse_principale__ligne_4": (
                    (adresse.get("ligne4") or "").strip() or None
                ),
                "adresse_principale__ligne_3": (
                    (adresse.get("ligne3") or "").strip() or None
                ),
                "adresse_principale__commune_implantation": adresse.get(
                    "communeImplantation"
                ),
                "adresse_principale__bureau_distributeur": adresse.get(
                    "bureauDistributeur"
                ),
                "contact__email": (contact.get("email") or "").strip() or None,
                "contact__telephone_public": (
                    (contact.get("telephonePublic") or "").strip() or None
                ),
                "horaires": horaires,
                "code": data.get("code"),
                "libelle_etendu": (data.get("libelleEtendu") or "").strip() or None,
                "siret": (data.get("siret") or "").strip() or None,
                "type": data.get("type"),
            }

        return pl.DataFrame([parse_agence(row["data"]) for row in raw.to_dicts()])

    def _transform_services(self) -> pl.DataFrame:
        raw = self.db.read_table(self.source_schema, "services")

        def parse_array(value):
            return (
                [v.strip() for v in value.split(", ") if v.strip()] if value else None
            )

        def parse_service(data_str):
            data = json.loads(data_str) if isinstance(data_str, str) else data_str
            if data.get("__ignore__", False):
                return None
            date_maj = data.get("date_maj")
            if date_maj:
                try:
                    parts = date_maj.split("/")
                    if len(parts) == 3:
                        date_maj = f"{parts[2]}-{parts[1]}-{parts[0]}"
                except Exception:
                    date_maj = None
            return {
                "id": data.get("id"),
                "date_maj": date_maj,
                "nom": data.get("nom"),
                "description": data.get("description"),
                "lien_source": data.get("lien_source"),
                "type": data.get("type"),
                "thematiques": parse_array(data.get("thematiques")),
                "frais": data.get("frais"),
                "frais_precisions": data.get("frais_precisions"),
                "publics": parse_array(data.get("publics")),
                "publics_precisions": data.get("publics_precisions"),
                "conditions_acces": data.get("conditions_acces"),
                "telephone": data.get("telephone"),
                "courriel": data.get("courriel"),
                "contact_nom_prenom": (
                    (data.get("contact_nom_prenom") or "").strip() or None
                ),
                "modes_accueil": parse_array(data.get("modes_accueil")),
                "modes_mobilisation": parse_array(data.get("modes_mobilisation")),
                "lien_mobilisation": data.get("lien_mobilisation"),
                "mobilisation_precisions": data.get("mobilisation_precisions"),
                "mobilisable_par": parse_array(data.get("mobilisable_par")),
                "zone_eligibilite": parse_array(data.get("zone_eligibilite")),
                "volume_horaire_hebdomadaire": (
                    float(data.get("volume_horaire_hebdomadaire"))
                    if data.get("volume_horaire_hebdomadaire")
                    else None
                ),
                "nombre_semaines": (
                    int(data.get("nombre_semaines"))
                    if data.get("nombre_semaines")
                    else None
                ),
                "horaires_accueil": data.get("horaires_accueil"),
            }

        rows = [r for r in (parse_service(row["data"]) for row in raw.to_dicts()) if r]
        return pl.DataFrame(rows)
