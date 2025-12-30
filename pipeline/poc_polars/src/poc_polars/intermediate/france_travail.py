from dataclasses import dataclass
from datetime import date, datetime

import polars as pl

from ..utils.db import DatabaseConnection, TableNotFoundError
from ..utils.opening_hours import france_travail_opening_hours


@dataclass
class FranceTravailIntermediate:
    db: DatabaseConnection
    staging_schema: str = "poc_staging"
    target_schema: str = "poc_intermediate"
    decoupage_schema: str = "public_staging"

    def run(self) -> dict[str, pl.DataFrame]:
        print("Running France Travail intermediate transformations...")
        self.db.create_schema(self.target_schema)

        agences = self.db.read_table(self.staging_schema, "stg_france_travail__agences")
        services = self.db.read_table(
            self.staging_schema, "stg_france_travail__services"
        )
        communes = self._read_communes()

        structures = self._transform_structures(agences)
        adresses = self._transform_adresses(agences, communes)
        services_int = self._transform_services(agences, services, communes)

        print(f"  - Transformed {len(structures)} structures")
        print(f"  - Transformed {len(adresses)} adresses")
        print(f"  - Transformed {len(services_int)} services")

        tables = {
            "int_france_travail__structures_v1": structures,
            "int_france_travail__adresses_v1": adresses,
            "int_france_travail__services_v1": services_int,
        }

        for table_name, df in tables.items():
            self.db.write_table(df, self.target_schema, table_name)
            print(f"  - Wrote {table_name}")

        return tables

    def _read_communes(self) -> pl.DataFrame:
        try:
            query = """
                SELECT code, nom, code_departement, code_region, code_epci
                FROM public_staging.stg_decoupage_administratif__communes
            """
            return self.db.read_query(query)
        except TableNotFoundError as e:
            print(f"  Warning: Could not read communes data: {e}")
            return pl.DataFrame({"code": [], "nom": [], "code_departement": []})

    def _transform_structures(self, agences: pl.DataFrame) -> pl.DataFrame:
        rows = []
        today = date.today()
        for row in agences.to_dicts():
            horaires = france_travail_opening_hours(row.get("horaires"))
            accessibilite = (
                "https://www.francetravail.fr/actualites/a-laffiche/2022/"
                "adeda-un-dispositif-pour-mieux-a.html"
                if row.get("dispositif_adeda")
                else None
            )
            code = row["code"]
            rows.append(
                {
                    "source": "france-travail",
                    "id": f"france-travail--{code}",
                    "adresse_id": f"france-travail--{code}",
                    "nom": row["libelle_etendu"],
                    "date_maj": today,
                    "lien_source": None,
                    "siret": row["siret"],
                    "telephone": "3949",
                    "courriel": None,
                    "site_web": "https://www.francetravail.fr",
                    "description": None,
                    "horaires_accueil": horaires,
                    "accessibilite_lieu": accessibilite,
                    "reseaux_porteurs": ["france-travail"],
                }
            )
        return pl.DataFrame(rows)

    def _transform_adresses(
        self, agences: pl.DataFrame, communes: pl.DataFrame
    ) -> pl.DataFrame:
        communes_dict = {r["code"]: r["nom"] for r in communes.to_dicts()}
        rows = []
        for row in agences.to_dicts():
            code = row["code"]
            code_insee = row["adresse_principale__commune_implantation"]
            rows.append(
                {
                    "source": "france-travail",
                    "id": f"france-travail--{code}",
                    "longitude": row["adresse_principale__gps_lon"],
                    "latitude": row["adresse_principale__gps_lat"],
                    "complement_adresse": row["adresse_principale__ligne_3"],
                    "commune": communes_dict.get(code_insee),
                    "adresse": row["adresse_principale__ligne_4"],
                    "code_postal": row["adresse_principale__bureau_distributeur"],
                    "code_insee": code_insee,
                }
            )
        return pl.DataFrame(rows)

    def _transform_services(
        self, agences: pl.DataFrame, services: pl.DataFrame, communes: pl.DataFrame
    ) -> pl.DataFrame:
        communes_dict = {
            r["code"]: {"nom": r["nom"], "code_departement": r.get("code_departement")}
            for r in communes.to_dicts()
        }
        agences_info = {}
        for row in agences.to_dicts():
            code = row["code"]
            code_insee = row["adresse_principale__commune_implantation"]
            agences_info[code] = {
                "horaires": france_travail_opening_hours(row.get("horaires")),
                "code_departement": communes_dict.get(code_insee, {}).get(
                    "code_departement"
                ),
            }

        rows = []
        for agence in agences.to_dicts():
            agence_code = agence["code"]
            info = agences_info.get(agence_code, {})
            for svc in services.to_dicts():
                svc_id = svc["id"]
                zone = svc.get("zone_eligibilite") or (
                    [info["code_departement"]] if info.get("code_departement") else None
                )
                horaires = svc.get("horaires_accueil") or info.get("horaires")
                date_maj = svc.get("date_maj")
                if isinstance(date_maj, str):
                    try:
                        date_maj = datetime.strptime(date_maj, "%Y-%m-%d").date()
                    except Exception:
                        date_maj = None
                rows.append(
                    {
                        "source": "france-travail",
                        "structure_id": f"france-travail--{agence_code}",
                        "adresse_id": f"france-travail--{agence_code}",
                        "id": f"france-travail--{agence_code}-{svc_id}",
                        "courriel": svc.get("courriel"),
                        "contact_nom_prenom": svc.get("contact_nom_prenom"),
                        "date_maj": date_maj,
                        "nom": svc.get("nom"),
                        "description": svc.get("description"),
                        "lien_source": svc.get("lien_source"),
                        "type": svc.get("type"),
                        "thematiques": svc.get("thematiques"),
                        "frais": svc.get("frais"),
                        "frais_precisions": svc.get("frais_precisions"),
                        "publics": svc.get("publics"),
                        "publics_precisions": svc.get("publics_precisions"),
                        "conditions_acces": svc.get("conditions_acces"),
                        "telephone": svc.get("telephone"),
                        "modes_mobilisation": svc.get("modes_mobilisation"),
                        "lien_mobilisation": svc.get("lien_mobilisation"),
                        "mobilisable_par": svc.get("mobilisable_par"),
                        "mobilisation_precisions": svc.get("mobilisation_precisions"),
                        "modes_accueil": svc.get("modes_accueil"),
                        "zone_eligibilite": zone,
                        "volume_horaire_hebdomadaire": svc.get(
                            "volume_horaire_hebdomadaire"
                        ),
                        "nombre_semaines": svc.get("nombre_semaines"),
                        "horaires_accueil": horaires,
                    }
                )
        return pl.DataFrame(rows)
