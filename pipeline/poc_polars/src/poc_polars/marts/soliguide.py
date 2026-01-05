from dataclasses import dataclass

import polars as pl

from ..utils.db import DatabaseConnection
from ..utils.formatters import format_description, format_name
from ..utils.json_utils import parse_json_array
from ..utils.validation import validate_dataframe

STRUCTURE_FIELDS = [
    "id",
    "adresse_id",
    "siret",
    "nom",
    "commune",
    "code_postal",
    "code_insee",
    "adresse",
    "complement_adresse",
    "longitude",
    "latitude",
    "telephone",
    "courriel",
    "site_web",
    "description",
    "source",
    "date_maj",
    "lien_source",
    "horaires_accueil",
    "accessibilite_lieu",
    "reseaux_porteurs",
    "_cluster_id",
    "_has_pii",
    "_email_is_bad",
    "_siret_status",
    "_in_opendata",
    "_is_valid",
    "_is_closed",
]

SERVICE_FIELDS = [
    "id",
    "adresse_id",
    "source",
    "structure_id",
    "nom",
    "description",
    "type",
    "thematiques",
    "frais",
    "frais_precisions",
    "publics",
    "publics_precisions",
    "conditions_acces",
    "commune",
    "code_postal",
    "code_insee",
    "adresse",
    "complement_adresse",
    "longitude",
    "latitude",
    "horaires_accueil",
    "lien_source",
    "telephone",
    "courriel",
    "contact_nom_prenom",
    "date_maj",
    "lien_mobilisation",
    "mobilisable_par",
    "mobilisation_precisions",
    "modes_accueil",
    "modes_mobilisation",
    "zone_eligibilite",
    "volume_horaire_hebdomadaire",
    "nombre_semaines",
    "score_qualite",
    "_has_pii",
    "_email_is_bad",
    "_in_opendata",
    "_is_valid",
]


@dataclass
class SoliguideMarts:
    db: DatabaseConnection
    intermediate_schema: str = "poc_intermediate"
    target_schema: str = "poc_marts"

    def run_with_data(
        self,
        enriched_structures: pl.DataFrame,
        enriched_services: pl.DataFrame,
    ) -> tuple[dict[str, pl.DataFrame], pl.DataFrame, pl.DataFrame]:
        print("Running Soliguide marts with enriched data...")
        self.db.create_schema(self.target_schema)

        adresses = self.db.read_table(
            self.intermediate_schema, "int_soliguide__adresses_v1"
        )
        adresses_dict = {r["id"]: r for r in adresses.to_dicts()}

        structures, rej_str = self._build_structures(enriched_structures, adresses_dict)
        services, rej_svc = self._build_services(enriched_services, adresses_dict)

        print(f"  - {len(structures)} structures ({len(rej_str)} rejected)")
        print(f"  - {len(services)} services ({len(rej_svc)} rejected)")

        for table, df in [
            ("marts__structures_v1", structures),
            ("marts__services_v1", services),
        ]:
            self.db.write_table(df, self.target_schema, table, if_table_exists="append")
            print(f"  - Appended to {table}")

        return (
            {"marts__structures_v1": structures, "marts__services_v1": services},
            rej_str,
            rej_svc,
        )

    def _build_structures(
        self, enriched: pl.DataFrame, adresses: dict
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        rows = []
        for row in enriched.to_dicts():
            addr = adresses.get(row.get("adresse_id"), {})
            out = {
                **{k: row.get(k) for k in STRUCTURE_FIELDS},
                **{
                    k: addr.get(k)
                    for k in [
                        "commune",
                        "code_postal",
                        "code_insee",
                        "adresse",
                        "complement_adresse",
                        "longitude",
                        "latitude",
                    ]
                },
                "nom": format_name(row.get("nom")),
                "description": format_description(row.get("description")),
                "reseaux_porteurs": parse_json_array(row.get("reseaux_porteurs")),
                "_has_pii": row.get("_has_pii", False),
                "_email_is_bad": row.get("_email_is_bad", False),
                "_in_opendata": True,
                "_is_valid": True,
                "_is_closed": row.get("_is_closed", False),
            }
            rows.append(out)

        return validate_dataframe(
            pl.DataFrame(rows, infer_schema_length=None), "structure"
        )

    def _build_services(
        self, enriched: pl.DataFrame, adresses: dict
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        rows = []
        for row in enriched.to_dicts():
            addr = adresses.get(row.get("adresse_id"), {})
            out = {
                **{k: row.get(k) for k in SERVICE_FIELDS},
                **{
                    k: addr.get(k)
                    for k in [
                        "commune",
                        "code_postal",
                        "code_insee",
                        "adresse",
                        "complement_adresse",
                        "longitude",
                        "latitude",
                    ]
                },
                "nom": format_name(row.get("nom")),
                "description": format_description(row.get("description")),
                "thematiques": parse_json_array(row.get("thematiques")),
                "publics": parse_json_array(row.get("publics")),
                "mobilisable_par": parse_json_array(row.get("mobilisable_par")),
                "modes_accueil": parse_json_array(row.get("modes_accueil")),
                "modes_mobilisation": parse_json_array(row.get("modes_mobilisation")),
                "zone_eligibilite": parse_json_array(row.get("zone_eligibilite")),
                "score_qualite": None,
                "_has_pii": row.get("_has_pii", False),
                "_email_is_bad": row.get("_email_is_bad", False),
                "_in_opendata": True,
                "_is_valid": True,
            }
            rows.append(out)

        return validate_dataframe(
            pl.DataFrame(rows, infer_schema_length=None), "service"
        )
