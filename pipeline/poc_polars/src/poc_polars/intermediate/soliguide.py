from dataclasses import dataclass
from datetime import date

import polars as pl

from ..utils.db import DatabaseConnection
from ..utils.soliguide_opening_hours import soliguide_opening_hours


def format_date_fr(d: date | None) -> str | None:
    if d is None:
        return None
    return d.strftime("%d/%m/%Y")


THEMATIQUES_MAP = {
    "access_to_housing": [
        "logement-hebergement--acheter-un-logement",
        "logement-hebergement--louer-un-logement",
    ],
    "addiction": ["sante--addictions"],
    "administrative_assistance": [
        "difficultes-administratives-ou-juridiques"
        "--accompagnement-aux-demarches-administratives"
    ],
    "baby_parcel": ["famille--soutien-a-la-parentalite-et-a-leducation"],
    "babysitting": ["famille--garde-denfants"],
    "budget_advice": ["difficultes-financieres--ameliorer-sa-gestion-budgetaire"],
    "carpooling": ["mobilite--mobilite-douce-partagee-collective"],
    "chauffeur_driven_transport": [
        "mobilite--etre-accompagne-dans-son-parcours-mobilite"
    ],
    "child_care": ["famille--garde-denfants"],
    "citizen_housing": [
        "logement-hebergement--rechercher-une-solution-dhebergement-temporaire"
    ],
    "clothing": ["equipement-et-alimentation--habillement"],
    "community_garden": ["remobilisation--activites-sportives-et-culturelles"],
    "computers_at_your_disposal": [
        "numerique--acceder-a-des-services-en-ligne",
        "numerique--acceder-a-une-connexion-internet",
    ],
    "cooking_workshop": ["remobilisation--activites-sportives-et-culturelles"],
    "day_hosting": [
        "logement-hebergement--rechercher-une-solution-dhebergement-temporaire"
    ],
    "dental_care": ["sante--acces-aux-soins"],
    "digital_tools_training": ["numerique--maitriser-les-fondamentaux-du-numerique"],
    "disability_advice": [
        "difficultes-administratives-ou-juridiques"
        "--accompagnement-pour-lacces-aux-droits"
    ],
    "domiciliation": [
        "difficultes-administratives-ou-juridiques"
        "--accompagnement-aux-demarches-administratives"
    ],
    "emergency_accommodation": [
        "logement-hebergement--rechercher-une-solution-dhebergement-temporaire"
    ],
    "family_area": [
        "logement-hebergement--rechercher-une-solution-dhebergement-temporaire"
    ],
    "food_distribution": ["equipement-et-alimentation--alimentation"],
    "food_packages": ["equipement-et-alimentation--alimentation"],
    "food_voucher": ["equipement-et-alimentation--alimentation"],
    "french_course": ["lecture-ecriture-calcul--maitriser-le-francais"],
    "general_practitioner": ["sante--acces-aux-soins"],
    "health_specialists": ["sante--acces-aux-soins"],
    "infirmary": ["sante--acces-aux-soins"],
    "information_point": [
        "logement-hebergement--rechercher-une-solution-dhebergement-temporaire"
    ],
    "integration_through_economic_activity": [
        "preparer-sa-candidature--organiser-ses-demarches-de-recherche-demploi"
    ],
    "job_coaching": [
        "preparer-sa-candidature--organiser-ses-demarches-de-recherche-demploi"
    ],
    "legal_advice": [
        "difficultes-administratives-ou-juridiques"
        "--prendre-en-compte-une-problematique-judiciaire"
    ],
    "libraries": ["remobilisation--activites-sportives-et-culturelles"],
    "long_term_accomodation": [
        "logement-hebergement--acheter-un-logement",
        "logement-hebergement--louer-un-logement",
    ],
    "mobility_assistance": ["mobilite--etre-accompagne-dans-son-parcours-mobilite"],
    "museums": ["remobilisation--activites-sportives-et-culturelles"],
    "other_activities": ["remobilisation--activites-sportives-et-culturelles"],
    "overnight_stop": [
        "logement-hebergement--rechercher-une-solution-dhebergement-temporaire"
    ],
    "parent_assistance": ["famille--soutien-a-la-parentalite-et-a-leducation"],
    "pregnancy_care": ["sante--sante-sexuelle"],
    "provision_of_vehicles": ["mobilite--acceder-a-un-vehicule"],
    "psychological_support": ["sante--sante-mentale"],
    "public_writer": [
        "difficultes-administratives-ou-juridiques"
        "--accompagnement-aux-demarches-administratives"
    ],
    "rest_area": [
        "logement-hebergement--rechercher-une-solution-dhebergement-temporaire"
    ],
    "seated_catering": ["equipement-et-alimentation--alimentation"],
    "shared_kitchen": ["equipement-et-alimentation--alimentation"],
    "social_accompaniment": [
        "preparer-sa-candidature--organiser-ses-demarches-de-recherche-demploi"
    ],
    "social_grocery_stores": ["equipement-et-alimentation--alimentation"],
    "solidarity_fridge": ["equipement-et-alimentation--alimentation"],
    "solidarity_store": [
        "equipement-et-alimentation--alimentation",
        "equipement-et-alimentation--electromenager",
        "equipement-et-alimentation--habillement",
    ],
    "sport_activities": ["remobilisation--activites-sportives-et-culturelles"],
    "std_testing": ["sante--sante-sexuelle"],
    "tutoring": ["choisir-un-metier--confirmer-son-choix-de-metier"],
    "vaccination": ["sante--acces-aux-soins"],
    "wellness": ["remobilisation--bien-etre-confiance-en-soi"],
    "wifi": ["numerique--acceder-a-des-services-en-ligne"],
}

TYPES_MAP = {
    "access_to_housing": "accompagnement",
    "addiction": "accompagnement",
    "administrative_assistance": "accompagnement",
    "baby_parcel": "aide-materielle",
    "babysitting": "aide-materielle",
    "budget_advice": "accompagnement",
    "carpooling": "aide-materielle",
    "chauffeur_driven_transport": "aide-materielle",
    "child_care": "accompagnement",
    "citizen_housing": "accompagnement",
    "clothing": "aide-materielle",
    "community_garden": "atelier",
    "computers_at_your_disposal": "aide-materielle",
    "cooking_workshop": "atelier",
    "day_hosting": "aide-materielle",
    "dental_care": "accompagnement",
    "digital_tools_training": "formation",
    "disability_advice": "accompagnement",
    "domiciliation": "accompagnement",
    "emergency_accommodation": "aide-materielle",
    "family_area": "aide-materielle",
    "food_distribution": "aide-materielle",
    "food_packages": "aide-materielle",
    "food_voucher": "aide-financiere",
    "french_course": "formation",
    "general_practitioner": "accompagnement",
    "health_specialists": "accompagnement",
    "infirmary": "accompagnement",
    "information_point": "information",
    "integration_through_economic_activity": "accompagnement",
    "job_coaching": "accompagnement",
    "legal_advice": "accompagnement",
    "libraries": "aide-materielle",
    "long_term_accomodation": "accompagnement",
    "mobility_assistance": "accompagnement",
    "museums": "atelier",
    "other_activities": "atelier",
    "overnight_stop": "aide-materielle",
    "parent_assistance": "accompagnement",
    "pregnancy_care": "accompagnement",
    "provision_of_vehicles": "aide-materielle",
    "psychological_support": "accompagnement",
    "public_writer": "accompagnement",
    "rest_area": "aide-materielle",
    "seated_catering": "aide-materielle",
    "shared_kitchen": "aide-materielle",
    "social_accompaniment": "accompagnement",
    "social_grocery_stores": "aide-materielle",
    "solidarity_fridge": "aide-materielle",
    "solidarity_store": "aide-materielle",
    "sport_activities": "atelier",
    "std_testing": "accompagnement",
    "tutoring": "accompagnement",
    "vaccination": "accompagnement",
    "wellness": "atelier",
    "wifi": "aide-materielle",
}

PUBLICS_MAP = {
    "addiction": ["personnes-en-situation-durgence"],
    "asylum": ["personnes-exilees"],
    "couple": ["familles"],
    "family": ["familles"],
    "handicap": ["personnes-en-situation-de-handicap"],
    "isolated": ["personnes-en-situation-durgence", "seniors"],
    "pregnant": ["femmes", "familles"],
    "prison": ["personnes-en-situation-juridique-specifique"],
    "refugee": ["personnes-exilees"],
    "student": ["etudiants"],
    "undocumented": ["personnes-exilees"],
    "violence": ["personnes-en-situation-durgence"],
    "women": ["femmes"],
    "tous-publics": ["tous-publics"],
}

RESEAUX_MAP = {
    "Conseil Départemental du 59": "departements",
    "Croix-Rouge française": "croix-rouge",
    "restos": "restos-du-coeur",
}

CATEGORIES_MAP = {
    "access_to_housing": "Conseil logement",
    "addiction": "Addiction",
    "administrative_assistance": "Conseil administratif",
    "baby_parcel": "Colis bébé",
    "babysitting": "Garde d'enfants",
    "budget_advice": "Conseil budget",
    "carpooling": "Co-voiturage",
    "chauffeur_driven_transport": "Transport avec chauffeur",
    "child_care": "Soins enfants",
    "citizen_housing": "Hébergement citoyen",
    "clothing": "Vêtements",
    "community_garden": "Jardin solidaire",
    "computers_at_your_disposal": "Ordinateur",
    "cooking_workshop": "Atelier cuisine",
    "day_hosting": "Accueil de jour",
    "dental_care": "Dentaire",
    "digital_tools_training": "Atelier numérique",
    "disability_advice": "Conseil handicap",
    "domiciliation": "Domiciliation",
    "emergency_accommodation": "Hébergement d'urgence",
    "family_area": "Espace famille",
    "food_distribution": "Distribution de repas",
    "food_packages": "Panier alimentaire",
    "food_voucher": "Bon / chèque alimentaire",
    "french_course": "Cours de français",
    "general_practitioner": "Médecin généraliste",
    "health_specialists": "Spécialistes",
    "infirmary": "Infirmerie",
    "information_point": "Point d'information et d'orientation",
    "integration_through_economic_activity": "Insertion par l'activité économique",
    "job_coaching": "Accompagnement à l'emploi",
    "legal_advice": "Permanence juridique",
    "libraries": "Bibliothèque",
    "long_term_accomodation": "Hébergement à long terme",
    "mobility_assistance": "Aide à la mobilité",
    "museums": "Musée",
    "other_activities": "Activités diverses",
    "overnight_stop": "Halte de nuit",
    "parent_assistance": "Conseil aux parents",
    "pregnancy_care": "Suivi grossesse",
    "provision_of_vehicles": "Mise à disposition de véhicule",
    "psychological_support": "Psychologie",
    "public_writer": "Écrivain public",
    "rest_area": "Espace de repos",
    "seated_catering": "Restauration assise",
    "shared_kitchen": "Cuisine partagée",
    "social_accompaniment": "Accompagnement social",
    "social_grocery_stores": "Épicerie Sociale et Solidaire",
    "solidarity_fridge": "Frigo solidaire",
    "solidarity_store": "Boutique solidaire",
    "sport_activities": "Activités sportives",
    "std_testing": "Dépistage",
    "tutoring": "Soutien scolaire",
    "vaccination": "Vaccination",
    "wellness": "Bien-être",
    "wifi": "Wifi",
}


@dataclass
class SoliguideIntermediate:
    db: DatabaseConnection
    staging_schema: str = "poc_staging"
    target_schema: str = "poc_intermediate"

    def run(self) -> dict[str, pl.DataFrame]:
        print("Running Soliguide intermediate transformations...")
        self.db.create_schema(self.target_schema)

        lieux = self.db.read_table(self.staging_schema, "stg_soliguide__lieux")
        services = self.db.read_table(self.staging_schema, "stg_soliguide__services")
        phones = self.db.read_table(self.staging_schema, "stg_soliguide__phones")
        sources = self.db.read_table(self.staging_schema, "stg_soliguide__sources")
        lieux_publics = self.db.read_table(
            self.staging_schema, "stg_soliguide__lieux_publics"
        )

        structures = self._transform_structures(lieux, phones, sources)
        adresses = self._transform_adresses(lieux)
        services_int = self._transform_services(lieux, services, phones, lieux_publics)

        print(f"  - Transformed {len(structures)} structures")
        print(f"  - Transformed {len(adresses)} adresses")
        print(f"  - Transformed {len(services_int)} services")

        tables = {
            "int_soliguide__structures_v1": structures,
            "int_soliguide__adresses_v1": adresses,
            "int_soliguide__services_v1": services_int,
        }

        for table_name, df in tables.items():
            self.db.write_table(df, self.target_schema, table_name)
            print(f"  - Wrote {table_name}")

        return tables

    def _get_phone(self, lieu_id: str, phones_dict: dict) -> str | None:
        phones_list = phones_dict.get(lieu_id, [])
        return phones_list[0] if phones_list else None

    def _get_reseaux(self, lieu_id: str, sources_dict: dict) -> list | None:
        source_names = sources_dict.get(lieu_id, [])
        reseaux = []
        for name in source_names:
            if name in RESEAUX_MAP and RESEAUX_MAP[name]:
                reseaux.append(RESEAUX_MAP[name])
        return list(set(reseaux)) if reseaux else None

    def _transform_structures(
        self, lieux: pl.DataFrame, phones: pl.DataFrame, sources: pl.DataFrame
    ) -> pl.DataFrame:
        phones_dict = {}
        for row in phones.to_dicts():
            lid = row["lieu_id"]
            if lid not in phones_dict:
                phones_dict[lid] = []
            if row.get("phone_number"):
                phones_dict[lid].append(row["phone_number"])

        sources_dict = {}
        for row in sources.to_dicts():
            lid = row["lieu_id"]
            if lid not in sources_dict:
                sources_dict[lid] = []
            if row.get("name"):
                sources_dict[lid].append(row["name"])

        rows = []
        for row in lieux.to_dicts():
            lieu_id = row["lieu_id"]
            temp_msg = row.get("temp_infos__message__name")
            desc = row.get("description")
            full_desc = (
                "\n\n".join(
                    filter(
                        None,
                        [
                            f"Information temporaire : {temp_msg}"
                            if temp_msg
                            else None,
                            desc,
                        ],
                    )
                )
                or None
            )

            horaires = self._get_horaires(row)

            rows.append(
                {
                    "source": "soliguide",
                    "id": f"soliguide--{lieu_id}",
                    "adresse_id": f"soliguide--{lieu_id}",
                    "nom": row.get("name"),
                    "date_maj": row.get("updated_at"),
                    "lien_source": (
                        f"https://soliguide.fr/fr/fiche/{row['seo_url']}"
                        if row.get("seo_url")
                        else None
                    ),
                    "siret": None,
                    "telephone": self._get_phone(lieu_id, phones_dict),
                    "courriel": row.get("entity_mail"),
                    "site_web": row.get("entity_website"),
                    "description": full_desc,
                    "horaires_accueil": horaires,
                    "accessibilite_lieu": None,
                    "reseaux_porteurs": self._get_reseaux(lieu_id, sources_dict),
                }
            )

        return pl.DataFrame(rows)

    def _get_horaires(self, row: dict) -> str | None:
        if row.get("temp_infos__closure__actif"):
            return 'closed "fermeture temporaire"'
        hours_actif = row.get("temp_infos__hours__actif")
        hours_data = row.get("temp_infos__hours__hours")
        if hours_actif and hours_data:
            return soliguide_opening_hours(hours_data)
        if row.get("newhours"):
            return soliguide_opening_hours(row["newhours"])
        return None

    def _transform_adresses(self, lieux: pl.DataFrame) -> pl.DataFrame:
        rows = []
        for row in lieux.to_dicts():
            lieu_id = row["lieu_id"]
            rows.append(
                {
                    "source": "soliguide",
                    "id": f"soliguide--{lieu_id}",
                    "longitude": row.get("position__coordinates__x"),
                    "latitude": row.get("position__coordinates__y"),
                    "complement_adresse": row.get("position__additional_information"),
                    "adresse": row.get("position__address"),
                    "commune": row.get("position__city"),
                    "code_postal": row.get("position__postal_code"),
                    "code_insee": None,
                }
            )
        return pl.DataFrame(rows)

    def _get_publics(
        self, lieu_id: str, lieux_publics_dict: dict, publics_accueil: int | None
    ) -> list | None:
        if publics_accueil in (0, 1):
            return ["tous-publics"]
        public_values = lieux_publics_dict.get(lieu_id, [])
        publics = set()
        for val in public_values:
            if val in PUBLICS_MAP:
                publics.update(PUBLICS_MAP[val])
        return list(publics) if publics else None

    def _transform_services(
        self,
        lieux: pl.DataFrame,
        services: pl.DataFrame,
        phones: pl.DataFrame,
        lieux_publics: pl.DataFrame,
    ) -> pl.DataFrame:
        lieux_dict = {r["lieu_id"]: r for r in lieux.to_dicts()}

        phones_dict = {}
        for row in phones.to_dicts():
            lid = row["lieu_id"]
            if lid not in phones_dict:
                phones_dict[lid] = []
            if row.get("phone_number"):
                phones_dict[lid].append(row["phone_number"])

        lieux_publics_dict = {}
        for row in lieux_publics.to_dicts():
            lid = row["lieu_id"]
            if lid not in lieux_publics_dict:
                lieux_publics_dict[lid] = []
            if row.get("value"):
                lieux_publics_dict[lid].append(row["value"])

        rows = []
        for svc in services.to_dicts():
            lieu_id = svc["lieu_id"]
            lieu = lieux_dict.get(lieu_id, {})
            category = svc.get("category")

            thematiques = THEMATIQUES_MAP.get(category)
            if not thematiques:
                continue

            service_type = TYPES_MAP.get(category)
            nom = CATEGORIES_MAP.get(category, category)
            full_desc = self._build_service_description(svc, lieu)
            publics = self._get_publics(
                lieu_id, lieux_publics_dict, lieu.get("publics__accueil")
            )
            publics_prec = self._build_publics_precisions(svc, lieu)
            horaires = self._get_service_horaires(svc, lieu)
            modes_mob = self._get_modes_mobilisation(svc, lieu)
            mob_prec = self._get_mobilisation_precisions(svc, lieu)

            is_payant = svc.get("modalities__price__checked")

            rows.append(
                {
                    "source": "soliguide",
                    "id": f"soliguide--{svc['id']}",
                    "adresse_id": f"soliguide--{lieu_id}",
                    "structure_id": f"soliguide--{lieu_id}",
                    "nom": nom,
                    "description": full_desc,
                    "lien_source": (
                        f"https://soliguide.fr/fr/fiche/{lieu['seo_url']}"
                        if lieu.get("seo_url")
                        else None
                    ),
                    "date_maj": lieu.get("updated_at"),
                    "type": service_type,
                    "thematiques": thematiques,
                    "frais": "payant" if is_payant else "gratuit",
                    "frais_precisions": (
                        svc.get("modalities__price__precisions")
                        or lieu.get("modalities__price__precisions")
                    ),
                    "publics": publics,
                    "publics_precisions": publics_prec,
                    "conditions_acces": (
                        svc.get("modalities__other") or lieu.get("modalities__other")
                    ),
                    "telephone": self._get_phone(lieu_id, phones_dict),
                    "courriel": lieu.get("entity_mail"),
                    "contact_nom_prenom": None,
                    "modes_accueil": ["en-presentiel"],
                    "zone_eligibilite": None,
                    "lien_mobilisation": None,
                    "modes_mobilisation": modes_mob,
                    "mobilisable_par": ["usagers", "professionnels"],
                    "mobilisation_precisions": mob_prec,
                    "volume_horaire_hebdomadaire": None,
                    "nombre_semaines": None,
                    "horaires_accueil": horaires,
                }
            )

        return pl.DataFrame(rows)

    def _build_service_description(self, svc: dict, lieu: dict) -> str | None:
        close_msg = None
        if svc.get("close__actif"):
            date_debut_str = format_date_fr(svc.get("close__date_debut"))
            date_fin_str = format_date_fr(svc.get("close__date_fin"))
            if date_fin_str:
                close_msg = (
                    f"Ce service est fermé temporairement du "
                    f"{date_debut_str} au {date_fin_str}."
                )
            elif date_debut_str:
                close_msg = (
                    f"Ce service est fermé temporairement depuis le {date_debut_str}."
                )
        saturated_msg = None
        if svc.get("saturated__status") == "high":
            saturated_msg = (
                "Attention, la structure est très sollicitée pour ce service."
            )
        desc_parts = [
            svc.get("description"),
            close_msg,
            saturated_msg,
            lieu.get("description"),
        ]
        return "\n\n".join(filter(None, desc_parts)) or None

    def _build_publics_precisions(self, svc: dict, lieu: dict) -> str | None:
        age_min = lieu.get("publics__age__min")
        age_max = lieu.get("publics__age__max")
        age_min_msg = f"L'âge minimum est de {age_min} ans." if age_min else None
        age_max_msg = f"L'âge maximum est de {age_max} ans." if age_max else None
        return (
            "\n\n".join(
                filter(
                    None,
                    [
                        svc.get("publics__description")
                        or lieu.get("publics__description"),
                        age_min_msg,
                        age_max_msg,
                    ],
                )
            )
            or None
        )

    def _get_service_horaires(self, svc: dict, lieu: dict) -> str | None:
        if lieu.get("temp_infos__closure__actif"):
            return 'closed "fermeture temporaire"'
        if svc.get("different_hours") and svc.get("hours"):
            return soliguide_opening_hours(svc["hours"])
        hours_actif = lieu.get("temp_infos__hours__actif")
        hours_data = lieu.get("temp_infos__hours__hours")
        if hours_actif and hours_data:
            return soliguide_opening_hours(hours_data)
        if lieu.get("newhours"):
            return soliguide_opening_hours(lieu["newhours"])
        return None

    def _get_modes_mobilisation(self, svc: dict, lieu: dict) -> list:
        modes_mob = ["envoyer-un-courriel", "telephoner"]
        diff_mod = svc.get("different_modalities")
        if diff_mod and svc.get("modalities__inconditionnel"):
            modes_mob.append("se-presenter")
        elif lieu.get("modalities__inconditionnel"):
            modes_mob.append("se-presenter")
        return modes_mob

    def _get_mobilisation_precisions(self, svc: dict, lieu: dict) -> str | None:
        diff_mod = svc.get("different_modalities")
        if diff_mod:
            parts = [
                svc.get("modalities__appointment__precisions"),
                svc.get("modalities__inscri_precisions"),
                svc.get("modalities__orientation__precisions"),
            ]
        else:
            parts = [
                lieu.get("modalities__appointment__precisions"),
                lieu.get("modalities__inscription__precisions"),
                lieu.get("modalities__orientation__precisions"),
            ]
        return "\n\n".join(filter(None, parts)) or None
