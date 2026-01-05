import re
from dataclasses import dataclass

import polars as pl

from ..utils.db import DatabaseConnection, TableNotFoundError

PERSONAL_EMAIL_PROVIDERS = [
    "free",
    "gmail",
    "hotmail",
    "laposte",
    "nordnet",
    "orange",
    "outlook",
    "sfr",
    "wanadoo",
    "yahoo",
]

NON_PERSONAL_PATTERNS = re.compile(r"mairie|commune|adil|services|ccas", re.IGNORECASE)


def _get_domain_2nd_level(email: str) -> str | None:
    try:
        domain = email.split("@")[1]
        parts = domain.split(".")
        if len(parts) >= 2:
            return parts[-2].lower()
    except (IndexError, AttributeError):
        pass
    return None


def _get_local_part(email: str) -> str | None:
    try:
        return email.split("@")[0].lower()
    except (IndexError, AttributeError):
        return None


def _normalize_email(email: str) -> str:
    """Normalize email like DBT: RTRIM(UNACCENT(LOWER(email)), '.')"""
    import unicodedata

    if not email:
        return ""
    email = email.lower().strip()
    email = unicodedata.normalize("NFD", email)
    email = "".join(c for c in email if unicodedata.category(c) != "Mn")
    return email.rstrip(".")


@dataclass
class EmailChecker:
    db: DatabaseConnection

    def run(
        self,
        structures: pl.DataFrame,
        services: pl.DataFrame,
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        print("  Checking emails...")

        prenoms = self._load_prenoms()
        personal_emails = self._identify_personal_emails(structures, services, prenoms)
        print(f"    - Found {len(personal_emails)} personal emails")

        bad_emails = self._load_brevo_bad_emails()
        print(f"    - Found {len(bad_emails)} bad emails from Brevo")

        structures = self._mark_pii_and_bad(structures, personal_emails, bad_emails)
        services = self._mark_pii_and_bad(services, personal_emails, bad_emails)

        return structures, services

    def _load_prenoms(self) -> set[str]:
        try:
            query = """
                SELECT DISTINCT LOWER(prenom) as prenom
                FROM public_staging.stg_etat_civil__prenoms
            """
            df = self.db.read_query(query)
            return {r["prenom"] for r in df.to_dicts()}
        except TableNotFoundError as e:
            print(f"    - Could not load prenoms: {e}")
            return set()

    def _load_brevo_bad_emails(self) -> set[str]:
        """
        Load emails that have hardbounced or were objected to from brevo.contacts.

        The brevo.contacts table is populated by a separate DAG that imports
        contacts from the Brevo API.
        """
        try:
            query = """
                SELECT
                    LOWER(TRIM(data->>'email')) as email
                FROM brevo.contacts
                WHERE
                    (data->>'emailBlacklisted')::boolean = true
                    OR data->'attributes'->>'DATE_DI_RGPD_OPPOSITION' IS NOT NULL
            """
            df = self.db.read_query(query)
            return {
                _normalize_email(r["email"]) for r in df.to_dicts() if r.get("email")
            }
        except TableNotFoundError as e:
            print(f"    - Could not load Brevo contacts: {e}")
            return set()

    def _identify_personal_emails(
        self,
        structures: pl.DataFrame,
        services: pl.DataFrame,
        prenoms: set[str],
    ) -> set[str]:
        all_emails = set()
        for row in structures.to_dicts():
            if row.get("courriel"):
                all_emails.add(row["courriel"].lower().strip())
        for row in services.to_dicts():
            if row.get("courriel"):
                all_emails.add(row["courriel"].lower().strip())

        personal = set()
        for email in all_emails:
            domain = _get_domain_2nd_level(email)
            local = _get_local_part(email)
            if not domain or not local:
                continue
            if domain not in PERSONAL_EMAIL_PROVIDERS:
                continue
            if NON_PERSONAL_PATTERNS.search(local):
                continue
            for prenom in prenoms:
                if local.startswith(f"{prenom}."):
                    personal.add(email)
                    break

        return personal

    def _mark_pii_and_bad(
        self, df: pl.DataFrame, personal_emails: set[str], bad_emails: set[str]
    ) -> pl.DataFrame:
        rows = []
        for row in df.to_dicts():
            email = (row.get("courriel") or "").lower().strip()
            normalized = _normalize_email(email)
            if email in personal_emails:
                row["_has_pii"] = True
            if normalized in bad_emails:
                row["_email_is_bad"] = True
            rows.append(row)
        return pl.DataFrame(rows, infer_schema_length=None)


def run_email_checks(
    db: DatabaseConnection,
    structures: pl.DataFrame,
    services: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    checker = EmailChecker(db)
    return checker.run(structures, services)
