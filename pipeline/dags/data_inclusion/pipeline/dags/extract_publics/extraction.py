import re
import textwrap
from typing import Annotated, Literal, TypedDict

import mlflow
import openai
from pydantic import BaseModel, Field, model_validator

from data_inclusion.pipeline.dags.extract_publics import constants

LEXIQUE = {
    "AAH": "Allocation aux Adultes Handicapés",
    "ASS": "Allocation de Solidarité Spécifique",
    "ASE": "Aide Sociale à l'Enfance",
    "CIR": "Contrat d'Intégration Républicaine",
    "CSP": "Contrat de Sécurisation Professionnelle",
    "CUI": "Contrat Unique d'Insertion",
    "DELD": "Demandeur d'Emploi Longue Durée",
    "RQTH": "Reconnaissance de la Qualité de Travailleur Handicapé",
    "RSA": "Revenu de Solidarité Active",
    "SDF": "Sans Domicile Fixe",
    "QPV": "Quartier Prioritaire de la Ville",
    "ZRR": "Zone de Revitalisation Rurale",
}


class TrancheAge(BaseModel):
    min: Annotated[int | None, Field(title="Âge minimum (inclusif)")] = None
    max: Annotated[int | None, Field(title="Âge maximum (inclusif)")] = None

    @model_validator(mode="after")
    def check_min_max(self):
        if self.min is not None and self.max is not None and self.min > self.max:
            raise ValueError("min cannot be greater than max")
        return self


class Profil(BaseModel):
    age: Annotated[
        Literal["jeune"] | Literal["senior"] | TrancheAge | None,
        Field(description="Privilégier une tranche d'âge si possible"),
    ] = None

    genre: Literal["homme"] | Literal["femme"] | None = None

    activite: (
        Literal["etudiant"]
        | Literal["apprenti"]
        | Literal["alternant"]
        | Literal["actif"]
        | Literal["retraite"]
        | Literal["demandeur-emploi"]
        | None
    ) = None

    allocation: Annotated[
        Annotated[Literal["minima-sociaux"], Field(title=LEXIQUE["RSA"])]
        | Annotated[Literal["aah"], Field(title=LEXIQUE["AAH"])]
        | Annotated[Literal["ass"], Field(title=LEXIQUE["ASS"])]
        | Annotated[Literal["rsa"], Field(title=LEXIQUE["RSA"])]
        | None,
        Field(
            description=textwrap.dedent("""\
            Préciser les allocations si possibles,
            sinon indiquer simplement 'minima-sociaux'.
            """)
        ),
    ] = None

    statut_administratif: (
        Literal["etranger"] | Literal["demandeur-asile"] | Literal["refugie"] | None
    ) = None

    lieu_residence: (
        Annotated[Literal["qpv"], Field(title=LEXIQUE["QPV"])]
        | Annotated[Literal["zrr"], Field(title=LEXIQUE["ZRR"])]
        | Annotated[Literal["sdf"], Field(title=LEXIQUE["SDF"])]
        | Annotated[str, Field(description="Lieu(x) de résidence")]
        | None
    ) = None

    # TODO: refine these fields
    handicap: bool | None = None
    famille: bool | None = None
    adherent: bool | None = None


class Extraction(BaseModel):
    profils: list[Profil] | None = None


class Exemple(TypedDict):
    description: str
    extraction: Extraction


EXEMPLES = {
    r"\bou\b": Exemple(
        description="Bénéficiaire des minimas sociaux ou alternant de moins de 30 ans",
        extraction=Extraction(
            profils=[
                Profil(allocation="minima-sociaux"),
                Profil(age=TrancheAge(max=29), activite="alternant"),
            ]
        ),
    ),
    r"\b(commun|habitant)": Exemple(
        description="Habitants de la commune de Lens",
        extraction=Extraction(
            profils=[
                Profil(lieu_residence="Lens"),
            ]
        ),
    ),
}


SYSTEM_PROMPT = """\
Tu es un assistant qui aide à extraire des informations sur le profil des bénéficiaires
éligibles à un service rendu par une structure.

Tu reçois une description du public éligible. La description peut contenir plusieurs profils.

Ton rôle est d'extraire autant de profils que nécessaire.

Règles à respecter:
- n'extraire que les informations présentes dans la description
- bien distinguer chaque profil possible
"""  # noqa: E501


class Profiler:
    def __init__(
        self,
        model: str | None = None,
        temperature: float | None = None,
    ):
        self.openai_client = openai.OpenAI(max_retries=5)
        self.model = model or constants.DEFAULT_MODEL
        self.temperature = temperature or constants.DEFAULT_TEMPERATURE

    def extract(self, input: str) -> Extraction | None:
        lexique = self.lexique(input=input)
        exemple = self.exemple(input=input)

        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": f"Description: {input}"},
            {"role": "user", "content": f"Lexique: {lexique}"},
            {"role": "user", "content": f"Exemple: {exemple}"},
        ]

        completion = self.openai_client.chat.completions.parse(
            model=self.model,
            messages=messages,
            temperature=self.temperature,
            response_format=Extraction,
        )

        return completion.choices[0].message.parsed

    def exemple(self, input: str) -> list[Exemple] | None:
        return [
            exemple
            for pattern, exemple in EXEMPLES.items()
            if re.search(pattern, input, flags=re.IGNORECASE)
        ]

    def lexique(self, input: str) -> dict:
        return {
            key: value
            for key, value in LEXIQUE.items()
            if re.search(rf"\b{key}\b", input, flags=re.IGNORECASE)
        }


def extract(input: str) -> dict | None:

    # TODO: log prompt

    @mlflow.trace
    def _extract(input: str) -> Extraction | None:
        profiler = Profiler()

        return profiler.extract(input=input)

    extraction = _extract(input=input)

    if extraction is None:
        return None

    return extraction.model_dump(exclude_none=True)
