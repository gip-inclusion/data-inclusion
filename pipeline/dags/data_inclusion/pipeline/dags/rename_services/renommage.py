import json
from typing import Annotated

import mlflow
import mlflow.genai
import openai
import pydantic
from mlflow.entities.model_registry import PromptModelConfig

from data_inclusion.pipeline.dags.rename_services import constants
from data_inclusion.schema import v1

INITIAL_SYSTEM_PROMPT = """
Ton rôle est de renommer le champ "nom" d'un service d'inclusion à partir des données \
fournies.

Le nom devra :

- reprendre les éléments clés de la description du service
- s'adresser aux bénéficiaires du service
- contenir des mots simples et accessibles
- ne pas contenir d'acronymes
- être un groupe nominal de quelques mots

Retourne uniquement le nom, sans explications.
"""

INITIAL_INPUT_PROMPT = "{{ service }}"

INITIAL_PROMPT = [
    {"role": "system", "content": INITIAL_SYSTEM_PROMPT},
    {"role": "user", "content": INITIAL_INPUT_PROMPT},
]


class Response(pydantic.BaseModel):
    nom: Annotated[str, pydantic.Field(min_length=3, max_length=150)]


def rename(service: v1.Service | dict, prompt_uri: str) -> str | None:
    if isinstance(service, v1.Service):
        service_data = service.model_dump(mode="json")
    else:
        service_data = service

    fields_of_interest = ["nom", "description", "thematiques", "type"]
    service_data = {field: service_data[field] for field in fields_of_interest}
    service_data = json.loads(json.dumps(service_data))

    @mlflow.trace
    def _rename(service: dict) -> str | None:
        # Load the prompt - this creates an automatic link to the trace
        prompt = mlflow.genai.load_prompt(name_or_uri=prompt_uri)

        model_config = PromptModelConfig.model_validate(prompt.model_config)

        openai_client = openai.OpenAI()
        completion = openai_client.chat.completions.parse(
            model=model_config.model_name or constants.DEFAULT_MODEL,
            messages=prompt.format(service=json.dumps(service)),
            max_completion_tokens=model_config.max_tokens,
            response_format=Response,
        )
        message = completion.choices[0].message

        return message.parsed.nom if message.parsed is not None else None

    return _rename(service=service_data)
