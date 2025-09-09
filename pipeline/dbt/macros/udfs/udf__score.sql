{% macro udf__score() %}

DROP FUNCTION IF EXISTS processings.score;

CREATE OR REPLACE FUNCTION processings.score(schema_version TEXT, data JSONB)
RETURNS
    TABLE(
        score_ligne FLOAT,
        nom_critere TEXT,
        score_critere FLOAT
    )
AS $$

import json

import pydantic

json_data = json.loads(data)

if schema_version == "v1":
    from data_inclusion.schema.v1 import Service, score_qualite
    # Ugly, but as it's the only field that changed ischema and not in name so far.
    json_data["frais"] = json_data["frais_v1"]
    json_data["thematiques"] = json_data["thematiques_v1"]
else:
    from data_inclusion.schema.v0 import Service, score_qualite


# TODO(vmttn): run score *after* pydantic validation then remove this try/except
try:
    service = Service(**json_data)
except pydantic.ValidationError as exc:
    return []

score, details = score_qualite.score(service)

return [
    {
        "score_ligne": score,
        "nom_critere": nom_critere,
        "score_critere": score_critere,
    }
    for nom_critere, score_critere in details.items()
]

$$ LANGUAGE plpython3u;

{% endmacro %}
