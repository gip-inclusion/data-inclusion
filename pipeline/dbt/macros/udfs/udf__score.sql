{% macro udf__score() %}

DROP FUNCTION IF EXISTS processings.score;

CREATE OR REPLACE FUNCTION processings.score(data JSONB)
RETURNS
    TABLE(
        score_ligne FLOAT,
        nom_critere TEXT,
        score_critere FLOAT
    )
AS $$

import json

import pydantic

from data_inclusion.schema import Service, score_qualite


# TODO(vmttn): run score *after* pydantic validation then remove this try/except
try:
    service = Service(**json.loads(data))
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