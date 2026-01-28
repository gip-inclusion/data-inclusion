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

import pydantic

from data_inclusion.schema import.v1 import Service, score_qualite

try:
    service = Service.model_validate_json(data)
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
