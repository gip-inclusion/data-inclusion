{# !!! THIS FILE IS AUTO-GENERATED. DO NOT EDIT MANUALLY. !!! #}

{% macro udf__html_to_markdown() %}

DROP FUNCTION IF EXISTS processings.html_to_markdown;

CREATE OR REPLACE FUNCTION processings.html_to_markdown(input TEXT)
RETURNS TEXT
AS $$

import markdownify


def html_to_markdown(content: str | None) -> str | None:
    if content is None or content == "":
        return content

    return markdownify.markdownify(
        autolinks=False,
        html=content,
        bullets="*",
        heading_style="ATX",  # use # for headings
        strip=["img"],  # ignore images
    )


return html_to_markdown(input)

$$ LANGUAGE plpython3u;

{% endmacro %}
